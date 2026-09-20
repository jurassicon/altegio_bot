#!/usr/bin/env python3
"""Change one rollout flag in ``easyweek.env`` safely, from the production host.

The runbook used to print a dotenv block and then a ``docker compose up -d``
command. That pair does not do what it looks like it does: recreating the
container re-reads ``easyweek.env`` from disk, and nothing had changed it. An
operator following it exactly would recreate the worker with the OLD value and
then verify the rollout against a flag that never moved.

This is the missing step, and it is deliberately narrow.

* stdlib only, and no ``altegio_bot`` import, so it runs with the host's
  ``python3`` without uv, a virtualenv or a built image;
* an ALLOWLIST of editable keys — it cannot touch an API key, a webhook secret
  or a database URL even if asked;
* every key must already have EXACTLY ONE assignment. Zero means the operator
  is editing a file that does not control the flag; more than one means the
  last line silently wins. Both refuse BEFORE anything is written;
* a timestamped backup with closed permissions, then an atomic replace that
  preserves the file's original mode;
* stdout carries only the keys that changed, as ``KEY=value``. The file is
  never echoed, and no other key — secret or not — is ever printed.

It does not restart anything. Recreating the worker and verifying the effective
value with ``printenv`` inside the container are separate, deliberate steps.
"""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ENV_FILE = "/opt/altegio_bot/easyweek.env"

# `true` / `false` only, lowercase and exact: pydantic accepts more spellings,
# but a rollout flag that reads `True` in one file and `true` in another is a
# diff nobody can review.
_BOOL_VALUES = ("true", "false")

# The ONLY keys this tool may write. Everything else in easyweek.env — API
# keys, the webhook secret, the workspace slug, the location map — is outside
# its reach by construction rather than by care.
BOOL_KEYS = (
    "EASYWEEK_NOTIFICATIONS_ENABLED",
    "EASYWEEK_REMINDERS_ENABLED",
    "EASYWEEK_REMINDER_API_GUARD_ENABLED",
    "EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED",
    "EASYWEEK_MULTI_SERVICE_SEND_ENABLED",
    "EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED",
)
# Empty (no restriction) or exactly one positive decimal job id. The same rule
# the runtime parser enforces, stated here so a typo is refused at the moment
# it is typed instead of silently holding the whole queue later.
JOB_ID_KEYS = ("EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID",)
ALLOWED_KEYS = BOOL_KEYS + JOB_ID_KEYS

_BACKUP_MODE = 0o600
PROG = "easyweek_env_set"


class EnvEditError(Exception):
    """Refusal. Raised before any write, so the file is always untouched."""


def _assignment_pattern(key: str) -> re.Pattern[str]:
    # Leading whitespace is tolerated because dotenv files in the wild have it;
    # a commented-out line is NOT an assignment and must not be counted as one,
    # which is why `#` is excluded rather than stripped.
    return re.compile(r"^[ \t]*" + re.escape(key) + r"[ \t]*=")


def validate_value(key: str, value: str) -> str:
    """The value this key may take, or a refusal. Never echoes other keys."""
    if key in BOOL_KEYS:
        if value not in _BOOL_VALUES:
            raise EnvEditError(f"{key}: value must be exactly 'true' or 'false'")
        return value
    if value == "":
        return value
    if not value.isascii() or not value.isdecimal() or int(value) <= 0:
        raise EnvEditError(f"{key}: value must be empty or one positive decimal job id")
    return value


def parse_assignments(pairs: list[str]) -> dict[str, str]:
    """``KEY=VALUE`` arguments, validated against the allowlist."""
    result: dict[str, str] = {}
    for item in pairs:
        if "=" not in item:
            raise EnvEditError(f"--set expects KEY=VALUE, got {item.split('=', 1)[0]!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if key not in ALLOWED_KEYS:
            raise EnvEditError(f"{key}: not an editable rollout key")
        if key in result:
            raise EnvEditError(f"{key}: named twice on the command line")
        result[key] = validate_value(key, value.strip())
    if not result:
        raise EnvEditError("at least one --set KEY=VALUE is required")
    return result


def apply_assignments(lines: list[str], assignments: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    """Rewrite exactly the matched lines, or refuse. Pure: no file is touched.

    The count check runs over ALL keys before a single line is rewritten, so a
    second key with two assignments cannot leave the first one already changed.
    """
    for key in assignments:
        pattern = _assignment_pattern(key)
        found = sum(1 for line in lines if pattern.match(line))
        if found == 0:
            raise EnvEditError(f"{key}: no assignment found in the env file")
        if found > 1:
            raise EnvEditError(f"{key}: {found} assignments found, expected exactly one")

    changed: dict[str, str] = {}
    updated = list(lines)
    for key, value in assignments.items():
        pattern = _assignment_pattern(key)
        for index, line in enumerate(updated):
            if not pattern.match(line):
                continue
            replacement = f"{key}={value}"
            if line.rstrip("\n") != replacement:
                updated[index] = replacement + "\n"
                changed[key] = value
            break
    return updated, changed


def _backup(path: Path, text: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.name}.bak.{stamp}")
    if backup.exists():
        raise EnvEditError(f"backup {backup.name} already exists; refusing to overwrite it")
    # Opened with the closed mode rather than chmod'ed afterwards: a copy of a
    # secrets file must never exist world-readable, not even for an instant.
    descriptor = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _BACKUP_MODE)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(text)
    return backup


def _atomic_write(path: Path, text: str, mode: int) -> None:
    descriptor, temp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    except BaseException:
        # A failed edit must leave the original file exactly as it was.
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise


def edit_env_file(path: Path, assignments: dict[str, str], *, dry_run: bool = False) -> dict[str, str]:
    """Apply *assignments* to *path*, or raise before writing anything."""
    if not path.is_file():
        raise EnvEditError(f"{path} is not a file")
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    updated, changed = apply_assignments(lines, assignments)
    if not changed or dry_run:
        return changed
    _backup(path, original)
    _atomic_write(path, "".join(updated), stat.S_IMODE(path.stat().st_mode))
    return changed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Change allowlisted EasyWeek rollout flags in easyweek.env, atomically and reversibly.",
        allow_abbrev=False,
    )
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    parser.add_argument("--set", dest="assignments", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report; write nothing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        assignments = parse_assignments(list(args.assignments))
        changed = edit_env_file(Path(args.env_file), assignments, dry_run=bool(args.dry_run))
    except EnvEditError as exc:
        print(f"{PROG}: refused: {exc}", file=sys.stderr)
        return 1
    if not changed:
        print(f"{PROG}: already at the requested values; nothing written", file=sys.stderr)
        return 0
    for key in sorted(changed):
        print(f"{key}={changed[key]}")
    if args.dry_run:
        print(f"{PROG}: dry run; nothing written", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

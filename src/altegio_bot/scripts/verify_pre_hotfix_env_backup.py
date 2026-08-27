"""Bind the single-inbox rollback to the map that was actually live before it.

Rollback restores `CHATWOOT_INBOX_COMPANY_MAP` from a backup file. Restoring the
wrong one is not cosmetic: a backup taken *after* the hotfix armed carries `{}`,
so the "rollback" would put production back into the empty-map state and every
operator reply would die as ``operator_relay: ambiguous_sender`` — the exact
failure the rollback exists to undo.

Two earlier attempts at that guarantee were wrong in opposite directions.
Choosing the newest `.env.bak.*` by filename cannot tell a pre- from a
post-hotfix backup at all. Comparing against a branch map hardcoded in this
repository is worse than useless the day production legitimately changes: plan
§10 records an EasyWeek location id that simply stopped existing, so a numeric
id pinned here would eventually block a rollback that must succeed.

So this module never knows what the topology *should* be. It snapshots what the
map *is*, immediately before the rollout empties it, and reduces it to a
fingerprint the handoff carries. Everything afterwards — the rollout's own
pre-write gate and the rollback — proves the backup still hashes to that
snapshot. The identity model itself is untouched: this reads
``CHATWOOT_INBOX_COMPANY_MAP`` through the workers' own fail-closed parser and
adds no new format.

Input is the *matching lines only*, never the whole `.env`::

    grep -E '^CHATWOOT_INBOX_COMPANY_MAP=' "$FILE" | python -m ... snapshot

so no secret from `.env` is ever piped anywhere. Output carries inbox ids,
providers, company ids, booleans and a hex digest — never the raw map value.

Usage::

    ... | python -m altegio_bot.scripts.verify_pre_hotfix_env_backup snapshot
    ... | python -m altegio_bot.scripts.verify_pre_hotfix_env_backup \\
              verify --expect-fingerprint <hex>

Exit code 0 means proven. Anything else means the caller must stop before
touching `.env`.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass, field

from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

MAP_KEY = "CHATWOOT_INBOX_COMPANY_MAP"
FINGERPRINT_OUTPUT_KEY = "PRE_HOTFIX_MAP_FINGERPRINT"

# Versioned so a future change to what the fingerprint covers can never be
# mistaken for a matching digest.
_FINGERPRINT_SCHEME = "single-inbox-branch-map-v1"


@dataclass(frozen=True)
class MapVerdict:
    """Safe-to-print outcome. Stable reason codes, never raw configuration."""

    ok: bool
    reason: str = ""
    fingerprint: str = ""
    expected_fingerprint: str = ""
    identities: list[tuple[int, str, int]] = field(default_factory=list)
    map_configured: bool = False
    map_valid: bool = False
    map_provider_scoped: bool = False

    def as_safe_dict(self) -> dict[str, object]:
        return {
            "map_ok": self.ok,
            "reason": self.reason,
            "map_configured": self.map_configured,
            "map_valid": self.map_valid,
            "map_provider_scoped": self.map_provider_scoped,
            "branch_identities": self.identities,
            "map_fingerprint": self.fingerprint,
            "expected_fingerprint": self.expected_fingerprint,
            "fingerprint_matches": bool(self.fingerprint) and self.fingerprint == self.expected_fingerprint,
        }


def extract_map_value(lines: str) -> tuple[str | None, str | None]:
    """The single ``CHATWOOT_INBOX_COMPANY_MAP=`` value, or a refusal.

    The caller pipes in the grep output, so "exactly one" here is exactly the
    "appears once in the file" check — two lines mean a duplicated key, which
    Docker Compose would resolve by last-wins and nobody should have to guess.
    """
    candidates = [line for line in lines.splitlines() if line.startswith(f"{MAP_KEY}=")]
    if not candidates:
        return None, "map_line_missing"
    if len(candidates) > 1:
        return None, "map_line_not_unique"
    return candidates[0].split("=", 1)[1], None


def map_fingerprint(identities: list[tuple[int, str, int]]) -> str:
    """A digest of the normalised topology — order- and formatting-independent.

    Whitespace, key order and JSON spacing must not change the answer; a
    different provider, company id or inbox must.
    """
    canonical = "|".join(f"{inbox_id}:{provider}:{company_id}" for inbox_id, provider, company_id in identities)
    return hashlib.sha256(f"{_FINGERPRINT_SCHEME}|{canonical}".encode()).hexdigest()


def snapshot_branch_map(lines: str, *, expected_fingerprint: str = "") -> MapVerdict:
    """Prove one branch map is a usable provider-scoped topology, and hash it.

    Fail-closed at every step, and deliberately opinion-free about *which*
    topology is correct: only that there is exactly one, that the workers' own
    parser accepts it, and that it proves a provider for every inbox.
    """
    raw, extract_error = extract_map_value(lines)
    if extract_error is not None:
        return MapVerdict(ok=False, reason=extract_error, expected_fingerprint=expected_fingerprint)

    parsed = parse_chatwoot_inbox_company_map(raw)
    identities = sorted(
        (int(inbox_id), identity.provider, int(identity.company_id)) for inbox_id, identity in parsed.mapping.items()
    )
    base = {
        "identities": identities,
        "map_configured": parsed.configured,
        "map_valid": parsed.valid,
        "map_provider_scoped": parsed.provider_scoped,
        "expected_fingerprint": expected_fingerprint,
    }

    if not parsed.configured:
        # `""` or `{}`: either this predates the branch map, or it was captured
        # after the hotfix emptied it. Neither is a pre-hotfix state.
        return MapVerdict(ok=False, reason="map_unconfigured", **base)
    if not parsed.valid:
        # Malformed JSON, duplicate JSON key, colliding inbox key, repeated tenant.
        return MapVerdict(ok=False, reason="map_invalid", **base)
    if not parsed.provider_scoped:
        # Legacy integer-only map: a company id with no provider proves nothing.
        return MapVerdict(ok=False, reason="map_not_provider_scoped", **base)

    fingerprint = map_fingerprint(identities)
    if expected_fingerprint and fingerprint != expected_fingerprint:
        return MapVerdict(ok=False, reason="map_fingerprint_mismatch", fingerprint=fingerprint, **base)
    return MapVerdict(ok=True, reason="map_proven", fingerprint=fingerprint, **base)


def _parse_argv(argv: list[str]) -> tuple[str, str] | None:
    """(command, expected_fingerprint) or None for an unusable invocation."""
    if not argv:
        return None
    command = argv[0]
    if command == "snapshot":
        return ("snapshot", "") if len(argv) == 1 else None
    if command == "verify":
        if len(argv) == 3 and argv[1] == "--expect-fingerprint" and argv[2]:
            return "verify", argv[2]
        return None
    return None


def main(argv: list[str] | None = None) -> int:
    parsed_argv = _parse_argv(list(argv if argv is not None else sys.argv[1:]))
    if parsed_argv is None:
        print({"map_ok": False, "reason": "usage_error"})
        return 2
    _command, expected_fingerprint = parsed_argv

    verdict = snapshot_branch_map(sys.stdin.read(), expected_fingerprint=expected_fingerprint)
    print(verdict.as_safe_dict())
    if not verdict.ok:
        return 1
    # A single machine-readable line, so the runbook can capture the digest
    # without parsing the human summary above it.
    print(f"{FINGERPRINT_OUTPUT_KEY}={verdict.fingerprint}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

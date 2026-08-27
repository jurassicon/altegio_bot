"""Prove a pre-hotfix `.env` backup before the single-inbox rollback uses it.

Rollback restores the branch map from a backup file. Restoring the WRONG backup
is not a cosmetic mistake: a backup taken *after* the hotfix armed carries
``CHATWOOT_INBOX_COMPANY_MAP={}``, so the "rollback" would put production back
into the empty-map state, and every operator reply would die as
``operator_relay: ambiguous_sender`` — the exact failure the rollback exists to
undo. Picking the newest file by name cannot tell the two apart.

So the map is parsed with the same fail-closed parser the workers use and then
compared, identity by identity, against the topology the operator states. Empty,
partial, legacy integer-only, duplicate-key and foreign-identity maps all stop
the rollback here, before the live `.env` is touched.

Nothing raw is printed. The output carries inbox ids, providers, company ids and
booleans — never the raw configuration string, a token, a phone or a secret.

Usage (from the runbook, inside a worker container)::

    BACKUP_MAP=<value from the backup>       # never echoed
    EXPECTED_BRANCH_MAP=<expected topology>  # written in the runbook
    python -m altegio_bot.scripts.verify_pre_hotfix_env_backup

Exit code 0 means the backup is safe to restore. Any other code means it is not.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field

from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

BACKUP_MAP_ENV = "BACKUP_MAP"
EXPECTED_BRANCH_MAP_ENV = "EXPECTED_BRANCH_MAP"

# The value a backup taken *while the hotfix was armed* carries. Recognised
# explicitly so the operator gets "this is a post-hotfix backup" instead of the
# generic "not provider-scoped".
_ARMED_HOTFIX_REASONS = frozenset({"backup_map_unconfigured"})


@dataclass(frozen=True)
class BackupVerdict:
    """Safe-to-print outcome. Reasons are stable codes, never raw config."""

    ok: bool
    reason: str = ""
    branch_identities: list[tuple[int, str, int]] = field(default_factory=list)
    expected_identities: list[tuple[int, str, int]] = field(default_factory=list)
    backup_map_configured: bool = False
    backup_map_valid: bool = False
    backup_map_provider_scoped: bool = False

    def as_safe_dict(self) -> dict[str, object]:
        return {
            "backup_ok": self.ok,
            "reason": self.reason,
            "backup_map_configured": self.backup_map_configured,
            "backup_map_valid": self.backup_map_valid,
            "backup_map_provider_scoped": self.backup_map_provider_scoped,
            "backup_branch_identities": self.branch_identities,
            "expected_branch_identities": self.expected_identities,
            "identities_match": self.branch_identities == self.expected_identities,
            "post_hotfix_backup": self.reason in _ARMED_HOTFIX_REASONS,
        }


def _identities(parsed_mapping: dict) -> list[tuple[int, str, int]]:
    return sorted(
        (int(inbox_id), identity.provider, int(identity.company_id)) for inbox_id, identity in parsed_mapping.items()
    )


def verify_backup_map(backup_raw: object, expected_raw: object) -> BackupVerdict:
    """Is this backup's branch map the exact pre-hotfix production topology?

    The expected map is validated too: a typo in the runbook must not be able to
    wave a bad backup through by accidentally matching it.
    """
    expected = parse_chatwoot_inbox_company_map(expected_raw)
    if not (expected.configured and expected.valid and expected.provider_scoped):
        return BackupVerdict(ok=False, reason="expected_map_unusable")

    backup = parse_chatwoot_inbox_company_map(backup_raw)
    verdict = BackupVerdict(
        ok=False,
        branch_identities=_identities(backup.mapping),
        expected_identities=_identities(expected.mapping),
        backup_map_configured=backup.configured,
        backup_map_valid=backup.valid,
        backup_map_provider_scoped=backup.provider_scoped,
    )

    if not backup.configured:
        # `""` or `{}` — either the backup predates the branch map entirely, or
        # it was taken after the hotfix emptied it. Restoring it is not a
        # rollback.
        return BackupVerdict(**{**verdict.__dict__, "reason": "backup_map_unconfigured"})
    if not backup.valid:
        # Malformed JSON, duplicate key, colliding inbox key, repeated tenant.
        return BackupVerdict(**{**verdict.__dict__, "reason": "backup_map_invalid"})
    if not backup.provider_scoped:
        # Legacy integer-only map: a company id with no provider proves nothing.
        return BackupVerdict(**{**verdict.__dict__, "reason": "backup_map_not_provider_scoped"})
    if verdict.branch_identities != verdict.expected_identities:
        # Partial, extra or foreign branches — including one right identity and
        # one wrong one.
        return BackupVerdict(**{**verdict.__dict__, "reason": "backup_map_identity_mismatch"})

    return BackupVerdict(**{**verdict.__dict__, "ok": True, "reason": "backup_map_proven"})


def main(argv: list[str] | None = None) -> int:
    del argv
    backup_raw = os.environ.get(BACKUP_MAP_ENV)
    expected_raw = os.environ.get(EXPECTED_BRANCH_MAP_ENV)
    if backup_raw is None or expected_raw is None:
        print({"backup_ok": False, "reason": "missing_input"})
        return 2

    verdict = verify_backup_map(backup_raw, expected_raw)
    print(verdict.as_safe_dict())
    return 0 if verdict.ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

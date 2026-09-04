"""CLI for preparing a migration wave, before the migrator runs at all.

    python -m altegio_bot.scripts.easyweek_migration_prepare <mode> [options]

Modes::

    prepare           read Altegio, the EasyWeek catalogue and the EasyWeek
                      customers; propose a service mapping; work out which
                      customers are missing. Writes local files only.
    confirm           record what a person agreed to: services, customers,
                      skips and corrections. Local files only.
    create-customers  create exactly the confirmed customers. The ONLY mode
                      here that mutates EasyWeek.
    verify-dry-run    run the existing migrator's dry-run against the prepared
                      artefacts and hand back ITS digest, plus the apply command
                      that digest belongs to.

Why this is a separate command from ``easyweek_migration``
----------------------------------------------------------
Creating customer cards and migrating bookings are different powers. Keeping
them in one executable would mean one set of flags could do both, and the
attestations that guard a booking write have nothing to say about a customer
write. So they are separate programs with separate permissions: this one cannot
create a booking, and ``easyweek_migration --apply`` cannot create a customer.

``create-customers`` additionally requires ``--i-authorise-creating-customers``
**and** the environment variable ``EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE=true``
— a flag a person types and a setting an operator put on the host, because the
containerised runs are the ones where a stray argument is easiest to leave in a
saved command line.

Consent never comes from stdin
------------------------------
No mode reads stdin. A Docker run with no TTY, a closed pipe or an EOF cannot be
mistaken for agreement, because there is no code path that could read one. A
batch confirmation is bound to the digest of the exact list that was printed.

Exit codes::

    0  the mode completed and nothing is outstanding
    1  the mode refused, or something still needs a person
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Final

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_migration.altegio_source import build_window, fetch_company_records
from altegio_bot.easyweek_migration.customer_decisions import DecisionStoreError
from altegio_bot.easyweek_migration.customers import load_customer_directory
from altegio_bot.easyweek_migration.cutover import CutoverError, parse_cutover
from altegio_bot.easyweek_migration.manifest import inventory_manifest, load_manifest
from altegio_bot.easyweek_migration.mapping_proposal import (
    collect_source_services,
    propose_service_mapping,
)
from altegio_bot.easyweek_migration.prepare import (
    FILE_CUSTOMER_DIRECTORY,
    FILE_OPERATOR_REVIEW,
    MODE_CONFIRM,
    MODE_CREATE_CUSTOMERS,
    MODE_PREPARE,
    MODE_VERIFY_DRY_RUN,
    ConfirmRequest,
    PrepareError,
    PrepareInputs,
    apply_confirmations,
    run_create_customers,
    run_prepare,
)
from altegio_bot.easyweek_migration.runner import (
    DEFAULT_HORIZON_DAYS,
    MODE_DRY_RUN,
    RunInputs,
    new_run_id,
    run_inventory_or_dry_run,
)
from altegio_bot.easyweek_migration.service_catalog import ServiceEvidenceError, read_full_catalog
from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient

logger = logging.getLogger("easyweek_migration.prepare.cli")

MODES: Final = (MODE_PREPARE, MODE_CONFIRM, MODE_CREATE_CUSTOMERS, MODE_VERIFY_DRY_RUN)

DEFAULT_STATE_DIR: Final = os.environ.get("EASYWEEK_MIGRATION_STATE_DIR") or "outputs/easyweek_migration_prepare"

# The host-side half of the customer-creation permission. Checked in addition to
# the typed flag: the flag proves somebody meant it now, the variable proves the
# host is one where customer creation is allowed at all.
CREATE_ENV_FLAG: Final = "EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE"
CREATE_ARG_FLAG: Final = "--i-authorise-creating-customers"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_migration_prepare",
        description="Prepare an Altegio → EasyWeek wave. Read-only unless create-customers is asked for.",
    )
    parser.add_argument("mode", nargs="?", default=MODE_PREPARE, choices=MODES)
    parser.add_argument(
        "--manifest",
        required=True,
        help=(
            "path to the mapping JSON. For prepare/confirm this may still be incomplete; for verify-dry-run "
            "pass the merged file the confirmation wrote (manifest.proposed.json in the state directory), "
            "which is the same file the apply will use."
        ),
    )
    parser.add_argument("--company-id", type=int, required=True, help="the Altegio company id of the branch")
    parser.add_argument(
        "--cutover-at",
        required=True,
        help="the wave's cutover instant, ISO-8601 WITH offset. The same value the apply will use.",
    )
    parser.add_argument("--horizon-days", type=int, default=DEFAULT_HORIZON_DAYS)
    parser.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
        help="where decisions and the operator review live. HOLDS PII; never commit it.",
    )

    parser.add_argument("--confirm-customer", action="append", default=[], metavar="PHONE")
    parser.add_argument("--skip-customer", action="append", default=[], metavar="PHONE")
    parser.add_argument(
        "--confirm-all-pending-customers",
        action="store_true",
        help="confirm every pending customer. Requires --pending-digest, which binds it to the printed list.",
    )
    parser.add_argument("--pending-digest", help="the customer pending_digest printed by the prepare run")
    parser.add_argument("--correct-customer", metavar="PHONE", help="correct one customer's details")
    parser.add_argument("--first-name")
    parser.add_argument("--last-name")
    parser.add_argument("--email")

    parser.add_argument("--confirm-service", action="append", type=int, default=[], metavar="ALTEGIO_SERVICE_ID")
    parser.add_argument(
        "--confirm-all-services",
        action="store_true",
        help="confirm every unambiguous service proposal. Requires --mapping-digest.",
    )
    parser.add_argument("--mapping-digest", help="the mapping pending_digest printed by the prepare run")

    parser.add_argument(
        CREATE_ARG_FLAG,
        dest="authorise_customer_create",
        action="store_true",
        help=(
            "create-customers only: authorise creating EasyWeek customer cards. "
            f"Also needs {CREATE_ENV_FLAG}=true in the environment. This permission does NOT "
            "allow migrating a booking."
        ),
    )
    return parser


def _fail(message: str) -> int:
    print(f"easyweek_migration_prepare: refused: {message}", file=sys.stderr)
    return 1


def _read_manifest_json(path: str) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError:
        raise PrepareError("the manifest file cannot be read") from None
    except Exception:
        raise PrepareError("the manifest file is not valid JSON") from None
    if not isinstance(payload, dict):
        raise PrepareError("the manifest file is not a JSON object")
    return payload


def _create_permitted(args: argparse.Namespace) -> bool:
    """Both halves, or nothing. Neither one alone is authorisation."""
    env = (os.environ.get(CREATE_ENV_FLAG) or "").strip().lower()
    return bool(args.authorise_customer_create) and env == "true"


async def _load_proposals(inputs: PrepareInputs, write_client: Any) -> list[Any]:
    """Re-derive the proposals a confirmation refers to, from the same sources.

    Confirming has to see the same list ``prepare`` printed, and the digest check
    is only meaningful if the list is rebuilt rather than read back from a file
    the confirmation itself could have written.
    """
    branch = inputs.manifest.branch(inputs.altegio_company_id)
    if branch is None:
        raise PrepareError("manifest has no entry for that Altegio company id")
    window = build_window(inputs.cutover.at, horizon_days=inputs.horizon_days)
    records = await fetch_company_records(inputs.altegio_company_id, window)
    services = collect_source_services(records, staff_ids=set(branch.selected_staff_ids) or None)
    try:
        catalog = await read_full_catalog(write_client, location_uuid=branch.easyweek_location_uuid)
    except ServiceEvidenceError as error:
        raise PrepareError(f"catalogue unreadable ({error.reason})") from None
    return propose_service_mapping(
        altegio_company_id=inputs.altegio_company_id,
        source_services=services,
        catalog=catalog,
        catalog_staff={},
        selected_staff_uuids=set(inputs.selected_staff_uuids),
        branch=branch,
    )


async def _run(args: argparse.Namespace) -> int:
    try:
        manifest_json = _read_manifest_json(args.manifest)
    except PrepareError as error:
        return _fail(str(error))

    # `prepare` and `confirm` run while the mapping is still being built, which
    # is the whole point of them; `verify-dry-run` is the review artefact and
    # gets the strict, all-or-nothing parse the migrator itself uses.
    if args.mode == MODE_VERIFY_DRY_RUN:
        manifest = load_manifest(args.manifest)
    else:
        manifest = inventory_manifest(json.dumps(manifest_json))
    if not manifest.valid:
        return _fail(f"manifest is unusable ({manifest.reason})")

    try:
        cutover = parse_cutover(args.cutover_at)
    except CutoverError as error:
        return _fail(str(error))
    if args.horizon_days < 1:
        return _fail("--horizon-days must be >= 1")

    branch = manifest.branch(args.company_id)
    if branch is None:
        return _fail("the manifest has no entry for that Altegio company id")

    inputs = PrepareInputs(
        mode=args.mode,
        run_id=new_run_id(),
        state_dir=Path(args.state_dir),
        manifest=manifest,
        manifest_json=manifest_json,
        altegio_company_id=args.company_id,
        cutover=cutover,
        horizon_days=args.horizon_days,
        selected_staff_uuids=frozenset(
            uuid for staff_id, uuid in branch.staff.items() if staff_id in branch.selected_staff_ids
        ),
        create_allowed=_create_permitted(args),
    )

    try:
        if args.mode == MODE_PREPARE:
            async with EasyWeekMigrationWriteClient() as client:
                result = await run_prepare(inputs, write_client=client)
            _print(result.machine)
            print(
                f"easyweek_migration_prepare: operator review (PII) written to "
                f"{inputs.state_dir / FILE_OPERATOR_REVIEW}",
                file=sys.stderr,
            )
            return result.exit_code

        if args.mode == MODE_CONFIRM:
            request = ConfirmRequest(
                confirm_customers=tuple(args.confirm_customer),
                skip_customers=tuple(args.skip_customer),
                confirm_all_pending=bool(args.confirm_all_pending_customers),
                expected_pending_digest=args.pending_digest,
                correct_phone=args.correct_customer,
                correct_first_name=args.first_name,
                correct_last_name=args.last_name,
                correct_email=args.email,
                confirm_services=tuple(args.confirm_service),
                confirm_all_services=bool(args.confirm_all_services),
                expected_mapping_digest=args.mapping_digest,
            )
            proposals = None
            if request.confirm_services or request.confirm_all_services:
                async with EasyWeekMigrationWriteClient() as client:
                    proposals = await _load_proposals(inputs, client)
            outcome = apply_confirmations(inputs, request, proposals=proposals)
            _print(outcome)
            return 0

        if args.mode == MODE_CREATE_CUSTOMERS:
            if not inputs.create_allowed:
                return _fail(
                    f"customer creation needs BOTH {CREATE_ARG_FLAG} and {CREATE_ENV_FLAG}=true. "
                    "This permission is separate from the migrator's --apply."
                )
            async with EasyWeekMigrationWriteClient() as client:
                result = await run_create_customers(inputs, write_client=client)
            _print(result.machine)
            return result.exit_code

        return await _verify_dry_run(args, inputs)
    except (PrepareError, DecisionStoreError) as error:
        return _fail(str(error))
    except Exception as error:  # noqa: BLE001 — a type name, never a payload
        return _fail(f"{type(error).__name__}: {error}")


async def _verify_dry_run(args: argparse.Namespace, inputs: PrepareInputs) -> int:
    """Run the existing dry-run and report ITS digest, with the apply command.

    The digest is taken off the report object this process just produced. It is
    never read back from "the newest file in the report directory" — which is how
    an operator ends up approving one plan and applying another, and is exactly
    the identifier-passing this whole stage exists to remove.
    """
    directory_path = inputs.state_dir / FILE_CUSTOMER_DIRECTORY
    directory = load_customer_directory(directory_path)
    if not directory.valid:
        return _fail(f"the prepared customer directory is unusable ({directory.reason}); run prepare first")

    dry_run = RunInputs(
        mode=MODE_DRY_RUN,
        run_id=new_run_id(),
        cutover=inputs.cutover,
        manifest=inputs.manifest,
        directory=directory,
        horizon_days=inputs.horizon_days,
        cutover_supplied=True,
    )
    async with SessionLocal() as session:
        report = await run_inventory_or_dry_run(session, dry_run)

    manifest_path = args.manifest
    apply_command = " ".join(
        [
            "python -m altegio_bot.scripts.easyweek_migration apply",
            f"--manifest {manifest_path}",
            f"--customer-directory {directory_path}",
            f"--cutover-at {args.cutover_at}",
            f"--horizon-days {args.horizon_days}",
            "--apply",
            f"--verified-dry-run-id {report.plan_digest}",
            "--confirm-easyweek-native-notifications-disabled",
        ]
    )
    _print(
        {
            "mode": MODE_VERIFY_DRY_RUN,
            "verified_dry_run_id": report.plan_digest,
            "manifest": manifest_path,
            "customer_directory": str(directory_path),
            "ready_rows": report.outcomes.get("ready", 0),
            "blocked_rows": report.outcomes.get("blocked", 0),
            "errors": list(report.errors),
            "next_command_after_a_clean_canary": apply_command,
        }
    )
    return 1 if report.errors else 0


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.mode == MODE_PREPARE and args.manifest and not Path(args.manifest).exists():
        return _fail("the manifest file does not exist")
    print(
        f"easyweek_migration_prepare: state directory {args.state_dir} holds personal data; "
        "it is not committed and must not be shared",
        file=sys.stderr,
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())

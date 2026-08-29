# EasyWeek cutover manifest (PR-11.1)

`migration_manifest.example.json` is a **template with placeholder values only**.
It is the one migration input that may live in Git, because it holds identifiers
and nothing else — no phone, no name, no e-mail, no export.

## What goes in it

| Key | Where the real value comes from |
|---|---|
| `manifest_id` | operator-chosen label, `[a-z0-9-_.]`, ≤ 64 chars |
| `branches.<altegio_company_id>` | Karlsruhe `758285`, Rastatt `1271200`. No other company is accepted. |
| `easyweek_location_id` | numeric `location_id` for that branch — same value as in `EASYWEEK_LOCATION_MAP` |
| `easyweek_location_uuid` | `GET /locations` (`scripts/easyweek_probe.py`), matched by the human-readable branch name |
| `staff.<altegio_staff_id>` | EasyWeek staff UUID for that master, read from the EasyWeek UI/API |
| `services.<altegio_service_id>` | EasyWeek service UUID for that service |

Durlach is deliberately absent. It does not exist in Altegio, so there is no
`company_id` under which it could be written down, and no booking of its can be
fetched.

## Rules the parser enforces

- **Both company ids must agree.** The object key and `altegio_company_id` are
  the same value written twice. This is the check that would have caught the
  Durlach/Rastatt configuration swap in INTEGRATION_PLAN §10.
- **Canonical UUIDs only.** Uppercase, braces or surrounding whitespace are
  rejected at parse time rather than at the first 404 mid-apply.
- **Ids are scoped per branch.** Altegio numeric ids are unique per company, so a
  flat map would let one branch's staff `42` answer for another's.
- **All or nothing.** One bad entry rejects the whole file — a silently dropped
  branch would look exactly like a branch with no bookings.
- **Empty staff or services is invalid.** That is an unfinished manifest, and it
  should say so once rather than produce a report full of `mapping_missing`.

## How to build it

1. `inventory` mode runs **without** a customer directory and lists what the
   Altegio side actually contains. Use it to see which staff and service ids
   appear in real future bookings.
2. Look each one up in EasyWeek **by name, by hand, once**. Names may be used
   here, and only here: this is read-only preparation, and a human is checking
   the result. During `apply` no name is ever consulted.
3. Fill the file in, re-run `inventory`, and confirm the `mapping_missing` count
   is zero for the branches you intend to migrate.

Never copy an EasyWeek customer export into this file. Customers are resolved at
run time from a separate, git-ignored export passed with `--customer-directory`.

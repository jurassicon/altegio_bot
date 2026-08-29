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
| `services.<altegio_service_id>.easyweek_service_uuid` | EasyWeek service UUID for that service |
| `services.<altegio_service_id>.catalog_duration_minutes` | the service's catalogue length, whole minutes |
| `services.<altegio_service_id>.catalog_price` | the service's catalogue price, as a string (`"90.00"`, `"0"`) |

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
- **Empty staff or services is invalid** for `dry-run`, `canary` and `apply`.
  `inventory` deliberately accepts an empty mapping — it is the mode whose job
  is to tell you which ids to fill in, and requiring them first was a
  chicken-and-egg bug.
- **Both catalogue fields are mandatory.** They are what a per-booking override
  is measured *against*: without them a slot hand-stretched to 90 minutes, or a
  price discounted to zero, has nothing to disagree with and migrates as if it
  were the standard service. `catalog_price` is a string so `"0"` (a genuinely
  free service) stays distinguishable from a missing value, and money is
  compared as an exact decimal rather than a float.
- **The branch is proven independently.** `758285` must map to the EasyWeek
  registry slug `karlsruhe` and `1271200` to `rastatt`. That expectation lives
  in source control, not in this file, because a manifest with the two swapped
  is internally consistent and would migrate every Karlsruhe customer into
  Rastatt.

## How to build it

1. `inventory` mode runs **without** a customer directory and **without a
   finished manifest**. Its report lists, per branch, every Altegio staff and
   service id the future bookings use, how many bookings each carries, and
   which are still `missing` from this file.
2. Look each one up in EasyWeek **by name, by hand, once**. Names may be used
   here, and only here: this is read-only preparation, and a human is checking
   the result. During `apply` no name is ever consulted.
3. Fill the file in — including each service's catalogue duration and price —
   re-run `inventory`, and confirm `staff.missing` and `services.missing` are
   empty for the branches you intend to migrate.

Never copy an EasyWeek customer export into this file. Customers are resolved at
run time from a separate, git-ignored export passed with `--customer-directory`.

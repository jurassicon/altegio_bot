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
| `selected_altegio_staff_ids` | the masters migrating in THIS wave, by Altegio staff id |
| `deferred_altegio_staff_ids` | every other master — held back for a later wave, or already migrated by an earlier one |
| `staff.<altegio_staff_id>` | EasyWeek staff UUID for that master, read from the EasyWeek UI/API. Selected AND already-migrated masters both belong here |
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
- **Every master in the window must be named.** The cutover runs in waves, so
  each branch states `selected_altegio_staff_ids` and
  `deferred_altegio_staff_ids`. The two may not overlap, every selected master
  must have a real UUID in `staff`, and a wave that selects nobody is an
  unfinished manifest.

  Leaving a master out of `staff` is **not** a way to exclude her. If it were,
  "we deliberately deferred her" and "we forgot her" would be the same state —
  and the day somebody forgot a master, the completeness check would call the
  wave finished while her customers had nowhere to arrive. A master in neither
  list blocks the cutover with `staff_not_in_wave_scope`; a deferred one is
  skipped with `staff_deferred_to_later_wave` and counted separately.

  Both lists are part of the manifest digest, so moving a master between waves
  invalidates the verified dry-run and the canary proof.
- **The mapping is cumulative; the selector is not.** `staff` and `services` hold
  every master and service mapped by *any* wave so far. The selector holds only
  the composition of the wave being run now. Those are different questions, and
  the file answers them separately.

  So a mapping does **not** select anybody: a master with a full mapping who sits
  in `deferred_altegio_staff_ids` is not migrated again, her already-migrated
  bookings stay `already_migrated`, and her new ones are skipped with
  `staff_deferred_to_later_wave`. The example file shows exactly this shape —
  `1000002` is deferred and mapped, because an earlier wave migrated her.

  And a mapping may not be dropped once it has been used: while a master has a
  live source booking with a `created` ledger row, her staff mapping, that
  booking's service mapping and its catalogue baseline must stay in every later
  manifest, unchanged, until that row is finished. The final reconciliation of
  *every* later wave re-reads those live bookings with the current manifest, so a
  manifest that lost the mapping turns an untouched, perfectly healthy booking
  into `migrated_source_lifecycle_unprovable`. Checked before the first mutation
  of a wave, not after — see the runbook, step 5a.

  The reverse is not required: the first wave owes nothing for masters who have
  never been migrated. A deferred master with no `created` rows needs no mapping
  at all.
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
3. Decide the wave. Put every master the report listed into exactly one of
   `selected_altegio_staff_ids` or `deferred_altegio_staff_ids` — names may
   guide the decision, but only ids go in the file.
4. Fill the mapping in — including each service's catalogue duration and price —
   re-run `inventory`, and confirm `staff.missing` and `services.missing` are
   empty. "Empty" covers two different obligations, and only the first is about
   this wave:
   - every selected master, and every service her future bookings use, must be
     mapped — otherwise the wave cannot run;
   - every master and service that already has a live `created` ledger row must
     *still* be mapped, with its catalogue baseline intact, even though she is
     now deferred — otherwise the wave runs and can never be closed.

   On the first wave the second list is empty, because nothing has been migrated
   yet.
5. Cross-check the `wave` section of the next dry-run against Altegio: it counts
   active bookings per branch and per staff id, split into selected, deferred
   and unknown.
6. From wave 2 on, the manifest is the previous wave's file plus the new wave's
   mappings: copy it, change the selector, **add**, never delete. The dry-run
   report's `previous_wave_context` says whether it still proves the earlier
   waves' live rows; `proven: false` names each missing Altegio staff/service id
   and blocks the canary before its first POST.

Never copy an EasyWeek customer export into this file. Customers are resolved at
run time from a separate, git-ignored export passed with `--customer-directory`,
and that export is a **full, fresh** one every wave — never filtered to the
selected masters, because it also has to resolve the customers of earlier waves'
live bookings.

Nothing real goes into the committed example. No production Altegio company,
staff or service ids, no EasyWeek location/staff/service UUIDs, no customer
UUIDs, no names and no phone numbers — placeholders only, including after a wave
has run successfully.

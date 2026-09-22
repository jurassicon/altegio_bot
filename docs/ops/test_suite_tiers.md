# Test suite tiers

Owner decision: §39 (revision 42, 2026-09-21) of the canonical
`docs/easyweek/INTEGRATION_PLAN.md`. Agent-facing summary: the **Test suite
tiers** section of `AGENTS.md`. Both of those are local-only working documents
and are not tracked in this repository; this file is the tracked reference.

The project suite has two tiers. They differ only in *when* they run — never in
how strict they are. No test is deleted, skipped or xfailed to make CI faster.

| Tier | What it is | When it runs |
| --- | --- | --- |
| **Required** | Everything except modules explicitly marked `legacy_altegio`, plus the dedicated migration / Nginx / reminder-handover gates | Every pull request, `CI / Deploy`, reported by job `tests` (`Run Tests`) |
| **Legacy** | Exactly the modules marked `legacy_altegio` | Nightly at 02:23 UTC and on demand, workflow `Legacy Altegio Tests` |

A plain `uv run pytest` is **neither** tier: it is the full project suite, and
it stays that way. The marker filter lives only in the CI invocation, never in
`addopts`.

## Required-gate topology

The required tier executes on **three runners in parallel** and is reported by
a fourth, step-only job:

| Job key | Name | What it runs |
| --- | --- | --- |
| `required-tests-heavy` | Required Tests (heavy shard) | exactly the 16 heaviest general modules, listed explicitly |
| `required-tests-rest` | Required Tests (rest shard) | the whole test root **minus** the dedicated suites **minus** heavy |
| `required-tests-dedicated` | Required Tests (dedicated gates) | the three mandatory gates, under their env flags |
| `tests` | **Run Tests** | nothing — it aggregates the three above |

Each execution job gets its **own** PostgreSQL 16 service. That is also why
`pytest-xdist` is deliberately not used: two workers sharing one database would
race on migrations and on the truncated shared tables. Parallelism here comes
from separate GitHub jobs with separate databases, not from separate processes
against one.

`tests` keeps its key and its name because branch protection refers to those
strings. It runs under `always()` — without that, a failed or cancelled
dependency would *skip* it, and a skipped required check reports as neutral
rather than red — and then fails unless every dependency result is exactly
`success`. Failure, cancelled and skipped are all red.

### Why the split is asymmetric

Only `heavy` is a list. `rest` is a subtraction:

```
heavy = exactly the 16 listed modules
rest  = the whole test root − the 5 dedicated suites − the 16 heavy modules
```

This is the property that makes the union total. A module nobody classified —
**including every test file added tomorrow** — is collected by `rest` and stays
required by default. An allowlist of "light" modules would invert that, and the
first forgotten entry would leave the gate with no failing check to notice it.

`src/altegio_bot/tests/test_ci_required_test_shards.py` refuses a positional
target on the rest shard for exactly this reason, and proves heavy and rest do
not overlap.

## Commands

Full project suite — the local default, unchanged:

```bash
uv run pytest -q
```

Heavy shard, exactly as `required-tests-heavy` runs it:

```bash
uv run pytest -q -m "not legacy_altegio" src/altegio_bot/tests/test_easyweek_inbox_worker_integration.py src/altegio_bot/tests/test_easyweek_outbox_pr5_integration.py src/altegio_bot/tests/test_easyweek_voucher_delivery_runner.py src/altegio_bot/tests/test_easyweek_manual_voucher_canary.py src/altegio_bot/tests/test_easyweek_migration_live_proof.py src/altegio_bot/tests/test_easyweek_migration_rollback_recovery.py src/altegio_bot/tests/test_chatwoot_branch_compose_contract.py src/altegio_bot/tests/test_easyweek_pr4_migration.py src/altegio_bot/tests/test_easyweek_migration_apply.py src/altegio_bot/tests/test_easyweek_multi_service_snapshot_recovery.py src/altegio_bot/tests/test_easyweek_voucher_canary_runner.py src/altegio_bot/tests/test_chatwoot_webhook_sanitization.py src/altegio_bot/tests/test_easyweek_manual_recipient.py src/altegio_bot/tests/test_easyweek_visit_counter.py src/altegio_bot/tests/test_easyweek_migration_cumulative_manifest.py src/altegio_bot/tests/test_easyweek_post_booking_handover.py
```

Rest shard, exactly as `required-tests-rest` runs it:

```bash
uv run pytest -q -m "not legacy_altegio" --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover.py --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover_db.py --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover_safety.py --ignore=src/altegio_bot/tests/test_easyweek_migration_integration.py --ignore=src/altegio_bot/tests/test_nginx_webhook_logging_integration.py --ignore=src/altegio_bot/tests/test_easyweek_inbox_worker_integration.py --ignore=src/altegio_bot/tests/test_easyweek_outbox_pr5_integration.py --ignore=src/altegio_bot/tests/test_easyweek_voucher_delivery_runner.py --ignore=src/altegio_bot/tests/test_easyweek_manual_voucher_canary.py --ignore=src/altegio_bot/tests/test_easyweek_migration_live_proof.py --ignore=src/altegio_bot/tests/test_easyweek_migration_rollback_recovery.py --ignore=src/altegio_bot/tests/test_chatwoot_branch_compose_contract.py --ignore=src/altegio_bot/tests/test_easyweek_pr4_migration.py --ignore=src/altegio_bot/tests/test_easyweek_migration_apply.py --ignore=src/altegio_bot/tests/test_easyweek_multi_service_snapshot_recovery.py --ignore=src/altegio_bot/tests/test_easyweek_voucher_canary_runner.py --ignore=src/altegio_bot/tests/test_chatwoot_webhook_sanitization.py --ignore=src/altegio_bot/tests/test_easyweek_manual_recipient.py --ignore=src/altegio_bot/tests/test_easyweek_visit_counter.py --ignore=src/altegio_bot/tests/test_easyweek_migration_cumulative_manifest.py --ignore=src/altegio_bot/tests/test_easyweek_post_booking_handover.py
```

Legacy tier:

```bash
uv run pytest -q -m legacy_altegio
```

The 21 `--ignore` entries on the rest shard are not an exclusion: every one of
those paths is executed exactly once on another runner — the five dedicated
suites under their mandatory env flags, the sixteen heavy modules on the heavy
runner. Dropping an `--ignore` makes a module run twice; dropping a dedicated
step removes a gate. The dedicated steps are **never** marker-filtered.

### The 16 heavy modules

Chosen from median per-module duration over the three profiling runs below. The
list is a measurement taken beforehand, never a rule the workflow evaluates at
run time: it is not a glob, not "modules whose name contains EasyWeek", and not
anything derived from timings observed during the run being sharded.

```
src/altegio_bot/tests/test_easyweek_inbox_worker_integration.py
src/altegio_bot/tests/test_easyweek_outbox_pr5_integration.py
src/altegio_bot/tests/test_easyweek_voucher_delivery_runner.py
src/altegio_bot/tests/test_easyweek_manual_voucher_canary.py
src/altegio_bot/tests/test_easyweek_migration_live_proof.py
src/altegio_bot/tests/test_easyweek_migration_rollback_recovery.py
src/altegio_bot/tests/test_chatwoot_branch_compose_contract.py
src/altegio_bot/tests/test_easyweek_pr4_migration.py
src/altegio_bot/tests/test_easyweek_migration_apply.py
src/altegio_bot/tests/test_easyweek_multi_service_snapshot_recovery.py
src/altegio_bot/tests/test_easyweek_voucher_canary_runner.py
src/altegio_bot/tests/test_chatwoot_webhook_sanitization.py
src/altegio_bot/tests/test_easyweek_manual_recipient.py
src/altegio_bot/tests/test_easyweek_visit_counter.py
src/altegio_bot/tests/test_easyweek_migration_cumulative_manifest.py
src/altegio_bot/tests/test_easyweek_post_booking_handover.py
```

Re-balancing the shard means editing both the workflow and
`EXPECTED_HEAVY_MODULES` in the shard contract, which is intentional: the two
must move together or the contract fails.

Dedicated required gates, each in its own step with a mandatory env flag and no
`continue-on-error` and no `if:`:

```bash
ALTEGIO_REQUIRE_MIGTEST=1 uv run pytest -q src/altegio_bot/tests/test_easyweek_migration_integration.py
```

```bash
ALTEGIO_REQUIRE_NGINX_LOGTEST=1 uv run pytest -q src/altegio_bot/tests/test_nginx_webhook_logging_integration.py
```

```bash
REQUIRE_PG_CONCURRENCY=1 uv run pytest -q src/altegio_bot/tests/test_easyweek_reminder_handover.py src/altegio_bot/tests/test_easyweek_reminder_handover_db.py src/altegio_bot/tests/test_easyweek_reminder_handover_safety.py
```

## The `legacy_altegio` criterion

A module may carry the marker only when **every test in it** exercises
Altegio-only functionality: behaviour that no EasyWeek path, no shared runtime
component and no security control depends on.

Marking is explicit and per module:

```python
pytestmark = pytest.mark.legacy_altegio
```

If the module already has a `pytestmark`, combine the markers into a list
rather than overwriting the existing one. Keep the statement after the module
docstring, `from __future__` import and the import block, so Ruff's import
sorting stays clean.

`--strict-markers` is on, so a misspelled marker is a collection error, not a
test that silently stays required.

Forbidden mechanisms, without exception:

- `pytest_collection_modifyitems` or any other conftest hook that attaches the
  marker;
- a central list or glob of file names;
- inferring "legacy" from a directory, an import list, or the absence of the
  word `easyweek`.

An unmarked test is required. That is the default, and it is what makes a new
test safe to write without thinking about tiers at all.

## Protected categories — never marked

These stay in the required tier even when the module also contains Altegio
logic. A mixed module is left required; it is not split for convenience.

- **EasyWeek** — anything under `test_easyweek_*` and any EasyWeek code path.
- **Migration / handover** — migration preparation and apply, reminder
  handover, post-booking ownership transfer, graceful drain.
- **Provider isolation** — anything asserting that one provider's rows,
  configuration or jobs do not leak into another's.
- **Security** — webhook auth, secret masking, token redaction, hostile input,
  dedupe, log hygiene.
- **Shared runtime** — Outbox, Chatwoot, Meta, WhatsApp, Nginx/webhook
  handling, and the workers shared between providers.

Concrete modules deliberately left required in wave 1, and why:

| Module | Why it stays required |
| --- | --- |
| `test_altegio_webhook.py` | auth, secret masking, dedupe, hostile input |
| `test_altegio_records.py` | mixed with token-redaction / security |
| `test_inbox_worker_shutdown.py` | migration graceful drain |
| `test_monthly_newsletter_smart.py` | provider isolation |
| `test_repeat_10d_gate.py`, `test_comeback_3d_gate.py`, `test_review_3d_visit_limit.py`, `test_message_planner*` | migration / post-booking safety |
| `campaigns/test_provider_scope.py` | provider isolation |
| `campaigns/test_runner_counters.py` | covers `_update_run_exclusion_counters`, which `run_preview` calls after **both** provider branches — the EasyWeek segment path included |
| `campaigns/test_preview_safety.py`, `campaigns/test_gift_card_readiness.py`, `campaigns/test_auto_hide_preview.py`, `campaigns/test_ops_campaigns.py`, `campaigns/test_progress.py`, `campaigns/test_reports.py`, `campaigns/test_snapshot_edit.py` | preview / readiness surface shared with EasyWeek |
| general Chatwoot, Meta, Outbox, WhatsApp, Nginx/webhook-security suites | shared runtime and security |
| mixed promo / newsletter modules containing provider, security or shared-outbox checks | mixed |

The five preview-adjacent campaign modules above may be reconsidered only in a
later wave, and only once EasyWeek preview is proven to be fully covered by its
own dedicated tests.

## Wave 1 — the modules marked in this PR

24 modules, 508 tests.

```
src/altegio_bot/tests/campaigns/test_card_resolution.py
src/altegio_bot/tests/campaigns/test_card_type_validation.py
src/altegio_bot/tests/campaigns/test_crm_only_send_real.py
src/altegio_bot/tests/campaigns/test_crm_parsing.py
src/altegio_bot/tests/campaigns/test_followup.py
src/altegio_bot/tests/campaigns/test_followup_final_guard_alignment.py
src/altegio_bot/tests/campaigns/test_followup_plan.py
src/altegio_bot/tests/campaigns/test_followup_worker.py
src/altegio_bot/tests/campaigns/test_outstanding_cards.py
src/altegio_bot/tests/campaigns/test_repair_schedule_followups.py
src/altegio_bot/tests/campaigns/test_resume.py
src/altegio_bot/tests/campaigns/test_returned_after_period.py
src/altegio_bot/tests/campaigns/test_segment.py
src/altegio_bot/tests/campaigns/test_segment_attendance.py
src/altegio_bot/tests/campaigns/test_segment_new_features.py
src/altegio_bot/tests/campaigns/test_send_real_run_status.py
src/altegio_bot/tests/campaigns/test_service_filter_cache.py
src/altegio_bot/tests/test_altegio_loyalty.py
src/altegio_bot/tests/test_inbox_worker.py
src/altegio_bot/tests/test_newsletter_new_clients.py
src/altegio_bot/tests/test_promo_discount_apply.py
src/altegio_bot/tests/test_promo_loyalty_cleanup.py
src/altegio_bot/tests/test_find_promo_discount_smoke_candidate_script.py
src/altegio_bot/tests/test_smoke_apply_promo_discount_script.py
```

None of these modules mentions EasyWeek, and every `provider=` literal in them
is `"altegio"`. **That is a supporting signal, not the proof.** A keyword and
provider-literal scan only narrows the candidate set; isolation is established
by reading the production call sites of the code each module exercises and
checking the provider semantics there.

`campaigns/test_runner_counters.py` is why the distinction is written down. It
passed every keyword check — no EasyWeek string, no foreign provider literal,
Altegio-shaped imports — and was marked in the first draft of this wave.
Review traced the call site: `run_preview` invokes
`_update_run_exclusion_counters` unconditionally after the EasyWeek branch and
the Altegio branch converge, so those six tests guard EasyWeek preview as much
as the Altegio path. The module is required, and
`test_ci_legacy_altegio_quarantine.py` now fails if it ever carries the marker
again.

## Adding the next wave

The order is fixed. Doing it in any other order is how a security test ends up
outside the gate.

1. **Prove semantic isolation first.** For each candidate module, show that
   every test in it is Altegio-only: no EasyWeek path, no provider-isolation
   assertion, no security control, no shared-runtime component. Proving it
   means following the production call sites of the code under test and
   checking the provider semantics there — a keyword or `provider=` scan is a
   filter for picking candidates, never the evidence. A module that is only
   *mostly* Altegio stays required.
2. **Then mark**, per module, with `pytestmark`.
3. **Then verify the partition.** Collection counts must satisfy
   `legacy + required = all`, with no overlap and nothing lost:

   ```bash
   uv run pytest --collect-only -q
   ```

   ```bash
   uv run pytest --collect-only -q -m legacy_altegio
   ```

   ```bash
   uv run pytest --collect-only -q -m "not legacy_altegio"
   ```

4. **Then benchmark** the full, legacy and required tiers on one SHA, in one
   environment, with no other pytest running, and record the numbers here.
5. **Then review**, with explicit owner approval for the new composition.

Changing the marked set without updating this document and the benchmark is not
allowed. The contract tests deliberately do not pin the number of legacy tests,
so a later approved wave can grow the set — what they do pin is that the
required gate keeps its identity, its five `--ignore` entries and its dedicated
gates, and that the legacy workflow keeps its schedule-only triggers and no
deploy power.

## Nightly failures

A red `Legacy Altegio Tests` run is a real finding about Altegio behaviour. It
is triaged like any other failure. It is not muted, not marked `xfail`, and the
workflow does not get `continue-on-error`.

## Shard sizing: profiling and projection

Three GitHub Actions runs of the **pre-split** general required invocation, all
on one SHA. This is runner data, not laptop data — the local benchmark further
down measures something else (the Wave 1 marker) on a contended machine.

| Run | Tests | Failures | Errors | pytest time |
| --- | --- | --- | --- | --- |
| 1 | 10 071 | 0 | 0 | 2879.097 s |
| 2 | 10 071 | 0 | 0 | 1593.708 s |
| 3 | 10 071 | 0 | 0 | 1521.105 s |

All three collected the **same set of test IDs**, so the runs differ in speed
only. Run 1 was globally slower — every module in it, not one hot spot — so it
is treated as an outlier of the runner, not as evidence about any test. Nothing
here was tuned against a single anomalous run.

**Median: 1593.708 s = 26:34.** That is the figure the split is sized against.

### Projected critical path

Summing median per-module durations:

| Group | Median total |
| --- | --- |
| the 16 heavy modules | ~800.619 s |
| all remaining general modules | ~766.715 s |

Running those two groups on separate runners puts the general critical path at
roughly **13–14 minutes**, against a 26:34 median today.

**This is a projection, not a measured speed-up.** It is arithmetic over
per-module medians. It ignores per-job setup (checkout, `uv sync --frozen`,
waiting for PostgreSQL), runner scheduling latency, and the ordinary variance
that made run 1 nearly twice run 3. The real number has to be read off three
GitHub runs after this PR is open, exactly as the median above was, and only
then compared. Until that happens, no percentage should be quoted from this
table.

### Trade-off: this buys wall time, not runner minutes

The split does not make the work smaller. It runs the same tests on three
runners instead of one, and each of those runners pays its own checkout,
dependency sync and PostgreSQL startup. Total consumed runner-minutes therefore
go **up**, by roughly the setup cost of two extra jobs plus whatever the
dedicated gates were previously sharing with the general run.

What the split buys is elapsed time to a red or green PR. **No saving of
billable runner-minutes is claimed, and none should be.**

### Pre-existing gate debt: nine plan-gated skips

`docs/easyweek/INTEGRATION_PLAN.md` is in `.gitignore`, so it does not exist on
a CI runner. Nine tests are guarded by
`skipif(not PLAN.exists(), reason="INTEGRATION_PLAN.md is untracked (.gitignore)")`
and therefore skip in every CI run today:

| Module | Plan-gated tests |
| --- | --- |
| `test_easyweek_pr7_4_rollout_contract.py` | 4 |
| `test_easyweek_multi_service_rollout_contract.py` | 2 |
| `test_easyweek_visit_counter_contract.py` | 2 |
| `test_easyweek_failed_cancellation_recovery.py` | 1 |

These predate this PR and are **not** touched by it: the split neither creates
nor removes a skip, and the same nine skip before and after. They are recorded
here as known gate debt — nine contract assertions that no CI run has ever
actually executed — to be resolved separately, not by relaxing anything here.
This PR adds no new skips.

## Benchmark — Wave 1 marker (local, superseded for sizing)

This section records the local measurement that accompanied the Wave 1 marker
split. It is kept as the audit trail for that decision. For sizing the shards,
use the GitHub profiling runs above instead: these numbers come from a
contended laptop and, as the section itself concludes, wall clock there could
not measure the effect at all.

### What was measured, and on what

Every number below comes from the **working tree of that branch**: base commit
`e478a24` (the first-wave quarantine commit) plus the uncommitted review fix
that returns `campaigns/test_runner_counters.py` to the required tier. No
measurement describes a released commit, and none of them was taken on
`5e32cd6`; that SHA is only the `main` tip this branch started from.

"Before" and "after" are **not two commits**. Both D runs execute this same
tree with the same five `--ignore` entries; they differ only in whether
`-m "not legacy_altegio"` is present. That isolates the marker filter and
nothing else.

Environment: macOS 26.6.2, Apple silicon, 8 cores, Python 3.12.12, pytest
9.0.2, local PostgreSQL 14.18 (Homebrew), no other pytest process running.
CI runs PostgreSQL 16 on `ubuntu-latest`. The host carried a load average of
roughly 5–7 on 8 cores from unrelated work throughout the session; that turns
out to matter more than anything else here.

### Inventory, confirmed by collection

|  | Files | Tests |
| --- | --- | --- |
| Full suite (`uv run pytest`) | 213 | 10 612 |
| Marked `legacy_altegio` | 24 | 508 |
| Required tier | 189 | 10 104 |
| CI general run, filter off | 208 | 10 109 |
| CI general run, filter on | 184 | 9 601 |

Partition verified by comparing sorted test-id lists, not by arithmetic:
`508 + 10 104 = 10 612`, no test in both halves, none in neither. The CI
general run drops exactly `10 109 − 9 601 = 508` tests, **5.03 %**.

The earlier working figures of 218 files / 10 912 tests came from an older
tree and are reference only.

### Runs

| Run | What | Tests | Skipped | Failures | pytest time | wall | CPU (user+sys) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | full suite | 10 612 | 121 | 0 | 1029.93 s | 1033.37 s | 193.68 s |
| B | legacy tier | 508 | 0 | 0 | 42.89 s | 44.88 s | 11.22 s |
| C | required tier | 10 104 | 121 | 0 | 1007.62 s | 1010.68 s | 179.02 s |
| D1 | CI general, filter off | 10 109 | 22 | 0 | 1677.73 s | 1680.94 s | 191.46 s |
| D2 | CI general, filter on | 9 601 | 22 | 0 | 778.23 s | 781.71 s | 161.64 s |
| D2b | CI general, filter on (repeat) | 9 601 | 22 | 0 | 1137.78 s | 1140.83 s | 178.28 s |
| D1b | CI general, filter off (repeat) | 10 109 | 22 | 0 | 818.14 s | 821.08 s | 181.81 s |

### Wall clock could not measure this, and says so

The two D pairs were run in opposite orders. They **disagree in sign**:

| Pair | Order | Filter off | Filter on | Wall difference |
| --- | --- | --- | --- | --- |
| 1 | off → on | 1680.94 s | 781.71 s | 899.23 s faster |
| 2 | on → off | 821.08 s | 1140.83 s | 319.75 s **slower** |

Two runs of the *identical* filter-off configuration came out 1680.94 s and
821.08 s — a spread of 859.86 s, which dwarfs anything 508 tests could
plausibly cost. Under the background load on this host, local wall time is
measuring the machine, not the change. **No wall-clock speed-up figure is
claimed from this data, and none should be quoted from it.**

### CPU time is steadier, and still only suggestive

CPU time (`user + sys`) is far less sensitive to contention, and here both
pairs agree in direction:

| Pair | Filter off | Filter on | Saving |
| --- | --- | --- | --- |
| 1 | 191.46 s | 161.64 s | 29.82 s (15.58 %) |
| 2 | 181.81 s | 178.28 s | 3.53 s (1.94 %) |
| mean | 186.63 s | 169.96 s | 16.68 s (8.93 %) |

The mean saving (16.68 s) is the same order as the legacy tier's own
standalone cost (11.22 s CPU / 44.88 s wall), which is the consistency check
one wants. But the spread within a single configuration is up to 16.64 s —
as large as the effect — so this is a direction, not a measurement.

### What can honestly be claimed

- **Exact:** the required CI invocation collects 508 fewer tests, 5.03 %.
- **Supported:** the saving is real and small, of the same order as the legacy
  tier's own cost — roughly 10–45 s of the general run on a laptop.
- **Not claimed:** any percentage speed-up of CI. These are local numbers from
  a contended host. The real effect has to be read off actual GitHub Actions
  run times once this PR is open, and compared there.

### Skips

All 121 skips in the full local run are environment-driven and none is new:

- 92 — `docker daemon is not usable` (`test_nginx_webhook_logging_integration.py`),
  which CI runs under `ALTEGIO_REQUIRE_NGINX_LOGTEST=1` where a skip becomes a
  failure;
- 7 — `test_easyweek_migration_integration.py`, likewise mandatory in CI under
  `ALTEGIO_REQUIRE_MIGTEST=1`;
- 17 — `test_easyweek_pr4_migration.py`, skipped when `DATABASE_URL` is absent
  from the process environment (CI exports it as a job variable);
- 5 — `test_ops_retire_runtime.py`, Docker.

The CI general run shows 22 skips with the filter off and 22 with it on, so
the marker filter changes no skip. The legacy tier runs with zero skips.

## Guardrails

`src/altegio_bot/tests/test_ci_legacy_altegio_quarantine.py` enforces the tier
policy structurally — parsed TOML, parsed YAML and tokenized shell commands,
not substring grep. It fails if the marker is unregistered, if strict markers
are turned off, if `addopts` starts filtering the marker, if the general CI run
loses or duplicates the negative filter, if a dedicated gate acquires one, if
the required job is renamed, if the legacy workflow gains a push/PR trigger or
deploy power, or if any gate gains `continue-on-error`.

`src/altegio_bot/tests/test_ci_workflow_nginx_gate.py` keeps its guarantees
about the Nginx and migration gates and the deploy split. Both files now look
for pytest invocations across the three execution jobs rather than inside
`tests`, since `tests` no longer runs anything. That widens where a gate may be
found; it does not widen what counts as one.

`src/altegio_bot/tests/test_ci_required_test_shards.py` owns the split itself.
It proves the four jobs exist and are pull-request scoped, that each execution
job has its own healthchecked PostgreSQL 16 and a throwaway database, that
heavy runs exactly the sixteen listed modules once each, that rest is a
subtraction and not an allowlist, that heavy and rest do not overlap, that
every path rest ignores is executed somewhere else, that the dedicated gates
keep their mandatory env flags and cannot be made conditional, and that the
aggregator keeps the branch-protection identity and fails closed on any result
other than `success`. It also tests its own parsers against positive and
negative examples, and it pins no total test count — new tests must be addable
without editing a number.

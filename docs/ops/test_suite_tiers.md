# Test suite tiers

Owner decision: §39 (revision 42, 2026-09-21) of the canonical
`docs/easyweek/INTEGRATION_PLAN.md`. Agent-facing summary: the **Test suite
tiers** section of `AGENTS.md`. Both of those are local-only working documents
and are not tracked in this repository; this file is the tracked reference.

The project suite has two tiers. They differ only in *when* they run — never in
how strict they are. No test is deleted, skipped or xfailed to make CI faster.

| Tier | What it is | When it runs |
| --- | --- | --- |
| **Required** | Everything except modules explicitly marked `legacy_altegio`, plus the dedicated migration / Nginx / reminder-handover gates | Every pull request, `CI / Deploy` → job `tests` (`Run Tests`) |
| **Legacy** | Exactly the modules marked `legacy_altegio` | Nightly at 02:23 UTC and on demand, workflow `Legacy Altegio Tests` |

A plain `uv run pytest` is **neither** tier: it is the full project suite, and
it stays that way. The marker filter lives only in the CI invocation, never in
`addopts`.

## Commands

Full project suite — the local default, unchanged:

```bash
uv run pytest -q
```

Required tier, exactly as the pull-request gate runs it:

```bash
uv run pytest -q -m "not legacy_altegio" --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover.py --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover_db.py --ignore=src/altegio_bot/tests/test_easyweek_reminder_handover_safety.py --ignore=src/altegio_bot/tests/test_easyweek_migration_integration.py --ignore=src/altegio_bot/tests/test_nginx_webhook_logging_integration.py
```

Legacy tier:

```bash
uv run pytest -q -m legacy_altegio
```

The five `--ignore` entries are not an exclusion: those suites already ran in
their own mandatory steps earlier in the same job. Dropping an `--ignore` makes
the job run heavy container work twice; dropping a dedicated step removes a
gate. The dedicated steps are **never** marker-filtered.

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
| `campaigns/test_preview_safety.py`, `campaigns/test_gift_card_readiness.py`, `campaigns/test_auto_hide_preview.py`, `campaigns/test_ops_campaigns.py`, `campaigns/test_progress.py`, `campaigns/test_reports.py`, `campaigns/test_snapshot_edit.py` | preview / readiness surface shared with EasyWeek |
| general Chatwoot, Meta, Outbox, WhatsApp, Nginx/webhook-security suites | shared runtime and security |
| mixed promo / newsletter modules containing provider, security or shared-outbox checks | mixed |

The five preview-adjacent campaign modules above may be reconsidered only in a
later wave, and only once EasyWeek preview is proven to be fully covered by its
own dedicated tests.

## Wave 1 — the modules marked in this PR

25 modules, 514 tests.

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
src/altegio_bot/tests/campaigns/test_runner_counters.py
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
is `"altegio"`.

## Adding the next wave

The order is fixed. Doing it in any other order is how a security test ends up
outside the gate.

1. **Prove semantic isolation first.** For each candidate module, show that
   every test in it is Altegio-only: no EasyWeek path, no provider-isolation
   assertion, no security control, no shared-runtime component. A module that
   is only *mostly* Altegio stays required.
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

## Benchmark

All numbers below were taken on one SHA (`5e32cd6`, the tip of `main` when this
PR was branched), in one environment, with no other pytest process running and
against a local test PostgreSQL only.

Environment: macOS 26.6.2, Apple silicon, 8 cores, Python 3.12.12, pytest
9.0.2, local PostgreSQL 14.18 (Homebrew). CI runs PostgreSQL 16 on
`ubuntu-latest`, so these numbers describe this laptop, not GitHub Actions.

### Inventory on this SHA

|  | Files | Tests |
| --- | --- | --- |
| Before (baseline on `main`) | 212 | 10 584 |
| After (adds one contract-test module) | 213 | 10 612 |
| Marked `legacy_altegio` | 25 | 514 |
| Required tier | 188 | 10 098 |

The earlier working figures of 218 files / 10 912 tests came from an older
tree and are reference only; 212 / 10 584 is the measured baseline on this SHA.

Collection partition, verified by comparing sorted test-id lists:
`514 + 10 098 = 10 612`, no test in both halves, no test in neither.

### Runs

| Run | Command | Tests | Skipped | Failures | pytest time | wall (`/usr/bin/time -p`) |
| --- | --- | --- | --- | --- | --- | --- |
| A — full suite | `uv run pytest -q -p no:randomly --durations=50` | 10 612 | 121 | 0 | 871.08 s | 874.17 s |
| B — legacy tier | `… -m legacy_altegio …` | 514 | 0 | 0 | 46.92 s | 48.60 s |
| C — required tier | `… -m "not legacy_altegio" …` | 10 098 | 121 | 0 | 846.53 s | 849.85 s |
| D1 — CI general run, before | five `--ignore`, no marker filter | 10 109 | 22 | 0 | 832.10 s | 835.81 s |
| D2 — CI general run, after | five `--ignore` + `-m "not legacy_altegio"` | 9 595 | 22 | 0 | 710.21 s | 713.99 s |

### Saving, and how much of it to believe

The exact, reproducible part is the collection delta: the required CI
invocation collects **514 fewer tests, 5.08 %** of 10 109.

Wall time is softer. The D1/D2 pair above gives **121.82 s / 14.58 %**. A
second pair was measured in the opposite order to separate the effect from
machine drift:

| Pair | Order | Before | After | Saving |
| --- | --- | --- | --- | --- |
| 1 | before → after | 835.81 s | 713.99 s | 121.82 s (14.58 %) |
| 2 | after → before | 1012.58 s | 918.55 s | 94.03 s (9.29 %) |
| mean | | 924.19 s | 816.27 s | 107.92 s (11.68 %) |

The saving is real and reproducible in direction — the filtered run was faster
in both orderings — but two runs of the *identical* before-configuration
differed by 176.77 s (835.81 s vs 1012.58 s), which is more than the effect
itself. So the honest local statement is: **roughly 1.5–2 minutes, 9–15 %, on
this laptop**, not a precise figure.

Note also that A minus C is only 24.32 s, far less than B's 48.60 s standalone
cost. That is the same drift, plus per-run fixed overhead (environment build,
collection, one-off schema drop/create) that every invocation pays regardless
of how many tests follow.

**This is a local measurement, not a measured CI speed-up.** GitHub Actions
runners have different hardware, a different PostgreSQL and different I/O. The
actual CI effect has to be compared separately from real workflow run times
once this PR is open.

### Skips

All 121 skips in the full local run are environment-driven and none of them is
new:

- 92 — `docker daemon is not usable` (`test_nginx_webhook_logging_integration.py`),
  which CI runs under `ALTEGIO_REQUIRE_NGINX_LOGTEST=1` where a skip becomes a
  failure;
- 7 — `test_easyweek_migration_integration.py`, likewise mandatory in CI under
  `ALTEGIO_REQUIRE_MIGTEST=1`;
- 17 — `test_easyweek_pr4_migration.py`, skipped when `DATABASE_URL` is absent
  from the process environment (CI exports it as a job variable);
- 5 — `test_ops_retire_runtime.py`, Docker.

The required CI invocation shows 22 skips both before and after the change, so
the marker filter changes no skip.

## Guardrails

`src/altegio_bot/tests/test_ci_legacy_altegio_quarantine.py` enforces this
policy structurally — parsed TOML, parsed YAML and tokenized shell commands,
not substring grep. It fails if the marker is unregistered, if strict markers
are turned off, if `addopts` starts filtering the marker, if the general CI run
loses or duplicates the negative filter, if a dedicated gate acquires one, if
the required job is renamed, if the legacy workflow gains a push/PR trigger or
deploy power, or if any gate gains `continue-on-error`.

`src/altegio_bot/tests/test_ci_workflow_nginx_gate.py` keeps its own,
unchanged, guarantees about the Nginx and migration gates and the deploy split.

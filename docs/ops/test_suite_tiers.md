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

## Benchmark

### What was measured, and on what

Every number below comes from the **working tree of this branch**: base commit
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

`src/altegio_bot/tests/test_ci_legacy_altegio_quarantine.py` enforces this
policy structurally — parsed TOML, parsed YAML and tokenized shell commands,
not substring grep. It fails if the marker is unregistered, if strict markers
are turned off, if `addopts` starts filtering the marker, if the general CI run
loses or duplicates the negative filter, if a dedicated gate acquires one, if
the required job is renamed, if the legacy workflow gains a push/PR trigger or
deploy power, or if any gate gains `continue-on-error`.

`src/altegio_bot/tests/test_ci_workflow_nginx_gate.py` keeps its own,
unchanged, guarantees about the Nginx and migration gates and the deploy split.

"""Deployment contract for PR-7 branch-scoped Chatwoot routing."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_COMPOSE = REPO_ROOT / "docker-compose.yml"
CHATWOOT_OVERRIDE = REPO_ROOT / "docker-compose.chatwoot-internal.yml"
ACTIVATION_RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
CHATWOOT_CONSUMERS = ("altegio-outbox-worker", "altegio-whatsapp-inbox-worker")


def test_base_compose_does_not_require_external_chatwoot_network() -> None:
    config = yaml.safe_load(BASE_COMPOSE.read_text())

    assert "chatwoot_internal" not in config.get("networks", {})
    for service_name in CHATWOOT_CONSUMERS:
        networks = config["services"][service_name].get("networks", [])
        assert "chatwoot_internal" not in networks


def test_production_override_attaches_both_workers_to_both_networks() -> None:
    config = yaml.safe_load(CHATWOOT_OVERRIDE.read_text())

    assert config["networks"]["chatwoot_internal"]["external"] is True
    for service_name in CHATWOOT_CONSUMERS:
        assert config["services"][service_name]["networks"] == ["default", "chatwoot_internal"]


def test_activation_runbook_preserves_production_compose_file_set() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert (
        'COMPOSE="docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml"'
    ) in text
    assert "$COMPOSE up -d --force-recreate \\\n  altegio-outbox-worker altegio-whatsapp-inbox-worker" in text
    assert "$COMPOSE up -d --force-recreate \\\n  altegio-easyweek-inbox-worker altegio-outbox-worker" in text
    assert "$COMPOSE stop altegio-outbox-worker" in text
    assert "$COMPOSE up -d altegio-outbox-worker" in text
    assert "$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker" in text


def test_activation_runbook_checks_both_worker_networks_and_api() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    worker_loop = "for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do"
    assert text.count(worker_loop) >= 3
    assert text.count("docker inspect") >= 2
    assert ".NetworkSettings.Networks" in text
    assert "socket.getaddrinfo" in text
    assert '"api_access_token": settings.chatwoot_api_token' in text
    assert '"api_status": response.status_code' in text
    assert "response.text" not in text


def test_activation_runbook_gates_isolated_general_inbox() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert '"global_general_inbox_configured": configured_general_id is not None' in text
    assert '"global_general_inbox_distinct_from_branches": general_inbox_id is not None' in text
    assert "resolve_chatwoot_general_inbox" in text
    assert "general_inbox_overlaps_branch" in text
    assert "**STOP**" in text


def test_activation_runbook_documents_only_the_explicit_identityless_general_routes() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert "STOP ACK, START ACK" in text
    assert "synchronous promo info" in text
    assert "synchronous promo funnel" in text
    assert "Это не EasyWeek promo и не филиальная маршрутизация" in text
    assert "обычный lifecycle send" in text
    assert "неявный fallback в General" in text


def test_official_env_example_uses_provider_scoped_branches_and_separate_general() -> None:
    from altegio_bot.webhooks.common import (
        parse_chatwoot_inbox_company_map,
        resolve_chatwoot_general_inbox,
    )

    lines = ENV_EXAMPLE.read_text().splitlines()
    general_id = int(next(line.split("=", 1)[1] for line in lines if line.startswith("CHATWOOT_INBOX_ID=")))
    example_prefix = "# Example: CHATWOOT_INBOX_COMPANY_MAP="
    raw_map = next(line.removeprefix(example_prefix) for line in lines if line.startswith(example_prefix))

    parsed = parse_chatwoot_inbox_company_map(raw_map)
    assert parsed.configured is True
    assert parsed.valid is True
    assert parsed.provider_scoped is True
    assert general_id not in parsed.mapping
    assert resolve_chatwoot_general_inbox(parsed, general_id) == (general_id, None)


# ---------------------------------------------------------------------------
# PR-7.4 single-inbox hotfix: the rollout has to be executable, not indicative
# ---------------------------------------------------------------------------
#
# The first draft of §14 listed bare `KEY=value` lines. Pasted into a shell those
# set shell variables and vanish: `.env` is untouched, Docker Compose never sees
# them, and the operator believes the rollback is armed when nothing changed.

SINGLE_INBOX_KEYS = (
    "CHATWOOT_INBOX_COMPANY_MAP",
    "CHATWOOT_INBOUND_ROUTING_MODE",
    "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID",
)
RECREATE_BOTH_WORKERS = "$COMPOSE up -d --force-recreate \\\n  altegio-outbox-worker altegio-whatsapp-inbox-worker"


def _single_inbox_section() -> str:
    """Only the PR-7.4 chapter, bounded at both ends like every other one."""
    import re

    text = ACTIVATION_RUNBOOK.read_text()
    start = text.index("## 14. PR-7.4")
    following = re.search(r"^## \d+\. ", text[start + 1 :], flags=re.M)
    end = start + 1 + following.start() if following else len(text)
    return text[start:end]


def test_single_inbox_rollout_edits_the_real_env_file() -> None:
    import re

    section = _single_inbox_section()

    assert "cd /opt/altegio_bot" in section
    assert 'cp -p .env ".env.bak.' in section, "the backup must preserve mode and owner"
    assert "test -f .env" in section
    assert 'cat "$ENV_TMP" > .env' in section, "the edit must land in .env, not in a shell variable"

    for key in SINGLE_INBOX_KEYS:
        # A bare assignment line is a shell variable that Compose never sees.
        assert not re.search(rf"^{key}=", section, flags=re.M), f"{key} is set as a shell variable"


def test_single_inbox_rollout_upserts_without_duplicating_keys() -> None:
    section = _single_inbox_section()
    dedupe = f"grep -Ev '^({'|'.join(SINGLE_INBOX_KEYS)})='"

    # Once for the rollout, once for the rollback.
    assert section.count(dedupe) == 2
    assert section.count("""printf '%s occurrences=%s\\n' "$KEY" "$(grep -Ec "^${KEY}=" .env)\"""") == 2


def test_single_inbox_rollout_validates_the_sender_id_before_writing_it() -> None:
    section = _single_inbox_section()

    assert "grep -Eq '^[1-9][0-9]*$'" in section, "only a positive integer may be written"


def test_single_inbox_rollout_never_prints_env_contents_or_secrets() -> None:
    section = _single_inbox_section()

    for leak in ("cat .env", "cat /opt/altegio_bot/.env", "grep CHATWOOT_API_TOKEN", "printenv"):
        assert leak not in section, leak
    assert "chatwoot_api_token" not in section
    # The only greps that touch .env count matches or strip keys; none print a value.
    assert "grep -Ec" in section and "grep -Ev" in section


def test_single_inbox_hotfix_does_not_touch_easyweek_env() -> None:
    section = _single_inbox_section()

    assert "easyweek.env" in section, "the runbook must say the file is NOT involved"
    assert "не трогает и править его не нужно" in section


def test_single_inbox_rollout_and_rollback_recreate_both_chatwoot_consumers() -> None:
    section = _single_inbox_section()

    assert section.count(RECREATE_BOTH_WORKERS) == 2, "both rollout and rollback recreate both workers"
    assert "restart" not in section.replace("`restart`", ""), "a plain restart does not re-read .env"
    for service_name in CHATWOOT_CONSUMERS:
        assert service_name in section


def test_single_inbox_rollback_restores_all_three_parameters() -> None:
    section = _single_inbox_section()
    rollback = section[section.index("### 14.7") :]

    assert "grep -E '^CHATWOOT_INBOX_COMPANY_MAP=' \"$ENV_BACKUP\"" in rollback, "the exact old map comes back"
    assert "CHATWOOT_INBOUND_ROUTING_MODE=affinity" in rollback
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0" in rollback
    assert "ambiguous_sender" in rollback, "removing only the sender id is explicitly not a rollback"


def test_single_inbox_rollout_and_rollback_both_preflight_both_workers() -> None:
    section = _single_inbox_section()
    worker_loop = "for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do"

    # Support gate, rollout preflight, rollback preflight.
    assert section.count(worker_loop) >= 3
    assert section.count('"single_inbox_sender_supported"') >= 3
    assert section.count('"branch_map_provider_scoped": parsed.provider_scoped') == 2
    assert section.count('"mode": settings.chatwoot_inbound_routing_mode') == 2


def test_single_inbox_preflight_prints_only_safe_configuration_facts() -> None:
    section = _single_inbox_section()
    preflight_start = section.index('"mode": settings.chatwoot_inbound_routing_mode')
    # Bounded by the end of the heredoc, so surrounding prose is not inspected.
    preflight = section[preflight_start : section.index("\nPY\n", preflight_start)]

    for allowed in (
        '"general_inbox_id"',
        '"branch_map_configured"',
        '"branch_map_valid"',
        '"branch_identities"',
        '"single_inbox_sender_id"',
    ):
        assert allowed in preflight, allowed
    for forbidden in ("api_token", "phone_e164", "response.text", "payload"):
        assert forbidden not in preflight, forbidden


def test_single_inbox_stop_conditions_cover_split_worker_configuration() -> None:
    section = _single_inbox_section()

    assert "single_inbox_sender_supported: False" in section
    assert "worker видят разные значения" in section
    assert "один worker остался в `general`" in section
    assert "продолжает видеть пустую карту" in section
    assert "маршрутизируется в филиал технического" in section
    assert "зеркала" in section

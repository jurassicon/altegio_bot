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

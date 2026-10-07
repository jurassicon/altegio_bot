"""§45 owns a separate exact message contract; historical rows stay untouched."""

from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract as historical
from altegio_bot.campaigns.easyweek_voucher_production import template_contract as current
from altegio_bot.campaigns.easyweek_voucher_production.readiness import prove_live_meta_template, prove_prerequisites
from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT
from altegio_bot.models.models import MessageTemplate
from altegio_bot.ops.voucher_mailing import _message_preview
from altegio_bot.scripts import reconcile_easyweek_voucher_template as reconciler
from altegio_bot.settings import settings

COMPANY = 322579
EXPECTED_BODY = """Hallo {{1}}!

Als Dankeschön für Ihren ersten Besuch erhalten Sie einen KitiLash-Gutschein im Wert von 10 €.

Ihr Gutscheincode: {{2}}

Der Gutschein ist ab Aktivierung einen Monat gültig und einmalig einlösbar. Ein Restbetrag verfällt.

Termin buchen:
{{3}}

Bitte zeigen Sie den Gutscheincode bei Ihrem nächsten Besuch vor.

Wenn Sie keine weiteren Nachrichten erhalten möchten, antworten Sie mit STOP."""


def approved(**changes):
    result = {
        "name": "kitilash_ka_new_client_voucher_10eur_v2",
        "language": "de",
        "status": "APPROVED",
        "category": "MARKETING",
        "parameter_format": "POSITIONAL",
        "components": [{"type": "BODY", "text": EXPECTED_BODY}],
    }
    result.update(changes)
    return result


def test_exact_named_body_parameter_order_and_historical_contract_are_separate():
    assert current.positional_body() == EXPECTED_BODY
    assert current.VOUCHER_TEMPLATE_FIELDS == ("client_name", "voucher_code", "booking_link")
    assert current.VOUCHER_TEMPLATE_CODE == CURRENT_PRODUCTION_CONTRACT.message_code
    assert current.VOUCHER_META_TEMPLATE_NAME == CURRENT_PRODUCTION_CONTRACT.meta_template_name
    assert historical.VOUCHER_TEMPLATE_CODE == "new_client_voucher"
    assert "15 €" in historical.VOUCHER_TEMPLATE_BODY
    assert "10 €" not in historical.VOUCHER_TEMPLATE_BODY
    assert current.for_schema("3") is current
    assert current.for_schema("1") is historical
    assert current.for_schema("2") is historical
    with pytest.raises(ValueError):
        current.for_schema("4")


def test_exact_live_approved_passes():
    proof = current.prove_meta_templates([approved()])
    assert proof.proven and proof.meta_verified
    assert proof.as_safe_dict()["template_code"] == "new_client_voucher_10eur_v2"


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "PENDING"},
        {"status": "REJECTED"},
        {"name": "kitilash_ka_new_client_voucher_10eur_v1"},
        {"name": historical.VOUCHER_META_TEMPLATE_NAME},
        {"language": "en"},
        {"category": "UTILITY"},
        {"parameter_format": "NAMED"},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY.replace("einen Monat", "30 Tage")}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY.replace("10 €", "15 €")}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY.replace("{{3}}", "{{4}}")}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY}, {"type": "HEADER", "text": "x"}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY}, {"type": "FOOTER", "text": "x"}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY}, {"type": "BUTTONS", "buttons": []}]},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY}, "malformed"]},
    ],
)
def test_no_fallback_or_terms_drift(changes):
    proof = current.prove_meta_templates([approved(**changes)])
    assert not proof.proven and not proof.meta_verified


def test_duplicate_approval_never_selects_arbitrarily():
    assert not current.prove_meta_templates([approved(), approved()]).proven


@pytest.mark.asyncio
async def test_live_proof_uses_read_adapter_without_any_mutation():
    class Reader:
        calls = 0

        async def list_meta_templates(self):
            self.calls += 1
            return [approved()]

    reader = Reader()
    result = await prove_live_meta_template(reader=reader)
    assert result.proven and result.meta_verified and reader.calls == 1


@pytest.mark.asyncio
async def test_missing_meta_credentials_do_not_fabricate_approval(monkeypatch):
    monkeypatch.setattr(settings, "whatsapp_access_token", "")
    monkeypatch.setattr(settings, "meta_waba_id", "")
    proof = await prove_live_meta_template()
    assert not proof.proven and not proof.meta_verified


@pytest.mark.asyncio
async def test_new_readiness_requires_exact_live_proof_even_with_valid_local_row(session_maker):
    async with session_maker() as session:
        session.add(
            MessageTemplate(
                provider="easyweek",
                company_id=COMPANY,
                code=current.VOUCHER_TEMPLATE_CODE,
                language="de",
                body=current.VOUCHER_TEMPLATE_BODY,
                meta_template_name=current.VOUCHER_META_TEMPLATE_NAME,
                is_active=True,
            )
        )
        await session.commit()
        absent = await prove_prerequisites(
            session,
            stage="create",
            company_id=COMPANY,
            sender_code="karlsruhe",
            schema_version="3",
            require_membership=False,
        )
        pending = await prove_prerequisites(
            session,
            stage="create",
            company_id=COMPANY,
            sender_code="karlsruhe",
            schema_version="3",
            require_membership=False,
            live_meta_proof=current.prove_meta_templates([approved(status="PENDING")]),
        )
        exact = await prove_prerequisites(
            session,
            stage="create",
            company_id=COMPANY,
            sender_code="karlsruhe",
            schema_version="3",
            require_membership=False,
            live_meta_proof=current.prove_meta_templates([approved()]),
        )
    assert absent.template_reason is not None and pending.template_reason is not None
    assert exact.template_reason is None and exact.live_meta_verified
    assert exact.as_safe_dict()["voucher_unit_price_minor"] == 1000
    assert exact.as_safe_dict()["template_code"] == current.VOUCHER_TEMPLATE_CODE


def test_ops_preview_is_scoped_and_states_activation_calendar_month():
    new = _message_preview()
    old = _message_preview("2")
    assert "10 €" in new["body"] and "einen Monat" in new["body"]
    assert "календарный месяц с активации" in new["terms"]
    assert "не с получения WhatsApp" in new["terms"]
    assert "15 €" in old["body"] and "10 €" not in old["body"]
    assert old["meta_template_name"] == historical.VOUCHER_META_TEMPLATE_NAME


class MetaReader:
    def __init__(self, templates):
        self.templates = templates

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def list_templates(self):
        return deepcopy(self.templates)


@pytest.mark.asyncio
@pytest.mark.parametrize("apply", [False, True])
async def test_new_reconciliation_never_modifies_legacy_row(session_maker, monkeypatch, apply):
    monkeypatch.setattr(reconciler, "SessionLocal", session_maker)
    monkeypatch.setattr(reconciler, "MetaTemplateClient", lambda **kwargs: MetaReader([approved()]))
    monkeypatch.setattr(settings, "whatsapp_access_token", "synthetic-token")
    monkeypatch.setattr(settings, "meta_waba_id", "synthetic-waba")
    async with session_maker() as session:
        old = MessageTemplate(
            provider="easyweek",
            company_id=COMPANY,
            code=historical.VOUCHER_TEMPLATE_CODE,
            language="de",
            body=historical.VOUCHER_TEMPLATE_BODY,
            meta_template_name=historical.VOUCHER_META_TEMPLATE_NAME,
            is_active=False,
        )
        session.add(old)
        await session.commit()
        old_id = old.id
    report, status = await reconciler.reconcile(
        company_id=COMPANY, apply=apply, contract=reconciler.PRODUCTION_CONTRACT
    )
    assert report["applied"] is apply
    assert status == (reconciler.EXIT_OK if apply else reconciler.EXIT_BLOCKED)
    async with session_maker() as session:
        rows = list(
            (await session.execute(select(MessageTemplate).where(MessageTemplate.company_id == COMPANY))).scalars()
        )
        historical_row = next(row for row in rows if row.id == old_id)
        assert historical_row.body == historical.VOUCHER_TEMPLATE_BODY
        assert historical_row.meta_template_name == historical.VOUCHER_META_TEMPLATE_NAME
        assert historical_row.is_active is False
        assert len(rows) == (2 if apply else 1)
        if apply:
            added = next(row for row in rows if row.id != old_id)
            assert current.db_row_blocker(added, company_id=COMPANY) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"status": "PENDING"},
        {"status": "REJECTED"},
        {"components": [{"type": "BODY", "text": EXPECTED_BODY.replace("einen Monat", "unbegrenzt")}]},
        {"name": historical.VOUCHER_META_TEMPLATE_NAME},
    ],
)
async def test_new_apply_refuses_without_exact_live_approval(session_maker, monkeypatch, changes):
    monkeypatch.setattr(reconciler, "SessionLocal", session_maker)
    monkeypatch.setattr(reconciler, "MetaTemplateClient", lambda **kwargs: MetaReader([approved(**changes)]))
    monkeypatch.setattr(settings, "whatsapp_access_token", "synthetic-token")
    monkeypatch.setattr(settings, "meta_waba_id", "synthetic-waba")
    report, status = await reconciler.reconcile(company_id=COMPANY, apply=True, contract=reconciler.PRODUCTION_CONTRACT)
    assert status == reconciler.EXIT_BLOCKED and not report["applied"]
    async with session_maker() as session:
        rows = list(
            (await session.execute(select(MessageTemplate).where(MessageTemplate.company_id == COMPANY))).scalars()
        )
    assert rows == []


def test_reconciliation_default_is_historical_and_explicit_new_selector_is_dry_run():
    old = reconciler._parse_args(["--company-id", str(COMPANY)])
    new = reconciler._parse_args(["--company-id", str(COMPANY), "--contract", "production-10eur-v2"])
    assert old.contract == reconciler.LEGACY_CONTRACT and old.apply is False
    assert new.contract == reconciler.PRODUCTION_CONTRACT and new.apply is False


@pytest.mark.asyncio
async def test_frozen_historical_batch_page_preserves_original_message_and_price(
    session_maker, production_configuration, binding_key, ui_client
):
    from altegio_bot.tests import easyweek_voucher_production_fixtures as old
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    run_id, _ = await old.seed_production_preview(session_maker, count=1)
    await old.seed_template_and_sender(session_maker)
    result = await _freeze(session_maker, old.FakeReader(count=1), old.production_request(run_id=run_id), count=1)
    response = await ui_client.get(f"/ops/voucher-mailings/{result.batch['batch_id']}")
    assert response.status_code == 200
    assert historical.VOUCHER_META_TEMPLATE_NAME in response.text
    assert "im Wert von 15 €" in response.text
    assert "const UNIT_PRICE_MINOR = 1500;" in response.text
    assert "im Wert von 10 €" not in response.text
    assert "Исторический ваучер 15 EUR" in response.text
    prepare = await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert "im Wert von 15 €" in prepare.text
    assert "im Wert von 10 €" not in prepare.text


@pytest.mark.asyncio
async def test_campaign_preview_explicitly_resolves_new_message_without_legacy_fallback(
    session_maker, production_configuration, ui_client
):
    from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
    from altegio_bot.tests import easyweek_voucher_production_fixtures as old

    await old.seed_template_and_sender(session_maker)
    params = {"company_id": COMPANY, "contract": "production-10eur-v2"}
    path = "/ops/campaigns/new-clients/easyweek-template-status"
    missing = (await ui_client.get(path, params=params)).json()
    assert missing["configured"] is False
    assert missing["template_code"] == current.VOUCHER_TEMPLATE_CODE
    await new.seed_template_and_sender(session_maker)
    ready = (await ui_client.get(path, params=params)).json()
    assert ready["configured"] is True and ready["meta_template_name"] == current.VOUCHER_META_TEMPLATE_NAME
    legacy = (await ui_client.get(path, params={"company_id": COMPANY})).json()
    assert legacy["configured"] is True and legacy["meta_template_name"] == historical.VOUCHER_META_TEMPLATE_NAME
    response = await ui_client.get(
        "/ops/campaigns/new-clients/template-text",
        params={
            "company_id": COMPANY,
            "provider": "easyweek",
            "template_name": current.VOUCHER_META_TEMPLATE_NAME,
        },
    )
    assert response.status_code == 200
    assert response.json()["body"] == current.VOUCHER_TEMPLATE_BODY
    page = await ui_client.get("/ops/campaigns/new-clients")
    assert "contract=production-10eur-v2" in page.text
    assert current.VOUCHER_TEMPLATE_CODE in page.text


@pytest.mark.asyncio
async def test_campaign_new_message_preview_rejects_ambiguous_or_foreign_row(
    session_maker, production_configuration, ui_client
):
    async with session_maker() as session:
        session.add_all(
            [
                MessageTemplate(
                    provider="easyweek",
                    company_id=COMPANY,
                    code=current.VOUCHER_TEMPLATE_CODE,
                    language="de",
                    body=current.VOUCHER_TEMPLATE_BODY,
                    meta_template_name=current.VOUCHER_META_TEMPLATE_NAME,
                    is_active=True,
                )
                for _ in range(2)
            ]
        )
        await session.commit()
    for company_id in (COMPANY, 123):
        response = await ui_client.get(
            "/ops/campaigns/new-clients/template-text",
            params={
                "company_id": company_id,
                "provider": "easyweek",
                "template_name": current.VOUCHER_META_TEMPLATE_NAME,
            },
        )
        assert response.status_code == 404

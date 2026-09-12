"""Regression tests for the narrow §36 Meta-to-DB reconciler."""

from __future__ import annotations

from sqlalchemy import select

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.models.models import MessageTemplate
from altegio_bot.scripts import reconcile_easyweek_voucher_template as reconciler
from altegio_bot.settings import settings


class _ApprovedMetaClient:
    async def __aenter__(self) -> "_ApprovedMetaClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, object]]:
        return [
            {
                "name": template_contract.VOUCHER_META_TEMPLATE_NAME,
                "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
                "status": "APPROVED",
                "category": template_contract.VOUCHER_TEMPLATE_CATEGORY,
                "parameter_format": template_contract.VOUCHER_TEMPLATE_PARAMETER_FORMAT,
                "components": [
                    {
                        "type": "BODY",
                        "text": template_contract.positional_body(),
                    }
                ],
            }
        ]


async def test_apply_can_create_the_missing_row_after_the_audit_select(
    monkeypatch,
    session_maker,
) -> None:
    """The audit SELECT must not autobegin a transaction outside the write."""
    monkeypatch.setattr(reconciler, "SessionLocal", session_maker)
    monkeypatch.setattr(reconciler, "MetaTemplateClient", lambda **_kwargs: _ApprovedMetaClient())
    monkeypatch.setattr(settings, "whatsapp_access_token", "test-token", raising=False)
    monkeypatch.setattr(settings, "meta_waba_id", "test-waba", raising=False)

    report, exit_code = await reconciler.reconcile(company_id=322579, apply=True)

    assert exit_code == reconciler.EXIT_OK
    assert report["applied"] is True
    assert report["db_row_blocker"] == "row_missing"
    assert report["meta"]["template_proven"] is True

    async with session_maker() as session:
        row = (
            await session.execute(
                select(MessageTemplate).where(
                    MessageTemplate.provider == "easyweek",
                    MessageTemplate.company_id == 322579,
                    MessageTemplate.code == template_contract.VOUCHER_TEMPLATE_CODE,
                    MessageTemplate.language == template_contract.VOUCHER_TEMPLATE_LANGUAGE,
                )
            )
        ).scalar_one()

    assert row.body == template_contract.VOUCHER_TEMPLATE_BODY
    assert row.meta_template_name == template_contract.VOUCHER_META_TEMPLATE_NAME
    assert row.is_active is True

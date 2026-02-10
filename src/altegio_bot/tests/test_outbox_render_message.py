from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from altegio_bot.workers import outbox_worker as ow


@dataclass
class FakeTemplate:
    body: str
    language: str = "de"


@dataclass
class FakeClient:
    id: int
    display_name: str
    phone_e164: str = "+491234567890"


@dataclass
class FakeRecord:
    id: int
    company_id: int
    client_id: int | None
    staff_name: str
    starts_at: datetime | None
    short_link: str = ""


@dataclass
class FakeService:
    title: str
    cost_to_pay: Decimal | None


class FakeScalars:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def all(self) -> list[Any]:
        return self._items


class FakeResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalars(self) -> FakeScalars:
        return FakeScalars(self._items)


class FakeSession:
    def __init__(self, services: list[FakeService]) -> None:
        self._services = services

    async def execute(self, stmt: Any) -> FakeResult:  # noqa: ARG002
        return FakeResult(self._services)


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_render_message_renders_services_and_total_cost(monkeypatch: Any) -> None:
    tmpl = FakeTemplate(
        body=(
            "Hello {client_name}\n"
            "Date {date} Time {time}\n"
            "{services}\n"
            "Total {total_cost}\n"
            "Sender {sender_id}\n"
        )
    )

    async def fake_load_template(*args: Any, **kwargs: Any) -> tuple[Any, str]:
        return tmpl, "de"

    async def fake_sender_code(*args: Any, **kwargs: Any) -> str:
        return "default"

    async def fake_sender_id(*args: Any, **kwargs: Any) -> int:
        return 123

    monkeypatch.setattr(ow, "_load_template", fake_load_template)
    monkeypatch.setattr(ow, "pick_sender_code_for_record", fake_sender_code)
    monkeypatch.setattr(ow, "pick_sender_id", fake_sender_id)
    monkeypatch.setattr(ow, "_fmt_date", lambda dt: "DATE")
    monkeypatch.setattr(ow, "_fmt_time", lambda dt: "TIME")

    session = FakeSession(
        services=[
            FakeService(title="Lashes", cost_to_pay=Decimal("50")),
            FakeService(title="Fix", cost_to_pay=Decimal("30")),
        ]
    )

    record = FakeRecord(
        id=10,
        company_id=758285,
        client_id=1,
        staff_name="Tanja",
        starts_at=datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc),
    )
    client = FakeClient(id=1, display_name="Anna")

    body, sender_id, lang = run(
        ow._render_message(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            record=record,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
        )
    )

    assert sender_id == 123
    assert lang == "de"
    assert "Lashes — 50.00€" in body
    assert "Fix — 30.00€" in body
    assert "Total 80.00" in body


def test_render_message_adds_notes_for_new_client(monkeypatch: Any) -> None:
    tmpl = FakeTemplate(
        body="Hi {client_name}{pre_appointment_notes}\nSender {sender_id}\n"
    )

    async def fake_load_template(*args: Any, **kwargs: Any) -> tuple[Any, str]:
        return tmpl, "de"

    async def fake_sender_code(*args: Any, **kwargs: Any) -> str:
        return "default"

    async def fake_sender_id(*args: Any, **kwargs: Any) -> int:
        return 777

    async def fake_is_new(*args: Any, **kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(ow, "_load_template", fake_load_template)
    monkeypatch.setattr(ow, "pick_sender_code_for_record", fake_sender_code)
    monkeypatch.setattr(ow, "pick_sender_id", fake_sender_id)
    monkeypatch.setattr(ow, "_is_new_client_for_record", fake_is_new)

    session = FakeSession(services=[])

    record = FakeRecord(
        id=11,
        company_id=758285,
        client_id=1,
        staff_name="Tanja",
        starts_at=datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc),
    )
    client = FakeClient(id=1, display_name="Anna")

    body, sender_id, lang = run(
        ow._render_message(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_created",
            record=record,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
        )
    )

    assert sender_id == 777
    assert lang == "de"
    assert ow.PRE_APPOINTMENT_NOTES_DE.strip() in body


def test_render_message_raises_when_no_sender(monkeypatch: Any) -> None:
    tmpl = FakeTemplate(body="Hello {client_name}\n")

    async def fake_load_template(*args: Any, **kwargs: Any) -> tuple[Any, str]:
        return tmpl, "de"

    async def fake_sender_code(*args: Any, **kwargs: Any) -> str:
        return "default"

    async def fake_sender_id(*args: Any, **kwargs: Any) -> Optional[int]:
        return None

    monkeypatch.setattr(ow, "_load_template", fake_load_template)
    monkeypatch.setattr(ow, "pick_sender_code_for_record", fake_sender_code)
    monkeypatch.setattr(ow, "pick_sender_id", fake_sender_id)

    session = FakeSession(services=[])

    record = FakeRecord(
        id=12,
        company_id=758285,
        client_id=1,
        staff_name="Tanja",
        starts_at=None,
    )
    client = FakeClient(id=1, display_name="Anna")

    try:
        run(
            ow._render_message(
                session=session,  # type: ignore[arg-type]
                company_id=758285,
                template_code="record_updated",
                record=record,  # type: ignore[arg-type]
                client=client,  # type: ignore[arg-type]
            )
        )
        raise AssertionError("Expected ValueError, got success")
    except ValueError as exc:
        assert "No active sender" in str(exc)

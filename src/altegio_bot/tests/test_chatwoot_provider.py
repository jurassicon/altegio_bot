"""Unit tests for ChatwootHybridProvider."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider


class _FakeMetaProvider:
    """Stub MetaCloudProvider that records calls."""

    def __init__(self, raise_on_send: bool = False) -> None:
        self.sent: list[tuple[int, str, str, str | None]] = []
        self.templates: list[tuple] = []
        self._raise = raise_on_send

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        if self._raise:
            raise RuntimeError("Meta API failure")
        self.sent.append((sender_id, phone_e164, text, contact_name))
        return f"meta-{uuid4()}"

    async def send_template(
        self,
        sender_id: int,
        phone_e164: str,
        template_name: str,
        language: str,
        params: list[str],
        fallback_text: str = "",
        *,
        contact_name: str | None = None,
        header_image_url: str | None = None,
    ) -> str:
        if self._raise:
            raise RuntimeError("Meta API failure")
        self.templates.append((sender_id, phone_e164, template_name, language, params, fallback_text, header_image_url))
        return f"meta-tpl-{uuid4()}"


class _FakeChatwootClient:
    """Stub ChatwootClient that records calls."""

    def __init__(self, raise_on_log: bool = False) -> None:
        self.notes: list[tuple[str, str]] = []
        self.contact_names: list[str | None] = []
        self.close_calls = 0
        self._raise = raise_on_log

    async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
        if self._raise:
            raise RuntimeError("Chatwoot API failure")
        self.notes.append((phone_e164, text))
        self.contact_names.append(contact_name)

    async def aclose(self) -> None:
        self.close_calls += 1


class _InboxChatwootClient(_FakeChatwootClient):
    def __init__(self, inbox_id: int, *, delay: float = 0.0) -> None:
        super().__init__()
        self.inbox_id = inbox_id
        self.delay = delay

    async def mirror_outbound_as_note(self, phone_e164: str, text: str, *, contact_name: str | None = None) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        await super().mirror_outbound_as_note(phone_e164, text, contact_name=contact_name)


class _InboxClientFactory:
    def __init__(self, *, delay: float = 0.0) -> None:
        self.delay = delay
        self.clients: dict[int, _InboxChatwootClient] = {}

    def __call__(self, inbox_id: int) -> _InboxChatwootClient:
        client = _InboxChatwootClient(inbox_id, delay=self.delay)
        self.clients[inbox_id] = client
        return client


@pytest.mark.asyncio
async def test_send_delegates_to_primary() -> None:
    """Hybrid provider must call the primary Meta provider."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send(1, "+49123", "Hello")
    assert msg_id.startswith("meta-")
    assert len(meta.sent) == 1
    # Allow the fire-and-forget task to run
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_send_fails_if_primary_fails() -> None:
    """If primary fails, the error must propagate."""
    meta = _FakeMetaProvider(raise_on_send=True)
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="Meta API failure"):
        await provider.send(1, "+49123", "Hello")


@pytest.mark.asyncio
async def test_send_continues_if_chatwoot_fails() -> None:
    """If Chatwoot logging fails, the message must still succeed."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient(raise_on_log=True)
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send(1, "+49123", "Hello")
    assert msg_id.startswith("meta-")
    # Give the fire-and-forget task a moment to complete (and swallow the error)
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_send_template_delegates_to_primary() -> None:
    """send_template must use the primary provider."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    msg_id = await provider.send_template(1, "+49123", "my_tpl", "de", ["p1", "p2"])
    assert msg_id.startswith("meta-tpl-")
    assert len(meta.templates) == 1
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_aclose_calls_both() -> None:
    """aclose must close both primary and chatwoot client."""
    meta = _FakeMetaProvider()
    closed_meta = False

    async def _aclose() -> None:
        nonlocal closed_meta
        closed_meta = True

    meta.aclose = _aclose  # type: ignore[method-assign]
    cw = _FakeChatwootClient()
    closed_cw = False

    async def _cw_aclose() -> None:
        nonlocal closed_cw
        closed_cw = True

    cw.aclose = _cw_aclose  # type: ignore[method-assign]
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]
    await provider.aclose()
    assert closed_meta
    assert closed_cw


@pytest.mark.asyncio
async def test_send_propagates_contact_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """contact_name passed to send() must reach _log_to_chatwoot."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    captured_names: list[str | None] = []
    original_log = provider._log_to_chatwoot

    async def _spy_log(
        phone: str,
        text: str,
        *,
        tenant_provider: str | None = None,
        company_id: int = 0,
        contact_name: str | None = None,
        meta: object = None,
    ) -> None:
        captured_names.append(contact_name)
        await original_log(
            phone,
            text,
            tenant_provider=tenant_provider,
            company_id=company_id,
            contact_name=contact_name,
            meta=meta,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(provider, "_log_to_chatwoot", _spy_log)

    await provider.send(1, "+49123", "Hello", contact_name="Anna Müller")
    await asyncio.sleep(0.05)
    assert captured_names == ["Anna Müller"]


@pytest.mark.asyncio
async def test_send_template_forwards_header_image_url_to_primary() -> None:
    """header_image_url must be forwarded to the primary provider, not dropped."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    header_url = "https://cdn.example.com/newsletter_header.jpg"
    msg_id = await provider.send_template(
        1,
        "+49123",
        "kitilash_ka_newsletter_new_clients_monthly_v1",
        "de",
        ["Anna", "https://booking.link/", "Kundenkarte #001"],
        "Fallback text",
        header_image_url=header_url,
    )

    assert msg_id.startswith("meta-tpl-")
    assert len(meta.templates) == 1
    # The 7th element in the recorded tuple is header_image_url
    recorded_header = meta.templates[0][6]
    assert recorded_header == header_url, (
        f"primary.send_template must receive header_image_url={header_url!r}, got {recorded_header!r}"
    )
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_send_template_none_header_image_url_not_forwarded_as_string() -> None:
    """When header_image_url is None (no header template), primary still gets None."""
    meta = _FakeMetaProvider()
    cw = _FakeChatwootClient()
    provider = ChatwootHybridProvider(primary=meta, chatwoot=cw)  # type: ignore[arg-type]

    await provider.send_template(
        1,
        "+49123",
        "kitilash_ka_record_created_v1",
        "de",
        ["Anna", "Tanja", "10.02.2026", "14:00", "Service", "60.00", "https://link"],
        header_image_url=None,
    )

    assert len(meta.templates) == 1
    recorded_header = meta.templates[0][6]
    assert recorded_header is None, (
        f"primary.send_template must get header_image_url=None for non-header templates, got {recorded_header!r}"
    )
    await asyncio.sleep(0.05)


_BRANCH_ROUTES = [
    ("easyweek", 900001, 101, "durlach"),
    ("easyweek", 900002, 102, "rastatt"),
    ("altegio", 900003, 103, "karlsruhe"),
]
_THREE_BRANCH_MAP = (
    '{"101":{"provider":"easyweek","company_id":900001},'
    '"102":{"provider":"easyweek","company_id":900002},'
    '"103":{"provider":"altegio","company_id":900003}}'
)


@pytest.mark.parametrize("method", ["send", "send_template"])
@pytest.mark.parametrize(("tenant_provider", "company_id", "inbox_id", "_branch"), _BRANCH_ROUTES)
async def test_configured_map_routes_each_company_to_its_own_inbox(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    tenant_provider: str,
    company_id: int,
    inbox_id: int,
    _branch: str,
) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    meta = _FakeMetaProvider()
    legacy = _FakeChatwootClient()
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=meta,
        chatwoot=legacy,  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    if method == "send":
        await provider.send(
            1,
            "+49123000000",
            f"{_branch} text",
            tenant_provider=tenant_provider,
            company_id=company_id,
            contact_name=f"{_branch} contact",
        )
    else:
        await provider.send_template(
            1,
            "+49123000000",
            f"{_branch}_template",
            "de",
            ["param"],
            f"{_branch} fallback",
            tenant_provider=tenant_provider,
            company_id=company_id,
            contact_name=f"{_branch} contact",
        )
    await provider.aclose()

    assert legacy.notes == []
    assert set(factory.clients) == {inbox_id}
    assert len(factory.clients[inbox_id].notes) == 1
    assert factory.clients[inbox_id].contact_names == [f"{_branch} contact"]


async def test_concurrent_same_phone_sends_never_cross_du_ra_inboxes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    factory = _InboxClientFactory(delay=0.02)
    legacy = _FakeChatwootClient()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=legacy,  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )
    phone = "+49123456789"

    await asyncio.gather(
        provider.send(1, phone, "DU", tenant_provider="easyweek", company_id=900001),
        provider.send(1, phone, "RA", tenant_provider="easyweek", company_id=900002),
    )
    await provider.aclose()

    assert factory.clients[101].notes == [(phone, "DU")]
    assert factory.clients[102].notes == [(phone, "RA")]
    assert legacy.notes == []


async def test_same_company_id_for_different_providers_routes_to_distinct_inboxes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_map = '{"101":{"provider":"easyweek","company_id":900001},"201":{"provider":"altegio","company_id":900001}}'
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", raw_map)
    meta = _FakeMetaProvider()
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=meta,
        chatwoot=_FakeChatwootClient(),  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    await provider.send(1, "+49123456789", "EW", tenant_provider="easyweek", company_id=900001)
    await provider.send(1, "+49123456789", "ALT", tenant_provider="altegio", company_id=900001)
    await provider.aclose()

    assert len(meta.sent) == 2
    assert factory.clients[101].notes == [("+49123456789", "EW")]
    assert factory.clients[201].notes == [("+49123456789", "ALT")]


@pytest.mark.parametrize(
    ("raw_map", "company_id", "reason"),
    [
        (_THREE_BRANCH_MAP, 999999, "tenant_mapping_missing"),
        (
            '{"101":{"provider":"easyweek","company_id":900001},"102":{"provider":"easyweek","company_id":900001}}',
            900001,
            "invalid_inbox_company_map",
        ),
        ('{"101":900001}', 900001, "provider_scope_missing"),
    ],
)
async def test_configured_unknown_or_invalid_map_never_uses_global_inbox(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    raw_map: str,
    company_id: int,
    reason: str,
) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", raw_map)
    legacy = _FakeChatwootClient()
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=legacy,  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )
    phone = "+49999999999"
    text = "private-message-marker"

    with caplog.at_level("WARNING", logger="altegio_bot.providers.chatwoot_hybrid"):
        msg_id = await provider.send(
            1,
            phone,
            text,
            tenant_provider="easyweek",
            company_id=company_id,
            staff_id=900001,
            contact_name="Private Name",
        )
        await provider.aclose()

    assert msg_id.startswith("meta-")
    assert legacy.notes == []
    assert factory.clients == {}
    assert reason in caplog.text
    assert phone not in caplog.text
    assert text not in caplog.text
    assert "Private Name" not in caplog.text
    assert raw_map not in caplog.text


async def test_empty_map_preserves_legacy_global_inbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", "{}")
    legacy = _FakeChatwootClient()
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=legacy,  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    await provider.send(1, "+49123000000", "legacy", company_id=0)
    await provider.aclose()

    assert legacy.notes == [("+49123000000", "legacy")]
    assert factory.clients == {}


async def test_routed_mirror_keeps_contact_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=_FakeChatwootClient(),  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    await provider.send(
        1,
        "+49123000000",
        "hello",
        tenant_provider="easyweek",
        company_id=900001,
        contact_name="Anna Müller",
    )
    await provider.aclose()

    assert factory.clients[101].contact_names == ["Anna Müller"]


async def test_primary_failure_creates_no_routed_mirror_task(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(raise_on_send=True),
        chatwoot=_FakeChatwootClient(),  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="Meta API failure"):
        await provider.send(
            1,
            "+49123000000",
            "never mirrored",
            tenant_provider="easyweek",
            company_id=900001,
        )

    assert provider._background_tasks == set()
    assert factory.clients == {}
    await provider.aclose()


async def test_primary_template_failure_creates_no_routed_mirror_task(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(raise_on_send=True),
        chatwoot=_FakeChatwootClient(),  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="Meta API failure"):
        await provider.send_template(
            1,
            "+49123000000",
            "template",
            "de",
            ["param"],
            tenant_provider="easyweek",
            company_id=900001,
        )

    assert provider._background_tasks == set()
    assert factory.clients == {}
    await provider.aclose()


async def test_aclose_closes_legacy_and_every_created_inbox_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    legacy = _FakeChatwootClient()
    factory = _InboxClientFactory()
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=legacy,  # type: ignore[arg-type]
        chatwoot_factory=factory,  # type: ignore[arg-type]
    )

    for tenant_provider, company_id, _inbox_id, branch in _BRANCH_ROUTES:
        await provider.send(
            1,
            "+49123000000",
            branch,
            tenant_provider=tenant_provider,
            company_id=company_id,
        )
    await provider.aclose()

    assert legacy.close_calls == 1
    assert set(factory.clients) == {101, 102, 103}
    assert all(client.close_calls == 1 for client in factory.clients.values())


async def test_aclose_does_not_close_same_client_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("altegio_bot.providers.chatwoot_hybrid.settings.chatwoot_inbox_company_map", _THREE_BRANCH_MAP)
    shared = _InboxChatwootClient(101)
    provider = ChatwootHybridProvider(
        primary=_FakeMetaProvider(),
        chatwoot=shared,  # type: ignore[arg-type]
        chatwoot_factory=lambda _inbox_id: shared,  # type: ignore[arg-type]
    )

    await provider.send(1, "+49123000000", "DU", tenant_provider="easyweek", company_id=900001)
    await provider.send(1, "+49123000000", "RA", tenant_provider="easyweek", company_id=900002)
    await provider.aclose()

    assert shared.close_calls == 1

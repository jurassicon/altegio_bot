"""PR-9: the review link, the moment, and the identity — proven directly.

Three decisions live in ``easyweek_review`` and each one reaches a customer if
it is wrong: a link they tap, a message they receive days after a visit, and
whether they receive it twice.

The link gets the most attention here, and from the refusal side. A review URL
is attacker-influenced text that ends up in a WhatsApp message, so every test
below is a way the validator could be talked into returning something it should
not: a different host, a different booking's hash, the MANAGE path, a scheme
that is not https, a port that is a different service behind the same name, a
character that makes a URL render as one thing and resolve as another.

The validator must never raise. A hostile value that becomes an exception in
the caller's error path is a second bug on top of the first, so malformed ports
and malformed IPv6 are asserted to come back as a plain ``None``.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from altegio_bot.easyweek_review import (
    MAX_REVIEW_URL_LEN,
    REVIEW_DELAY,
    easyweek_review_dedupe_key,
    plan_review,
    review_job_payload,
    review_run_at,
    validate_review_url,
)

BOOKING = uuid.UUID("11111111-2222-4333-8444-555555555555")
OTHER_BOOKING = uuid.UUID("99999999-8888-4777-8666-555555555555")
HASH = "40589417"
OTHER_HASH = "90000001"
NOW = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
STARTS_AT = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)
COMPANY_ID = 999001

GOOD_URL = f"https://eyw.me/f/{HASH}"

# PR-10: what the planner is actually handed now — our own Google review link,
# chosen by company_id. EasyWeek never sends a review_url at all.
GOOGLE_URL = "https://g.page/r/CaV0vSmrSYkdEAE/review"


def _url(raw: object, *, booking_hash_id: object = HASH) -> str | None:
    return validate_review_url(raw, booking_hash_id=booking_hash_id)


# ---------------------------------------------------------------------------
# The link: what is accepted
# ---------------------------------------------------------------------------


def test_the_documented_review_url_is_accepted() -> None:
    """EasyWeek's own catalogue pairs this hash with this booking."""
    assert _url(GOOD_URL) == GOOD_URL


@pytest.mark.parametrize("pad", [" ", "\t", "\n", "\u2028"])
def test_edge_whitespace_is_trimmed_but_the_url_is_otherwise_unchanged(pad: str) -> None:
    """Trimming is an EDGE-only allowance, and the result is rebuilt clean.

    An interior copy of any of these is refused (see the table below) — the
    difference matters because a padded value is a copy-paste artefact, while an
    interior one is how a URL renders as one thing and resolves as another.
    """
    assert _url(f"{pad}{GOOD_URL}{pad}") == GOOD_URL


def test_an_explicit_443_is_the_same_origin_and_is_normalised_away() -> None:
    """`:443` and no port are one https origin; the output is canonical."""
    assert _url(f"https://eyw.me:443/f/{HASH}") == GOOD_URL


def test_an_uppercase_host_is_accepted_and_returned_lowercased() -> None:
    assert _url(f"https://EYW.ME/f/{HASH}") == GOOD_URL


@pytest.mark.parametrize("hash_id", ["1", "0040589417", "abc-DEF_123"])
def test_any_bounded_hash_shape_the_booking_proved_is_accepted(hash_id: str) -> None:
    """The hash is the booking's, not a format this module invents."""
    assert _url(f"https://eyw.me/f/{hash_id}", booking_hash_id=hash_id) == f"https://eyw.me/f/{hash_id}"


def test_an_integer_booking_hash_still_matches_its_string_url() -> None:
    """`normalize_booking_hash_id` accepts the numeric column form."""
    assert _url(GOOD_URL, booking_hash_id=int(HASH)) == GOOD_URL


# ---------------------------------------------------------------------------
# The link: everything that is refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "raw"),
    [
        ("http", f"http://eyw.me/f/{HASH}"),
        ("javascript", f"javascript:alert('{HASH}')"),
        ("data", "data:text/html,hi"),
        ("protocol-relative", f"//eyw.me/f/{HASH}"),
        ("no-scheme", f"eyw.me/f/{HASH}"),
        ("other-host", f"https://evil.invalid/f/{HASH}"),
        ("subdomain", f"https://a.eyw.me/f/{HASH}"),
        ("host-prefix", f"https://eyw.me.evil.invalid/f/{HASH}"),
        ("credentials", f"https://user:pw@eyw.me/f/{HASH}"),
        ("username-only", f"https://user@eyw.me/f/{HASH}"),
        ("query", f"https://eyw.me/f/{HASH}?utm=1"),
        ("empty-query", f"https://eyw.me/f/{HASH}?"),
        ("fragment", f"https://eyw.me/f/{HASH}#top"),
        ("other-port", f"https://eyw.me:8443/f/{HASH}"),
        ("port-80", f"https://eyw.me:80/f/{HASH}"),
        ("malformed-port", f"https://eyw.me:notaport/f/{HASH}"),
        ("malformed-ipv6", f"https://[oops/f/{HASH}"),
        ("manage-path", f"https://eyw.me/r/{HASH}"),
        ("no-hash", "https://eyw.me/f/"),
        ("root", "https://eyw.me/"),
        ("extra-segment", f"https://eyw.me/f/{HASH}/extra"),
        ("trailing-slash", f"https://eyw.me/f/{HASH}/"),
        ("encoded-slash", f"https://eyw.me/f/{HASH}%2Fx"),
        ("dot-in-hash", f"https://eyw.me/f/{HASH}.1"),
        ("interior-space", f"https://eyw.me/f/{HASH} x"),
        ("tab-inside", f"https://eyw.me/f/\t{HASH}"),
        ("null-byte", f"https://eyw.me/f/{HASH}\x00"),
        ("bom", f"﻿https://eyw.me/f/{HASH}"),
        ("empty", ""),
        ("whitespace-only", "   "),
    ],
)
def test_every_unsafe_url_shape_is_refused(label: str, raw: str) -> None:
    assert _url(raw) is None, label


def test_an_oversize_url_is_refused_before_it_is_parsed() -> None:
    assert _url("https://eyw.me/f/" + "9" * (MAX_REVIEW_URL_LEN + 10)) is None


@pytest.mark.parametrize("raw", [None, 12345, b"https://eyw.me/f/40589417", ["https://eyw.me/f/40589417"], {}])
def test_a_non_string_url_is_refused(raw: object) -> None:
    assert _url(raw) is None


def test_a_link_for_another_booking_is_refused() -> None:
    """Well-formed, and an invitation to review someone else's appointment."""
    assert _url(f"https://eyw.me/f/{OTHER_HASH}") is None


@pytest.mark.parametrize("booking_hash_id", [None, "", "   ", 0, False, [], {}])
def test_a_booking_with_no_usable_hash_can_prove_nothing(booking_hash_id: object) -> None:
    assert _url(GOOD_URL, booking_hash_id=booking_hash_id) is None


def test_the_validator_never_raises_on_hostile_input() -> None:
    """A refusal must be a value, not an exception in the caller's error path."""
    for raw in ("https://eyw.me:bad/f/1", "https://[/f/1", "https://[::1]:99999/f/1", "https://%%/f/1"):
        assert _url(raw) is None


# ---------------------------------------------------------------------------
# The moment
# ---------------------------------------------------------------------------


def _plan(**overrides):
    kwargs = {
        "booking_uuid": BOOKING,
        "starts_at": STARTS_AT,
        "now": NOW,
        "review_url": GOOGLE_URL,
        "booking_hash_id": HASH,
        "is_deleted": False,
    }
    kwargs.update(overrides)
    return plan_review(**kwargs)


def test_a_review_is_due_exactly_three_days_after_the_appointment() -> None:
    planned = _plan()
    assert planned is not None
    assert planned.run_at == STARTS_AT + timedelta(days=3)
    assert REVIEW_DELAY == timedelta(days=3)


def test_the_due_moment_is_normalised_to_utc() -> None:
    local = STARTS_AT.astimezone(timezone(timedelta(hours=2)))
    assert review_run_at(local) == STARTS_AT + timedelta(days=3)
    assert review_run_at(local).tzinfo is timezone.utc


def test_a_naive_start_is_read_as_utc_rather_than_shifting_the_visit() -> None:
    assert review_run_at(STARTS_AT.replace(tzinfo=None)) == STARTS_AT + timedelta(days=3)


def test_a_cancelled_booking_earns_no_review() -> None:
    assert _plan(is_deleted=True) is None


def test_a_booking_without_a_known_start_earns_no_review() -> None:
    assert _plan(starts_at=None) is None


def test_an_unprovable_link_earns_no_review() -> None:
    # An eyw.me link is no longer the contract: PR-10 sends a Google link.
    assert _plan(review_url=GOOD_URL) is None
    assert _plan(review_url=f"https://eyw.me/r/{HASH}") is None
    assert _plan(review_url=None) is None


def test_a_booking_without_a_proven_hash_earns_no_review() -> None:
    """The hash stopped sourcing the link, but still proves the booking."""
    assert _plan(booking_hash_id=None) is None
    assert _plan(booking_hash_id="  ") is None


@pytest.mark.parametrize("days_ago", [0, 1, 30])
def test_a_moment_already_past_is_never_backfilled(days_ago: int) -> None:
    """Late marketing is not a reminder that slipped; it is unsolicited."""
    assert _plan(now=STARTS_AT + timedelta(days=3) + timedelta(days=days_ago)) is None


def test_the_boundary_is_strictly_in_the_future() -> None:
    assert _plan(now=STARTS_AT + timedelta(days=3)) is None
    assert _plan(now=STARTS_AT + timedelta(days=3) - timedelta(seconds=1)) is not None


def test_a_planned_review_carries_the_normalised_link() -> None:
    planned = _plan(review_url="https://g.page:443/r/CaV0vSmrSYkdEAE/review")
    assert planned is not None
    assert planned.review_url == GOOGLE_URL


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def _key(*, booking: uuid.UUID = BOOKING, starts_at: datetime = STARTS_AT) -> str:
    return easyweek_review_dedupe_key(booking_uuid=booking, starts_at=starts_at)


def test_the_same_business_fact_always_produces_the_same_key() -> None:
    assert _key() == _key()


def test_the_key_ignores_everything_about_the_delivery() -> None:
    """Two succeeded payloads for one visit owe one review, not two.

    The key takes no payload hash, no event id and no delivery id — the only
    inputs it has are the booking and the appointment.
    """
    assert _key() == easyweek_review_dedupe_key(booking_uuid=BOOKING, starts_at=STARTS_AT)


def test_a_different_booking_produces_a_different_key() -> None:
    assert _key() != _key(booking=OTHER_BOOKING)


def test_a_different_appointment_start_produces_a_different_key() -> None:
    assert _key() != _key(starts_at=STARTS_AT + timedelta(hours=1))


def test_the_same_instant_in_another_offset_is_one_fact() -> None:
    assert _key() == _key(starts_at=STARTS_AT.astimezone(timezone(timedelta(hours=2))))


def test_the_key_fits_the_real_column_and_is_namespaced() -> None:
    from altegio_bot.models.models import MessageJob

    limit = MessageJob.__table__.c.dedupe_key.type.length
    key = _key()
    assert key.startswith("easyweek_review:")
    assert "review_3d" in key
    assert len(key) <= limit


def test_the_key_leaks_neither_the_link_nor_anything_customer_facing() -> None:
    key = _key()
    for leak in (HASH, "eyw.me", "http", "Anna", "+49"):
        assert leak not in key


def test_a_planned_review_carries_the_key_it_will_be_inserted_with() -> None:
    planned = _plan()
    assert planned is not None
    assert planned.dedupe_key == _key()


# ---------------------------------------------------------------------------
# The payload
# ---------------------------------------------------------------------------


def _payload(**overrides):
    kwargs = {
        "booking_uuid": BOOKING,
        "company_id": COMPANY_ID,
        "starts_at": STARTS_AT,
        "review_url": GOOGLE_URL,
    }
    kwargs.update(overrides)
    return review_job_payload(**kwargs)


def test_the_payload_carries_only_technical_identity() -> None:
    assert set(_payload()) == {
        "provider",
        "company_id",
        "booking_uuid",
        "record_starts_at",
        "review_url",
        "job_type",
    }


def test_the_payload_values_are_canonical() -> None:
    payload = _payload(starts_at=STARTS_AT.astimezone(timezone(timedelta(hours=2))))
    assert payload["provider"] == "easyweek"
    assert payload["job_type"] == "review_3d"
    assert payload["booking_uuid"] == str(BOOKING)
    parsed = datetime.fromisoformat(str(payload["record_starts_at"]))
    assert parsed.tzinfo is not None and parsed == STARTS_AT


def test_the_source_markers_are_audit_only_and_optional() -> None:
    """They record where a review came from; nothing decides anything on them."""
    plain = _payload()
    assert "source_event_id" not in plain and "source_payload_hash" not in plain

    marked = _payload(source_event_id=42, source_payload_hash="abc123")
    assert marked["source_event_id"] == 42
    assert marked["source_payload_hash"] == "abc123"
    # The identity fields are byte-identical either way: an audit marker must
    # not be able to change what the job is.
    assert {k: marked[k] for k in plain} == plain


def test_the_payload_holds_no_customer_data() -> None:
    text = str(_payload(source_event_id=1, source_payload_hash="h"))
    for forbidden in ("name", "phone", "email", "service", "price", "title", "comment", "customer"):
        assert forbidden not in text.lower()


# ---------------------------------------------------------------------------
# PR-10: the Google link, and where it comes from
# ---------------------------------------------------------------------------
#
# Production settled the question: a `booking-succeeded` payload for Durlach
# (company 308697, record 6922) has 88 root keys and no `review_url`, so PR-9
# could never plan a job. The link now comes from our own configuration, keyed
# by EasyWeek company_id — a customer taps it, so a near-miss sends them to
# another salon's review form.

from altegio_bot.easyweek_review import (  # noqa: E402
    REVIEW_LINK_MISSING,
    REVIEW_LINKS_INVALID,
    REVIEW_LINKS_UNCONFIGURED,
    google_review_url_for_company,
    parse_google_review_links,
    validate_google_review_url,
)

GOOGLE_TOKEN = "CaV0vSmrSYkdEAE"
DURLACH_COMPANY = 308697
RASTATT_COMPANY = 308698
OTHER_GOOGLE_URL = "https://g.page/r/OtherTokenXYZ/review"


def test_the_real_durlach_review_link_is_accepted() -> None:
    assert validate_google_review_url(GOOGLE_URL) == GOOGLE_URL


def test_an_explicit_443_is_the_same_origin() -> None:
    assert validate_google_review_url(f"https://g.page:443/r/{GOOGLE_TOKEN}/review") == GOOGLE_URL


@pytest.mark.parametrize(
    ("label", "raw"),
    [
        ("http", f"http://g.page/r/{GOOGLE_TOKEN}/review"),
        ("foreign-host", f"https://evil.com/r/{GOOGLE_TOKEN}/review"),
        ("subdomain-suffix", f"https://g.page.evil.com/r/{GOOGLE_TOKEN}/review"),
        ("subdomain-prefix", f"https://www.g.page/r/{GOOGLE_TOKEN}/review"),
        # Cyrillic 'е' — visually identical, a different salon entirely.
        ("homoglyph", "https://g.pag" + chr(0x435) + f"/r/{GOOGLE_TOKEN}/review"),
        ("no-review-suffix", f"https://g.page/r/{GOOGLE_TOKEN}"),
        ("manage-path", f"https://g.page/f/{GOOGLE_TOKEN}/review"),
        ("extra-segment", f"https://g.page/r/{GOOGLE_TOKEN}/extra/review"),
        ("empty-token", "https://g.page/r//review"),
        ("bad-token-char", f"https://g.page/r/{GOOGLE_TOKEN}!/review"),
        ("empty-query", f"https://g.page/r/{GOOGLE_TOKEN}/review?"),
        ("query", f"https://g.page/r/{GOOGLE_TOKEN}/review?placeid=1"),
        ("fragment", f"https://g.page/r/{GOOGLE_TOKEN}/review#x"),
        ("credentials", f"https://user:pw@g.page/r/{GOOGLE_TOKEN}/review"),
        ("odd-port", f"https://g.page:8443/r/{GOOGLE_TOKEN}/review"),
        ("interior-space", f"https://g.page/r/{GOOGLE_TOKEN} /review"),
        ("tab", f"https://g.page/r/{GOOGLE_TOKEN}/review" + chr(9)),
        ("newline", f"https://g.page/r/{GOOGLE_TOKEN}/review" + chr(10)),
        ("too-long", "https://g.page/r/" + "A" * 600 + "/review"),
        ("leading-space", f"  https://g.page/r/{GOOGLE_TOKEN}/review"),
        ("trailing-space", f"https://g.page/r/{GOOGLE_TOKEN}/review  "),
        ("empty", ""),
        ("not-a-string", 12345),
        ("none", None),
        # A query is required by this form, and this contract forbids queries.
        ("search-google-form", "https://search.google.com/local/writereview?placeid=X"),
    ],
)
def test_every_near_miss_link_is_refused(label: str, raw: object) -> None:
    assert validate_google_review_url(raw) is None, f"{label} was accepted"


# ── the configuration map ─────────────────────────────────────────────────


def test_a_two_branch_map_parses() -> None:
    parsed = parse_google_review_links(
        '{"%d": "%s", "%d": "%s"}' % (DURLACH_COMPANY, GOOGLE_URL, RASTATT_COMPANY, OTHER_GOOGLE_URL)
    )

    assert parsed.ready is True
    assert parsed.links == {DURLACH_COMPANY: GOOGLE_URL, RASTATT_COMPANY: OTHER_GOOGLE_URL}


@pytest.mark.parametrize("raw", ["", "   ", "{}", None])
def test_an_absent_map_is_unconfigured_not_invalid(raw: object) -> None:
    parsed = parse_google_review_links(raw)

    assert parsed.configured is False
    assert parsed.ready is False
    assert parsed.unavailable_reason == REVIEW_LINKS_UNCONFIGURED


@pytest.mark.parametrize(
    ("label", "raw"),
    [
        ("not-json", "not json at all"),
        ("array", '[{"308697": "x"}]'),
        ("string", '"308697"'),
        ("number", "42"),
        ("null", "null"),
        ("duplicate-key", '{"308697": "%s", "308697": "%s"}' % (GOOGLE_URL, OTHER_GOOGLE_URL)),
        ("non-numeric-key", '{"durlach": "%s"}' % GOOGLE_URL),
        ("negative-key", '{"-5": "%s"}' % GOOGLE_URL),
        ("zero-key", '{"0": "%s"}' % GOOGLE_URL),
        ("bad-url-value", '{"308697": "http://g.page/r/T/review"}'),
        ("non-string-value", '{"308697": 42}'),
        ("null-value", '{"308697": null}'),
    ],
)
def test_one_bad_entry_invalidates_the_whole_map(label: str, raw: str) -> None:
    """All-or-nothing: a silently shrunken map looks like "not configured"."""
    parsed = parse_google_review_links(raw)

    assert parsed.valid is False, f"{label} was accepted"
    assert parsed.links == {}
    assert parsed.unavailable_reason == REVIEW_LINKS_INVALID


def test_the_parser_never_raises_on_hostile_input() -> None:
    for raw in ["[1]", "[[]]", '{"a"', "{" * 200, b"bytes", object(), 3.14, True]:
        parse_google_review_links(raw)


# ── resolution by company ─────────────────────────────────────────────────


def test_the_branch_link_is_chosen_by_company_id() -> None:
    config = '{"%d": "%s", "%d": "%s"}' % (DURLACH_COMPANY, GOOGLE_URL, RASTATT_COMPANY, OTHER_GOOGLE_URL)

    assert google_review_url_for_company(DURLACH_COMPANY, config) == (GOOGLE_URL, None)
    assert google_review_url_for_company(RASTATT_COMPANY, config) == (OTHER_GOOGLE_URL, None)


def test_a_branch_without_a_link_is_missing_not_invalid() -> None:
    url, reason = google_review_url_for_company(999999, '{"%d": "%s"}' % (DURLACH_COMPANY, GOOGLE_URL))

    assert url is None
    assert reason == REVIEW_LINK_MISSING


def test_an_unusable_map_blocks_every_branch() -> None:
    url, reason = google_review_url_for_company(DURLACH_COMPANY, "{not json")

    assert url is None
    assert reason == REVIEW_LINKS_INVALID


def test_an_easyweek_company_is_never_read_from_the_altegio_map() -> None:
    """Provider isolation: the two id spaces overlap and must not be mixed."""
    from altegio_bot.workers.outbox_worker import GOOGLE_MAPS_REVIEW_LINKS

    assert DURLACH_COMPANY not in GOOGLE_MAPS_REVIEW_LINKS
    # And the EasyWeek link is unreachable through the Altegio map.
    url, _reason = google_review_url_for_company(DURLACH_COMPANY, "")
    assert url is None


def test_a_good_entry_beside_a_bad_one_invalidates_both() -> None:
    """The dangerous shape: a map that silently shrinks.

    If the bad entry were merely skipped, the surviving branch would look
    perfectly configured while the dropped one looked like "no link set" — and
    would go quietly unreviewed forever. All or nothing.
    """
    parsed = parse_google_review_links(
        '{"%d": "%s", "%d": "http://g.page/r/T/review"}' % (DURLACH_COMPANY, GOOGLE_URL, RASTATT_COMPANY)
    )

    assert parsed.valid is False
    assert parsed.links == {}, "no branch may survive a partially valid map"
    assert parsed.ready is False


def test_a_good_entry_beside_an_unusable_key_invalidates_both() -> None:
    parsed = parse_google_review_links('{"%d": "%s", "durlach": "%s"}' % (DURLACH_COMPANY, GOOGLE_URL, GOOGLE_URL))

    assert parsed.valid is False
    assert parsed.links == {}

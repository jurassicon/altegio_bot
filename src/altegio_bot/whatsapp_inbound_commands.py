"""One definition of what an inbound WhatsApp message says (§46.4.1).

Why this is not in the worker
-----------------------------
Two processes now have to agree on what "this message is a STOP" means. The
inbox worker decides it in order to act, and the webhook ingress decides it in
order to write down, before answering 200, that an address said something that
has not been accounted for yet. Two parsers would eventually disagree, and the
direction of the disagreement would be a message sent to somebody who had
already asked to be left alone.

So the keyword sets and the tokenisation live here, the worker imports them, and
``whatsapp_inbox_worker`` keeps its historical private names as thin aliases so
existing importers and tests are unaffected.

What is recognised, and what deliberately is not
------------------------------------------------
The command is the FIRST whitespace-separated token, which is the rule this
project already had: ``STOP please`` is an unsubscribe and so is ``СТОП, bitte``.
Case is folded, runs of whitespace collapse, and ordinary punctuation is removed
from the EDGES of that first token — so ``"stop"``, ``(stop)``, ``*STOP*`` and
``«СТОП»`` are all the same command.

Edge-stripping cannot invent a command: it only ever removes characters from the
outside of the token, so ``nonstop``, ``stop-messages`` and ``bitte stop`` stay
exactly what they were — not a command, not a command, and not a command,
because the token containing the word is not the first one or is not the word.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Collection, Final

from altegio_bot.settings import settings
from altegio_bot.webhooks.common import list_or_empty, mapping_or_empty

# Both spellings of the same request, and the historical aliases this project
# already supported. The Cyrillic «стоп» is new in PR-22.1 and is equal to
# `stop` in every respect: same first-token rule, same casing, same punctuation.
STOP_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "stop",
        "стоп",
        "unsubscribe",
        "unsub",
        "abmelden",
        "отписка",
        "отпиши",
        "odjava",
    }
)

# Deliberately NOT extended. A STOP is a refusal and a wider net only ever costs
# an unsent marketing message; a START is permission, and every additional
# spelling is another way for permission to be claimed. The existing contract
# stays exactly as it was.
START_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "start",
        "subscribe",
        "anmelden",
        "подписка",
        "подпиши",
        "prijava",
    }
)

# Punctuation that may sit around a word without changing it. ASCII quotes and
# brackets were already stripped from the whole message; the typographic quotes,
# guillemets, dashes and the three WhatsApp formatting marks (``*_~``) are added
# here because a real person writing «СТОП» or *STOP* means STOP.
_EDGE_PUNCTUATION: Final = " \t\n\r.,!?:;\"'()[]{}<>«»‹›„“”‚‘’-–—…*_~/\\|"

# A plausible unix second. Not "around now": a constant window, so the same
# stored payload parses the same way whenever it is read.
_MIN_EVENT_EPOCH: Final = 1_000_000_000  # 2001-09-09
_MAX_EVENT_EPOCH: Final = 4_000_000_000  # 2096-10-02

# How far ahead of our own clock a claimed event time may sit and still be
# treated as proven. Beyond this it is not a clock difference, it is a number
# the sender chose, and a number the sender chose must not decide an ordering.
_MAX_EVENT_CLOCK_SKEW: Final = timedelta(minutes=5)


def normalize_command_text(raw: str | None) -> str:
    """Fold case, collapse whitespace, trim ordinary punctuation.

    The historical normaliser, unchanged in behaviour and moved here so one
    definition serves both the worker and the ingress.
    """
    if not raw:
        return ""

    text = raw.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" \t\n\r.,!?:;\"'()[]{}")
    return text


def first_command_token(text: str | None) -> str:
    """The first whitespace token of *text*, with punctuation trimmed off it.

    Trimming the token as well as the whole message is what makes ``«СТОП»
    bitte`` work: the closing guillemet of that message is not at the end of the
    message, so only a per-token trim can reach it.
    """
    norm = normalize_command_text(text)
    if not norm:
        return ""
    return norm.split(" ", 1)[0].strip(_EDGE_PUNCTUATION)


def consent_command(text: str | None) -> str | None:
    """``"stop"``, ``"start"`` or ``None`` for one message's text.

    Consent only. The promo funnel keeps its own, settings-driven keywords in
    the worker, because a promo word is a marketing trigger and has no business
    being decided by the module that decides whether marketing is allowed.
    """
    token = first_command_token(text)
    if not token:
        return None
    if token in STOP_KEYWORDS:
        return "stop"
    if token in START_KEYWORDS:
        return "start"
    return None


def extract_message_text(msg: dict[str, Any]) -> str:
    """The human-readable text of one inbound message, or ``""``.

    Every nested field is sender-controlled: ``msg.get("text") or {}`` does not
    protect against ``"text": []``/``"text": "bad"`` (truthy non-dict →
    ``.get()`` raises). ``mapping_or_empty`` degrades any non-dict to ``""``
    instead of a crash.
    """
    msg_type = msg.get("type")

    if msg_type == "text":
        text = mapping_or_empty(msg.get("text"))
        return str(text.get("body") or "")

    if msg_type == "button":
        btn = mapping_or_empty(msg.get("button"))
        return str(btn.get("text") or btn.get("payload") or "")

    if msg_type == "interactive":
        inter = mapping_or_empty(msg.get("interactive"))
        btn_reply = mapping_or_empty(inter.get("button_reply"))
        list_reply = mapping_or_empty(inter.get("list_reply"))
        return str(
            btn_reply.get("title") or btn_reply.get("id") or list_reply.get("title") or list_reply.get("id") or ""
        )

    return ""


def parse_event_timestamp(value: object, *, observed_at: datetime | None = None) -> datetime | None:
    """The moment the SENDER says this happened, or ``None`` when unprovable.

    ``None`` is the answer for an absent, non-numeric, out-of-range or
    implausibly future timestamp — and ``None`` is not a small loss here, it is
    the whole point: a START may only lift a refusal when its own event time and
    the refusal's are both known and strictly ordered, so an unparseable
    timestamp makes the START harmless rather than makes it win.

    ``bool`` is excluded explicitly: it is an ``int`` in Python and ``True``
    would otherwise parse as one second past the epoch.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        seconds = value
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate.isdigit():
            return None
        try:
            seconds = int(candidate)
        except ValueError:
            return None
    else:
        return None

    if not (_MIN_EVENT_EPOCH <= seconds <= _MAX_EVENT_EPOCH):
        return None

    moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if observed_at is not None and moment > observed_at + _MAX_EVENT_CLOCK_SKEW:
        return None
    return moment


def sender_route_of_change(change: object) -> str | None:
    """The Meta sender route one ``changes[]`` entry names, or ``None``.

    Per change, because that is the granularity Meta actually sends: one
    delivery may carry several changes, each with its own
    ``value.metadata.phone_number_id``. Reading only the first one and applying
    its verdict to the whole payload is what let a refusal from an allowed
    sender be dropped behind a disallowed one, and a command from a disallowed
    sender be executed behind an allowed one.
    """
    if not isinstance(change, dict):
        return None
    value = change.get("value")
    if not isinstance(value, dict):
        return None
    metadata = mapping_or_empty(value.get("metadata"))
    raw = metadata.get("phone_number_id")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def payload_sender_routes(payload: dict[str, Any]) -> list[str | None]:
    """One entry per ``changes[]`` of *payload*, in document order.

    ``None`` for a change that names no route at all, which is kept rather than
    dropped: "this change named nothing" is the historical not-ignored case and
    has to stay distinguishable from "this change named a route we refuse".
    """
    routes: list[str | None] = []
    for entry in list_or_empty(payload.get("entry")):
        if not isinstance(entry, dict):
            continue
        for change in list_or_empty(entry.get("changes")):
            if not isinstance(change, dict):
                continue
            routes.append(sender_route_of_change(change))
    return routes


def allowed_sender_routes() -> set[str]:
    """The Meta sender routes this deployment accepts, from settings.

    One definition, read by the ingress when it judges a delivery and by the
    inbox worker when it acts on one, because two readings of the same setting
    would eventually disagree about which sender is ours — and the direction of
    that disagreement is either a lost refusal or an executed command from a
    sender this deployment does not serve.
    """
    raw = getattr(settings, "whatsapp_allowed_phone_number_ids", "")
    ids = [value.strip() for value in str(raw).split(",")] if raw else []
    ids = [value for value in ids if value]
    if not ids and settings.meta_wa_phone_number_id:
        ids = [str(settings.meta_wa_phone_number_id).strip()]
    return {value for value in ids if value}


def route_allowed(phone_number_id: str | None, allowed: Collection[str] | None) -> bool:
    """The historical allow rule, asked about ONE route instead of a payload.

    Exactly the ingress's own condition — an unlisted ``phone_number_id`` is
    refused, an empty allow list means "no filter configured" and a change that
    names no route is not refused on a guess — moved here so the ingress, the
    inbox worker and the historical reconciliation cannot answer it differently.
    """
    if not allowed:
        return True
    if phone_number_id is None:
        return True
    return phone_number_id in allowed


__all__ = [
    "START_KEYWORDS",
    "allowed_sender_routes",
    "STOP_KEYWORDS",
    "consent_command",
    "extract_message_text",
    "first_command_token",
    "normalize_command_text",
    "parse_event_timestamp",
    "payload_sender_routes",
    "route_allowed",
    "sender_route_of_change",
]

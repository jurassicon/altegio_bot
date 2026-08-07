"""Clone approved Meta WhatsApp templates from one KitiLash location to another.

The script reads templates directly from the WhatsApp Business Account, copies
only approved German templates whose names start with the source location
prefix, replaces the Karlsruhe address and Google Maps URL, and submits the new
location-prefixed variants for Meta review.

The WABA, the bot number and the contact phone are shared by every location, so
the address and the Google Maps link are the only strings that are rewritten.

Meta review takes weeks and the name of a rejected template cannot be reused, so
every check below fails the whole run before the first POST rather than sending
something that would have to be fixed by a new version afterwards.

Safety defaults:
* dry-run unless --apply is supplied, and --apply demands an explicit target;
* the target address and map link are validated before anything is read from
  Meta: empty, control-charactered, placeholder-carrying or copied-from-source
  values are refused. Every other guard here watches the SOURCE side and would
  happily accept a replacement with an empty string;
* the full POST payload is printed before the confirmation is asked for, so
  --apply never submits anything the operator has not seen;
* a template with neither the address nor the Karlsruhe map link is genuinely
  location-neutral and is skipped;
* a template whose address was not recognised while its Karlsruhe map link WAS
  found is a contradiction, not neutrality: it is reported as an error;
* a branch-specific template whose Karlsruhe map link was not replaced is
  BLOCKED and never submitted — an approved template pointing at the wrong shop
  is worse than a missing one, because the outbox fails closed on a missing
  template while a wrong map silently misroutes clients;
* the finished payload is rescanned for source markers, so anything the
  replacement did not reach blocks the run instead of shipping;
* the {{n}} placeholder signature must survive the rewrite unchanged, because
  LIFECYCLE_PARAM_FIELDS binds template parameters by position;
* every branch-specific template known to ``meta_templates`` must be covered;
  anything missing aborts the run with a non-zero exit code;
* an existing target is a safe skip only while it is APPROVED or PENDING;
  REJECTED/PAUSED/DISABLED targets block the run instead of reporting success;
* source templates are never edited or deleted.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import re
import sys
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

import httpx

from altegio_bot.meta_templates import (
    META_TEMPLATE_MAP,
    UNIVERSAL_JOB_TYPES,
    resolve_meta_template,
)

DEFAULT_GRAPH_URL = "https://graph.facebook.com"
DEFAULT_API_VERSION = "v25.0"
DEFAULT_LANGUAGE = "de"
DEFAULT_SOURCE_LOCATION = "ka"
DEFAULT_TARGET_LOCATION = "du"
DEFAULT_TARGET_ADDRESS = "Pfinztalstraße 4, 76227 Karlsruhe-Durlach"
DEFAULT_TARGET_MAPS_URL = "https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8"

# The only host that may ever see the WABA access token.
ALLOWED_GRAPH_HOST = "graph.facebook.com"

# Current Karlsruhe spellings found in the repository/Meta template examples.
SOURCE_ADDRESS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"76133\s+Karlsruhe,\s*Kaiserstraße,?\s*68"),
    re.compile(r"Kaiserstraße,?\s*68,\s*76133\s+Karlsruhe"),
)
SOURCE_MAP_URLS: tuple[str, ...] = (
    "https://goo.gl/maps/p7quWqbAqY9cusuRA",
    "https://maps.app.goo.gl/p7quWqbAqY9cusuRA",
)

# Meta positional parameters. The rewrite must not add, drop or move one.
PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*[^{}]+?\s*\}\}")

# Anything unprintable in an operator-supplied value: a stray \r or \x00 would be
# pasted straight into an approved footer.
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")

# GET /message_templates can contain server-side fields that are not accepted by
# POST. Keep only fields used by template creation payloads.
_COMPONENT_KEYS = frozenset(
    {
        "type",
        "format",
        "text",
        "example",
        "buttons",
        "add_security_recommendation",
        "code_expiration_minutes",
    }
)
# Component keys whose strings may carry the address or the map link. The
# residual scan below covers the WHOLE payload, so a marker sitting under a key
# that is not listed here blocks the run instead of slipping through.
_REPLACEABLE_COMPONENT_KEYS = frozenset({"text", "example", "buttons"})
_TEMPLATE_KEYS = frozenset({"category", "components", "language", "name", "parameter_format"})

# An existing target template is a safe skip only in these states. Anything else
# (REJECTED, PAUSED, DISABLED, an unknown future state) needs a human first.
REUSABLE_TARGET_STATUSES = frozenset({"APPROVED", "PENDING"})

# Only Altegio locations need the dedicated new-client variant: outbox_worker
# resolves it through resolve_meta_template(..., is_new_client=...) in its
# non-EasyWeek branch, while an EasyWeek location takes the template name from
# message_templates.meta_template_name at runtime.
ALTEGIO_ONLY_SUFFIX = "_record_created_new_client_v1"
_ALTEGIO_ONLY_NOTE = "Altegio-only: EasyWeek locations read meta_template_name from message_templates"

_PAGE_LIMIT = 100
_MAX_TEMPLATE_PAGES = 50


class ScriptError(RuntimeError):
    """Expected configuration/API error with a user-safe message."""


class TemplateStatus(StrEnum):
    """Outcome of inspecting one source template."""

    READY = "READY"
    NEUTRAL = "NEUTRAL"
    ADDRESS_UNRECOGNIZED = "ADDRESS_UNRECOGNIZED"
    BLOCKED_NO_MAPS = "BLOCKED_NO_MAPS"
    RESIDUAL_SOURCE = "RESIDUAL_SOURCE"
    PLACEHOLDER_MISMATCH = "PLACEHOLDER_MISMATCH"


@dataclass(frozen=True)
class ReplacementStats:
    address: int = 0
    maps_url: int = 0

    def __add__(self, other: "ReplacementStats") -> "ReplacementStats":
        return ReplacementStats(
            address=self.address + other.address,
            maps_url=self.maps_url + other.maps_url,
        )

    @property
    def total(self) -> int:
        return self.address + self.maps_url


# One entry per component: (component type, placeholders in encounter order).
PlaceholderSignature = tuple[tuple[str, tuple[str, ...]], ...]


@dataclass(frozen=True)
class TemplateOutcome:
    source_name: str
    target_name: str
    status: TemplateStatus
    payload: dict[str, Any]
    replacements: ReplacementStats
    source_status: str | None = None
    # First footer line of the rewritten body (the brand line). The script never
    # rewrites it — the target location inherits whatever the source uses — but
    # the operator must see what is about to be submitted before approving it.
    brand_line: str | None = None
    residuals: tuple[str, ...] = ()
    placeholders_before: PlaceholderSignature = ()
    placeholders_after: PlaceholderSignature = ()


@dataclass(frozen=True)
class ExistingTarget:
    target_name: str
    status: str | None


@dataclass(frozen=True)
class ClonePlan:
    prepared: tuple[TemplateOutcome, ...] = ()
    existing_approved: tuple[str, ...] = ()
    existing_pending: tuple[str, ...] = ()
    existing_unusable: tuple[ExistingTarget, ...] = ()
    neutral: tuple[str, ...] = ()
    unrecognized: tuple[TemplateOutcome, ...] = ()
    blocked: tuple[TemplateOutcome, ...] = ()
    residual: tuple[TemplateOutcome, ...] = ()
    placeholder_mismatch: tuple[TemplateOutcome, ...] = ()
    missing_expected: tuple[str, ...] = ()
    neutral_expected: tuple[str, ...] = ()
    total_replacements: ReplacementStats = ReplacementStats()
    source_count: int = 0


def _location_code(value: str) -> str:
    code = value.strip().lower()
    if not re.fullmatch(r"[a-z0-9]+", code):
        raise argparse.ArgumentTypeError("location code must contain only lowercase letters and digits")
    return code


def _template_prefix(location_code: str) -> str:
    return f"kitilash_{location_code}_"


def _confirmation_word(location_code: str, count: int) -> str:
    """Confirmation phrase that changes whenever the plan changes."""
    return f"CREATE:{location_code.upper()}:{count}"


def _normalize_api_version(value: str) -> str:
    version = value.strip().lower()
    if not version:
        raise ScriptError("Meta Graph API version is empty")
    if not version.startswith("v"):
        version = f"v{version}"
    if not re.fullmatch(r"v\d+\.\d+", version):
        raise ScriptError(f"invalid Meta Graph API version: {value!r}")
    return version


def normalize_graph_url(value: str) -> str:
    """Pin the Graph host: a typo in WHATSAPP_GRAPH_URL must not leak the token."""
    raw = value.strip()
    parsed = urlsplit(raw)
    if parsed.scheme != "https":
        raise ScriptError(f"--graph-url must use https, got {raw!r}")
    if parsed.username or parsed.password:
        raise ScriptError("--graph-url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ScriptError("--graph-url must not contain a query string or a fragment")
    if parsed.hostname != ALLOWED_GRAPH_HOST or parsed.port is not None:
        raise ScriptError(
            f"--graph-url must be https://{ALLOWED_GRAPH_HOST}, got {raw!r}; "
            "the WABA access token is only ever sent to that host"
        )
    if parsed.path.strip("/"):
        raise ScriptError(f"--graph-url must not contain a path, got {parsed.path!r}")
    return f"https://{ALLOWED_GRAPH_HOST}"


def expected_branch_templates(*, source_prefix: str) -> frozenset[str]:
    """Template names that MUST be cloned, derived from ``meta_templates``.

    Branch-specific means "not in ``UNIVERSAL_JOB_TYPES``". The names come from
    ``META_TEMPLATE_MAP``/``resolve_meta_template`` instead of a hand-written
    list here, so adding a lifecycle template to the bot automatically makes
    this script demand a variant for the new location.
    """
    names: set[str] = set()
    for company_id, job_type in META_TEMPLATE_MAP:
        if job_type in UNIVERSAL_JOB_TYPES:
            continue
        for is_new_client in (False, True):
            name = resolve_meta_template(company_id, job_type, is_new_client=is_new_client)
            if isinstance(name, str) and name.startswith(source_prefix):
                names.add(name)
    return frozenset(names)


def is_altegio_only(template_name: str) -> bool:
    """True for a template only Altegio locations use (see ALTEGIO_ONLY_SUFFIX)."""
    return template_name.endswith(ALTEGIO_ONLY_SUFFIX)


def _altegio_only_note(template_name: str) -> str:
    return f"  [{_ALTEGIO_ONLY_NOTE}]" if is_altegio_only(template_name) else ""


def _replace_location_text(text: str, *, address: str, maps_url: str) -> tuple[str, ReplacementStats]:
    transformed = text
    address_count = 0
    maps_count = 0

    for pattern in SOURCE_ADDRESS_PATTERNS:
        transformed, count = pattern.subn(address, transformed)
        address_count += count

    for old_url in SOURCE_MAP_URLS:
        count = transformed.count(old_url)
        if count:
            transformed = transformed.replace(old_url, maps_url)
            maps_count += count

    return transformed, ReplacementStats(address=address_count, maps_url=maps_count)


def _replace_in_value(value: Any, *, address: str, maps_url: str) -> tuple[Any, ReplacementStats]:
    """Rewrite every string leaf of *value*, however deeply it is nested."""
    if isinstance(value, str):
        return _replace_location_text(value, address=address, maps_url=maps_url)
    if isinstance(value, list):
        items: list[Any] = []
        list_stats = ReplacementStats()
        for item in value:
            new_item, item_stats = _replace_in_value(item, address=address, maps_url=maps_url)
            items.append(new_item)
            list_stats += item_stats
        return items, list_stats
    if isinstance(value, dict):
        mapping: dict[str, Any] = {}
        dict_stats = ReplacementStats()
        for key, item in value.items():
            new_item, item_stats = _replace_in_value(item, address=address, maps_url=maps_url)
            mapping[key] = new_item
            dict_stats += item_stats
        return mapping, dict_stats
    return value, ReplacementStats()


def residual_source_markers(value: Any, *, path: str = "payload") -> list[str]:
    """Source markers still present in *value*, each with the path where it sits."""
    findings: list[str] = []
    if isinstance(value, str):
        for pattern in SOURCE_ADDRESS_PATTERNS:
            for match in pattern.finditer(value):
                findings.append(f"{path}: source address {match.group(0)!r}")
        for url in SOURCE_MAP_URLS:
            if url in value:
                findings.append(f"{path}: source maps link {url}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            findings.extend(residual_source_markers(item, path=f"{path}[{index}]"))
    elif isinstance(value, dict):
        for key, item in value.items():
            findings.extend(residual_source_markers(item, path=f"{path}.{key}"))
    return findings


def _placeholders(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(match.group(0) for match in PLACEHOLDER_PATTERN.finditer(value))
    if isinstance(value, list):
        return tuple(token for item in value for token in _placeholders(item))
    if isinstance(value, dict):
        return tuple(token for item in value.values() for token in _placeholders(item))
    return ()


def placeholder_signature(components: Iterable[dict[str, Any]]) -> PlaceholderSignature:
    """Placeholders per component, in order — the positional-parameter contract."""
    return tuple((str(component.get("type", "?")), _placeholders(component)) for component in components)


def format_placeholder_signature(signature: PlaceholderSignature) -> str:
    parts = [f"{name} {','.join(tokens) if tokens else '-'}" for name, tokens in signature]
    return " | ".join(parts) if parts else "-"


def _brand_line(text: str, *, address: str) -> str | None:
    """Return the footer line that precedes *address*, or None when absent."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if address not in line:
            continue
        for previous in reversed(lines[:index]):
            stripped = previous.strip()
            if stripped:
                return stripped
        return None
    return None


def _strip_component(component: dict[str, Any]) -> dict[str, Any]:
    return {key: copy.deepcopy(value) for key, value in component.items() if key in _COMPONENT_KEYS}


def _sanitize_component(
    component: dict[str, Any],
    *,
    address: str,
    maps_url: str,
) -> tuple[dict[str, Any], ReplacementStats]:
    """POST-safe component with the address and the map link rewritten."""
    sanitized: dict[str, Any] = {}
    stats = ReplacementStats()
    for key, value in _strip_component(component).items():
        if key in _REPLACEABLE_COMPONENT_KEYS:
            sanitized[key], value_stats = _replace_in_value(value, address=address, maps_url=maps_url)
            stats += value_stats
        else:
            sanitized[key] = value
    return sanitized, stats


def _classify(stats: ReplacementStats) -> TemplateStatus:
    if stats.address == 0:
        # No address AND no Karlsruhe map link: genuinely location-neutral.
        # No address but a Karlsruhe map link: the address is written in a
        # spelling the patterns above do not cover, so silently treating the
        # template as neutral would leave the new location without it.
        return TemplateStatus.NEUTRAL if stats.maps_url == 0 else TemplateStatus.ADDRESS_UNRECOGNIZED
    if stats.maps_url == 0:
        return TemplateStatus.BLOCKED_NO_MAPS
    return TemplateStatus.READY


def prepare_template(
    source: dict[str, Any],
    *,
    source_prefix: str,
    target_prefix: str,
    address: str,
    maps_url: str,
) -> TemplateOutcome:
    """Build a POST-safe target template and classify the source it came from."""
    source_name = source.get("name")
    if not isinstance(source_name, str) or not source_name.startswith(source_prefix):
        raise ScriptError(f"unexpected source template name: {source_name!r}")

    raw_components = source.get("components")
    if not isinstance(raw_components, list) or not raw_components:
        raise ScriptError(f"template {source_name!r} has no components")

    stripped: list[dict[str, Any]] = []
    components: list[dict[str, Any]] = []
    total_stats = ReplacementStats()
    brand_line: str | None = None
    for raw_component in raw_components:
        if not isinstance(raw_component, dict):
            raise ScriptError(f"template {source_name!r} contains an invalid component")
        stripped.append(_strip_component(raw_component))
        component, stats = _sanitize_component(raw_component, address=address, maps_url=maps_url)
        components.append(component)
        total_stats += stats
        if brand_line is None and isinstance(component.get("text"), str):
            brand_line = _brand_line(component["text"], address=address)

    target_name = f"{target_prefix}{source_name[len(source_prefix) :]}"
    payload: dict[str, Any] = {
        "name": target_name,
        "language": source.get("language"),
        "category": source.get("category"),
        "components": components,
    }
    if source.get("parameter_format") is not None:
        payload["parameter_format"] = source["parameter_format"]

    payload = {key: value for key, value in payload.items() if key in _TEMPLATE_KEYS and value is not None}

    # Scan the FINISHED payload, not only the fields the rewrite touched: this is
    # what catches a marker sitting somewhere the replacement never looked.
    residuals = tuple(residual_source_markers(payload))
    before = placeholder_signature(stripped)
    after = placeholder_signature(components)

    if residuals:
        status = TemplateStatus.RESIDUAL_SOURCE
    elif before != after:
        status = TemplateStatus.PLACEHOLDER_MISMATCH
    else:
        status = _classify(total_stats)

    source_status = source.get("status")
    return TemplateOutcome(
        source_name=source_name,
        target_name=target_name,
        status=status,
        payload=payload,
        replacements=total_stats,
        source_status=source_status if isinstance(source_status, str) else None,
        brand_line=brand_line,
        residuals=residuals,
        placeholders_before=before,
        placeholders_after=after,
    )


def select_sources(
    templates: Iterable[dict[str, Any]],
    *,
    source_prefix: str,
    language: str,
) -> list[dict[str, Any]]:
    selected = [
        template
        for template in templates
        if template.get("status") == "APPROVED"
        and template.get("language") == language
        and isinstance(template.get("name"), str)
        and template["name"].startswith(source_prefix)
    ]
    return sorted(selected, key=lambda item: item["name"])


def index_existing(templates: Iterable[dict[str, Any]]) -> dict[tuple[str, Any], str | None]:
    """Map (name, language) -> status for every template already in the WABA."""
    index: dict[tuple[str, Any], str | None] = {}
    for template in templates:
        name = template.get("name")
        if not isinstance(name, str):
            continue
        status = template.get("status")
        index[(name, template.get("language"))] = status if isinstance(status, str) else None
    return index


def build_plan(
    sources: Iterable[dict[str, Any]],
    *,
    source_prefix: str,
    target_prefix: str,
    address: str,
    maps_url: str,
    language: str,
    existing: Mapping[tuple[str, Any], str | None],
    expected: frozenset[str],
) -> ClonePlan:
    """Classify every source template and check the expected branch coverage."""
    prepared: list[TemplateOutcome] = []
    existing_approved: list[str] = []
    existing_pending: list[str] = []
    existing_unusable: list[ExistingTarget] = []
    neutral: list[str] = []
    unrecognized: list[TemplateOutcome] = []
    blocked: list[TemplateOutcome] = []
    residual: list[TemplateOutcome] = []
    placeholder_mismatch: list[TemplateOutcome] = []
    seen: set[str] = set()
    totals = ReplacementStats()
    source_count = 0

    for source in sources:
        source_count += 1
        outcome = prepare_template(
            source,
            source_prefix=source_prefix,
            target_prefix=target_prefix,
            address=address,
            maps_url=maps_url,
        )
        seen.add(outcome.source_name)
        totals += outcome.replacements

        if outcome.status is TemplateStatus.RESIDUAL_SOURCE:
            residual.append(outcome)
        elif outcome.status is TemplateStatus.PLACEHOLDER_MISMATCH:
            placeholder_mismatch.append(outcome)
        elif outcome.status is TemplateStatus.ADDRESS_UNRECOGNIZED:
            unrecognized.append(outcome)
        elif outcome.status is TemplateStatus.BLOCKED_NO_MAPS:
            blocked.append(outcome)
        elif outcome.status is TemplateStatus.NEUTRAL:
            neutral.append(outcome.source_name)
        elif (outcome.target_name, language) in existing:
            target_status = existing[(outcome.target_name, language)]
            if target_status not in REUSABLE_TARGET_STATUSES:
                existing_unusable.append(ExistingTarget(outcome.target_name, target_status))
            elif target_status == "APPROVED":
                existing_approved.append(outcome.target_name)
            else:
                existing_pending.append(outcome.target_name)
        else:
            prepared.append(outcome)

    return ClonePlan(
        prepared=tuple(prepared),
        existing_approved=tuple(existing_approved),
        existing_pending=tuple(existing_pending),
        existing_unusable=tuple(existing_unusable),
        neutral=tuple(neutral),
        unrecognized=tuple(unrecognized),
        blocked=tuple(blocked),
        residual=tuple(residual),
        placeholder_mismatch=tuple(placeholder_mismatch),
        missing_expected=tuple(sorted(name for name in expected if name not in seen)),
        neutral_expected=tuple(sorted(name for name in neutral if name in expected)),
        total_replacements=totals,
        source_count=source_count,
    )


def plan_blockers(plan: ClonePlan) -> list[str]:
    """Reasons the run must not submit anything; empty means the plan is sound."""
    blockers: list[str] = []

    if plan.source_count and plan.total_replacements.total == 0:
        blockers.append(
            f"none of the {plan.source_count} source template(s) contained the source address or the source "
            "maps link: SOURCE_ADDRESS_PATTERNS / SOURCE_MAP_URLS do not cover this --source-location"
        )

    for item in plan.residual:
        for residual in item.residuals:
            blockers.append(f"{item.source_name}: source marker left in the finished payload at {residual}")
    for item in plan.placeholder_mismatch:
        blockers.append(
            f"{item.source_name}: the rewrite changed the placeholders "
            f"({format_placeholder_signature(item.placeholders_before)} -> "
            f"{format_placeholder_signature(item.placeholders_after)}); "
            "LIFECYCLE_PARAM_FIELDS binds template parameters by position"
        )
    for item in plan.blocked:
        blockers.append(
            f"{item.source_name}: source maps link not found, the clone would carry the source map "
            f"(address={item.replacements.address}, maps=0)"
        )
    for item in plan.unrecognized:
        blockers.append(
            f"{item.source_name}: address not recognised while the source map link WAS found "
            f"(address=0, maps={item.replacements.maps_url}) — the address spelling is not covered by "
            "SOURCE_ADDRESS_PATTERNS"
        )
    for target in plan.existing_unusable:
        detail = (
            "Meta does not allow reusing the name of a rejected template: submit the fixed copy under a new "
            "version name (_v2)"
            if target.status == "REJECTED"
            else "resolve it in the WhatsApp Manager before cloning"
        )
        blockers.append(
            f"{target.target_name}: target already exists with status {target.status or 'UNKNOWN'} — {detail}"
        )
    for name in plan.neutral_expected:
        blockers.append(
            f"{name}: expected branch-specific template classified as location-neutral{_altegio_only_note(name)}"
        )
    for name in plan.missing_expected:
        blockers.append(
            f"{name}: expected branch-specific template not found among the APPROVED sources{_altegio_only_note(name)}"
        )
    return blockers


def _component_preview(component: dict[str, Any]) -> list[str]:
    kind = str(component.get("type", "?"))
    fmt = component.get("format")
    lines = [f"--- {kind}{f' ({fmt})' if isinstance(fmt, str) else ''} ---"]
    text = component.get("text")
    if isinstance(text, str):
        lines.extend(text.splitlines() or [""])
    buttons = component.get("buttons")
    if isinstance(buttons, list):
        for button in buttons:
            if not isinstance(button, dict):
                lines.append(f"  {button!r}")
                continue
            destination = button.get("url") or button.get("phone_number") or ""
            lines.append(
                f"  [{button.get('type', '?')}] {button.get('text', '')}{f' -> {destination}' if destination else ''}"
            )
    example = component.get("example")
    if example is not None:
        lines.append(f"  example: {json.dumps(example, ensure_ascii=False)}")
    return lines


def print_template_preview(item: TemplateOutcome) -> None:
    """Everything that will be submitted, so the operator reviews it before Meta does."""
    print("=" * 78)
    print(f"READY   {item.source_name} -> {item.target_name}{_altegio_only_note(item.source_name)}")
    print(
        f"        source status: {item.source_status or '-'}   "
        f"language: {item.payload.get('language', '-')}   "
        f"category: {item.payload.get('category', '-')}"
    )
    print(f"        replacements: address={item.replacements.address}, maps={item.replacements.maps_url}")
    print(f"        placeholders: {format_placeholder_signature(item.placeholders_after)}")
    # The brand line is never rewritten: the source brand is what the clone will
    # carry. Whether that is right for the new location is the operator's call,
    # so it is printed instead of guessed.
    print(f"        brand line (kept as is): {item.brand_line or '<not found>'}")
    for component in item.payload.get("components", []):
        if isinstance(component, dict):
            for line in _component_preview(component):
                print(f"  {line}")
    print("  --- POST payload ---")
    for line in json.dumps(item.payload, ensure_ascii=False, indent=2).splitlines():
        print(f"  {line}")
    print("=" * 78)


def print_plan(plan: ClonePlan, *, detailed: bool) -> None:
    for item in plan.prepared:
        if detailed:
            print_template_preview(item)
            continue
        print(
            f"READY   {item.source_name} -> {item.target_name} "
            f"[address={item.replacements.address}, maps={item.replacements.maps_url}]"
            f"{_altegio_only_note(item.source_name)}"
        )
        print(f"        brand line (kept as is): {item.brand_line or '<not found>'}")
    for item in plan.residual:
        print(f"BLOCKED {item.source_name}: source marker left in the finished payload")
        for residual in item.residuals:
            print(f"        {residual}")
    for item in plan.placeholder_mismatch:
        print(f"BLOCKED {item.source_name}: the rewrite changed the placeholder signature")
        print(f"        before: {format_placeholder_signature(item.placeholders_before)}")
        print(f"        after:  {format_placeholder_signature(item.placeholders_after)}")
    for item in plan.blocked:
        print(
            f"BLOCKED {item.source_name}: maps link not replaced "
            f"[address={item.replacements.address}, maps=0] — not prepared"
        )
    for item in plan.unrecognized:
        print(
            f"ERROR   {item.source_name}: address not recognised but source map found "
            f"[address=0, maps={item.replacements.maps_url}]"
        )
    for target in plan.existing_unusable:
        print(f"BLOCKED target exists with status {target.status or 'UNKNOWN'}: {target.target_name}")
    for name in plan.existing_approved:
        print(f"SKIP    APPROVED target already exists: {name}")
    for name in plan.existing_pending:
        print(f"SKIP    PENDING target already awaiting review: {name}")
    for name in plan.neutral:
        marker = " (EXPECTED BRANCH-SPECIFIC!)" if name in plan.neutral_expected else ""
        print(f"SKIP    location-neutral, no address and no map: {name}{marker}")
    for name in plan.missing_expected:
        print(f"MISSING expected branch-specific template not among APPROVED sources: {name}{_altegio_only_note(name)}")

    print()
    print(
        f"Summary: ready={len(plan.prepared)}, approved-targets={len(plan.existing_approved)}, "
        f"pending-targets={len(plan.existing_pending)}, unusable-targets={len(plan.existing_unusable)}, "
        f"location-neutral={len(plan.neutral)}, blocked={len(plan.blocked)}, "
        f"residual-markers={len(plan.residual)}, placeholder-mismatch={len(plan.placeholder_mismatch)}, "
        f"unrecognized-address={len(plan.unrecognized)}, missing={len(plan.missing_expected)}"
    )


class MetaTemplateClient:
    def __init__(
        self,
        *,
        token: str,
        waba_id: str,
        graph_url: str,
        api_version: str,
        timeout_seconds: float,
    ) -> None:
        self._url = f"{normalize_graph_url(graph_url)}/{api_version}/{waba_id}/message_templates"
        self._headers = {"Authorization": f"Bearer {token}"}
        self._client = httpx.AsyncClient(timeout=timeout_seconds, headers=self._headers)

    async def __aenter__(self) -> "MetaTemplateClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self._client.aclose()

    async def list_templates(self) -> list[dict[str, Any]]:
        """Read every page, always from OUR endpoint with a cursor of our own.

        ``paging.next`` is a server-supplied absolute URL while this client sends
        an Authorization header on every request, so following it verbatim would
        mean handing the WABA token to whatever host lands in that field.
        """
        templates: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()

        for _page in range(_MAX_TEMPLATE_PAGES):
            params: dict[str, Any] = {"limit": _PAGE_LIMIT}
            if after is not None:
                params["after"] = after
            response = await self._client.get(self._url, params=params)
            self._raise_for_meta_error(response, operation="read templates")
            payload = self._json_object(response, operation="read templates")

            page = payload.get("data", [])
            if not isinstance(page, list):
                raise ScriptError("Meta returned an invalid template list")
            templates.extend(item for item in page if isinstance(item, dict))

            paging = payload.get("paging")
            if not isinstance(paging, dict) or not paging.get("next"):
                return templates
            cursors = paging.get("cursors")
            after = cursors.get("after") if isinstance(cursors, dict) else None
            if not isinstance(after, str) or not after:
                raise ScriptError("Meta announced another page without a usable paging.cursors.after")
            if after in seen_cursors:
                raise ScriptError(f"Meta paging cursor repeated ({after!r}); refusing to loop")
            seen_cursors.add(after)

        raise ScriptError(f"Meta returned more than {_MAX_TEMPLATE_PAGES} template pages; aborting")

    async def create_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        operation = f"create {payload.get('name')}"
        response = await self._client.post(self._url, json=payload)
        self._raise_for_meta_error(response, operation=operation)
        return self._json_object(response, operation=operation)

    @staticmethod
    def _json_object(response: httpx.Response, *, operation: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ScriptError(
                f"cannot {operation}: Meta returned a non-JSON body (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise ScriptError(f"cannot {operation}: Meta returned {type(payload).__name__}, expected an object")
        return payload

    @staticmethod
    def _raise_for_meta_error(response: httpx.Response, *, operation: str) -> None:
        if response.is_success:
            return
        message = f"HTTP {response.status_code}"
        try:
            payload = response.json()
            error = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error, dict):
                details = [str(error.get("message") or "Meta API error")]
                for key in ("type", "code", "error_subcode", "fbtrace_id"):
                    if error.get(key) is not None:
                        details.append(f"{key}={error[key]}")
                message = "; ".join(details)
        except ValueError:
            pass
        raise ScriptError(f"cannot {operation}: {message}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-location", type=_location_code, default=DEFAULT_SOURCE_LOCATION)
    parser.add_argument(
        "--target-location",
        type=_location_code,
        default=None,
        help=f"target location code (dry-run default: {DEFAULT_TARGET_LOCATION}); required with --apply",
    )
    parser.add_argument("--language", default=DEFAULT_LANGUAGE)
    parser.add_argument(
        "--address",
        default=None,
        help=f"target address (dry-run default: {DEFAULT_TARGET_ADDRESS!r}); required with --apply",
    )
    parser.add_argument(
        "--maps-url",
        default=None,
        help=f"target maps link (dry-run default: {DEFAULT_TARGET_MAPS_URL}); required with --apply",
    )
    parser.add_argument(
        "--api-version",
        default=os.getenv("WHATSAPP_API_VERSION") or os.getenv("META_GRAPH_API_VERSION") or DEFAULT_API_VERSION,
    )
    parser.add_argument("--graph-url", default=os.getenv("WHATSAPP_GRAPH_URL") or DEFAULT_GRAPH_URL)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="submit prepared templates to Meta; without this flag the script is a dry-run",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="do not ask for interactive confirmation (only meaningful with --apply)",
    )
    return parser


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ScriptError(f"environment variable {name} is not set")
    return value


def _confirm(count: int, *, expected: str) -> None:
    answer = input(f"Submit {count} template(s) to Meta for review? Type {expected}: ").strip()
    if answer != expected:
        raise ScriptError("confirmation did not match; nothing was submitted")


def validate_target_address(value: str) -> str:
    """Reject an address the rest of the script would happily substitute.

    Every guard downstream watches the SOURCE side: the replacement counters, the
    residual scan and the placeholder signature all pass for ``--address ''``,
    because the Karlsruhe address really is gone — replaced by nothing. The value
    itself is the only place this is catchable.
    """
    address = value.strip()
    if not address:
        raise ScriptError("--address is empty; the clone would ship a footer with no address at all")
    if _CONTROL_CHARACTERS.search(address):
        raise ScriptError(f"--address contains a control character: {address!r}")
    if PLACEHOLDER_PATTERN.search(address):
        raise ScriptError(
            f"--address contains a Meta placeholder: {address!r}; Meta would read it as a positional "
            "parameter and the LIFECYCLE_PARAM_FIELDS order would no longer match"
        )
    for pattern in SOURCE_ADDRESS_PATTERNS:
        if pattern.search(address):
            raise ScriptError(
                f"--address is the SOURCE address: {address!r}; the clone would keep pointing at the source location"
            )
    return address


def validate_target_maps_url(value: str) -> str:
    """Reject a maps link that would be pasted into the footer as-is."""
    url = value.strip()
    if not url:
        raise ScriptError("--maps-url is empty; the clone would ship a footer with no map link at all")
    if _CONTROL_CHARACTERS.search(url):
        raise ScriptError(f"--maps-url contains a control character: {url!r}")
    if url in SOURCE_MAP_URLS:
        raise ScriptError(f"--maps-url is one of the SOURCE map links: {url}; the clone would keep the source map")
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise ScriptError(f"--maps-url must be an absolute https URL, got {url!r}")
    if not parsed.hostname:
        raise ScriptError(f"--maps-url has no hostname, got {url!r}")
    if parsed.username or parsed.password:
        raise ScriptError("--maps-url must not contain credentials")
    if parsed.fragment:
        raise ScriptError(f"--maps-url must not contain a fragment, got {url!r}")
    return url


def resolve_targets(args: argparse.Namespace) -> tuple[str, str, str]:
    """Target location/address/maps link: explicit under --apply, defaulted otherwise.

    The dry-run defaults describe Durlach. They are convenient for a plan and
    dangerous for a submission, so --apply refuses to inherit them. The values
    are then validated whether they were given or defaulted — a default is only
    trustworthy until someone edits the constant.
    """
    if args.apply:
        missing = [
            flag
            for flag, value in (
                ("--target-location", args.target_location),
                ("--address", args.address),
                ("--maps-url", args.maps_url),
            )
            if value is None
        ]
        if missing:
            raise ScriptError(
                f"--apply requires {', '.join(missing)} to be given explicitly; "
                "the dry-run defaults must not decide which location gets submitted"
            )
    return (
        args.target_location or DEFAULT_TARGET_LOCATION,
        validate_target_address(DEFAULT_TARGET_ADDRESS if args.address is None else args.address),
        validate_target_maps_url(DEFAULT_TARGET_MAPS_URL if args.maps_url is None else args.maps_url),
    )


async def async_main(args: argparse.Namespace) -> int:
    target_location, address, maps_url = resolve_targets(args)
    if args.source_location == target_location:
        raise ScriptError("source and target locations must be different")
    if args.timeout <= 0:
        raise ScriptError("--timeout must be greater than zero")

    graph_url = normalize_graph_url(args.graph_url)
    api_version = _normalize_api_version(args.api_version)
    token = _required_env("WHATSAPP_ACCESS_TOKEN")
    waba_id = _required_env("META_WABA_ID")
    source_prefix = _template_prefix(args.source_location)
    target_prefix = _template_prefix(target_location)

    async with MetaTemplateClient(
        token=token,
        waba_id=waba_id,
        graph_url=graph_url,
        api_version=api_version,
        timeout_seconds=args.timeout,
    ) as client:
        all_templates = await client.list_templates()
        sources = select_sources(all_templates, source_prefix=source_prefix, language=args.language)

        if not sources:
            raise ScriptError(f"no APPROVED {args.language!r} templates found with prefix {source_prefix!r}")

        plan = build_plan(
            sources,
            source_prefix=source_prefix,
            target_prefix=target_prefix,
            address=address,
            maps_url=maps_url,
            language=args.language,
            existing=index_existing(all_templates),
            expected=expected_branch_templates(source_prefix=source_prefix),
        )

        mode = "APPLY" if args.apply else "DRY-RUN"
        print(f"Mode: {mode}")
        print(f"Meta Graph API: {api_version}")
        print(f"Source: {source_prefix}* ({args.language}, APPROVED)")
        print(f"Target: {target_prefix}*")
        print(f"Address: {address}")
        print(f"Maps: {maps_url}")
        print()

        print_plan(plan, detailed=not args.apply)

        blockers = plan_blockers(plan)
        if blockers:
            print()
            details = "\n".join(f"  - {blocker}" for blocker in blockers)
            raise ScriptError(f"nothing was submitted, {len(blockers)} problem(s) must be fixed first:\n{details}")

        if not args.apply:
            print("Nothing submitted. Re-run with --apply after reviewing this plan.")
            return 0
        if not plan.prepared:
            print("Nothing to submit.")
            return 0

        # --apply does not require a prior dry-run, and CREATE:<TARGET>:<count>
        # encodes only how many templates go out — not which address or which map
        # link they carry. So the full payload is printed here, before the
        # confirmation, and with --yes as well, where it is the run transcript.
        print()
        print(f"About to submit {len(plan.prepared)} template(s) to Meta for review:")
        print()
        for item in plan.prepared:
            print_template_preview(item)
        print()

        if not args.yes:
            _confirm(len(plan.prepared), expected=_confirmation_word(target_location, len(plan.prepared)))

        failures = 0
        indeterminate = 0
        for item in plan.prepared:
            try:
                result = await client.create_template(item.payload)
            except ScriptError as exc:
                failures += 1
                print(f"FAIL    {item.target_name}: {exc}", file=sys.stderr)
                continue
            template_id = result.get("id")
            if not isinstance(template_id, str) or not template_id.strip():
                indeterminate += 1
                print(
                    f"UNKNOWN {item.target_name}: Meta accepted the request but returned no template id "
                    f"({json.dumps(result, ensure_ascii=False)}); check the WABA before re-running",
                    file=sys.stderr,
                )
                continue
            status = result.get("status") or "unknown"
            category = result.get("category") or item.payload.get("category", "-")
            print(f"SENT    {item.target_name} id={template_id} status={status} category={category}")

        if failures or indeterminate:
            print(
                f"Completed with {failures} failure(s) and {indeterminate} indeterminate result(s).",
                file=sys.stderr,
            )
            return 1
        print("All prepared templates were submitted to Meta for review.")
        return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(async_main(args))
    except (ScriptError, httpx.HTTPError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted. No further templates will be submitted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

"""Clone approved Meta WhatsApp templates from one KitiLash location to another.

The script reads templates directly from the WhatsApp Business Account, copies
only approved German templates whose names start with the source location
prefix, replaces the Karlsruhe address and Google Maps URL, and submits the new
location-prefixed variants for Meta review.

Safety defaults:
* dry-run unless --apply is supplied;
* templates without the source address are skipped (universal templates are not
  duplicated by accident);
* existing target template names are skipped;
* source templates are never edited or deleted.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Iterable

import httpx

DEFAULT_GRAPH_URL = "https://graph.facebook.com"
DEFAULT_API_VERSION = "v25.0"
DEFAULT_LANGUAGE = "de"
DEFAULT_SOURCE_LOCATION = "ka"
DEFAULT_TARGET_LOCATION = "du"
DEFAULT_TARGET_ADDRESS = "Pfinztalstraße 4, 76227 Karlsruhe-Durlach"
DEFAULT_TARGET_MAPS_URL = "https://maps.app.goo.gl/WWnqMQLH5avG7Ybn6"

# Current Karlsruhe spellings found in the repository/Meta template examples.
SOURCE_ADDRESS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"76133\s+Karlsruhe,\s*Kaiserstraße,?\s*68"),
    re.compile(r"Kaiserstraße,?\s*68,\s*76133\s+Karlsruhe"),
)
SOURCE_MAP_URLS: tuple[str, ...] = (
    "https://goo.gl/maps/p7quWqbAqY9cusuRA",
    "https://maps.app.goo.gl/p7quWqbAqY9cusuRA",
)

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
_TEMPLATE_KEYS = frozenset({"category", "components", "language", "name", "parameter_format"})


class ScriptError(RuntimeError):
    """Expected configuration/API error with a user-safe message."""


@dataclass(frozen=True)
class ReplacementStats:
    address: int = 0
    maps_url: int = 0

    def __add__(self, other: "ReplacementStats") -> "ReplacementStats":
        return ReplacementStats(
            address=self.address + other.address,
            maps_url=self.maps_url + other.maps_url,
        )


@dataclass(frozen=True)
class PreparedTemplate:
    source_name: str
    target_name: str
    payload: dict[str, Any]
    replacements: ReplacementStats


def _location_code(value: str) -> str:
    code = value.strip().lower()
    if not re.fullmatch(r"[a-z0-9]+", code):
        raise argparse.ArgumentTypeError("location code must contain only lowercase letters and digits")
    return code


def _template_prefix(location_code: str) -> str:
    return f"kitilash_{location_code}_"


def _normalize_api_version(value: str) -> str:
    version = value.strip().lower()
    if not version:
        raise ScriptError("Meta Graph API version is empty")
    if not version.startswith("v"):
        version = f"v{version}"
    if not re.fullmatch(r"v\d+\.\d+", version):
        raise ScriptError(f"invalid Meta Graph API version: {value!r}")
    return version


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


def _sanitize_component(
    component: dict[str, Any],
    *,
    address: str,
    maps_url: str,
) -> tuple[dict[str, Any], ReplacementStats]:
    sanitized = {key: copy.deepcopy(value) for key, value in component.items() if key in _COMPONENT_KEYS}
    stats = ReplacementStats()

    text = sanitized.get("text")
    if isinstance(text, str):
        sanitized["text"], text_stats = _replace_location_text(text, address=address, maps_url=maps_url)
        stats += text_stats

    return sanitized, stats


def prepare_template(
    source: dict[str, Any],
    *,
    source_prefix: str,
    target_prefix: str,
    address: str,
    maps_url: str,
) -> PreparedTemplate | None:
    """Build a POST-safe target template, or None for a location-neutral source."""
    source_name = source.get("name")
    if not isinstance(source_name, str) or not source_name.startswith(source_prefix):
        raise ScriptError(f"unexpected source template name: {source_name!r}")

    raw_components = source.get("components")
    if not isinstance(raw_components, list) or not raw_components:
        raise ScriptError(f"template {source_name!r} has no components")

    components: list[dict[str, Any]] = []
    total_stats = ReplacementStats()
    for raw_component in raw_components:
        if not isinstance(raw_component, dict):
            raise ScriptError(f"template {source_name!r} contains an invalid component")
        component, stats = _sanitize_component(raw_component, address=address, maps_url=maps_url)
        components.append(component)
        total_stats += stats

    # A template with no Karlsruhe address is location-neutral. Duplicating it
    # under du_* would create unnecessary templates and can require media sample
    # handles for image headers, so it is intentionally skipped.
    if total_stats.address == 0:
        return None

    target_name = f"{target_prefix}{source_name[len(source_prefix):]}"
    payload: dict[str, Any] = {
        "name": target_name,
        "language": source.get("language"),
        "category": source.get("category"),
        "components": components,
    }
    if source.get("parameter_format") is not None:
        payload["parameter_format"] = source["parameter_format"]

    payload = {key: value for key, value in payload.items() if key in _TEMPLATE_KEYS and value is not None}
    return PreparedTemplate(
        source_name=source_name,
        target_name=target_name,
        payload=payload,
        replacements=total_stats,
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
        self._url = f"{graph_url.rstrip('/')}/{api_version}/{waba_id}/message_templates"
        self._headers = {"Authorization": f"Bearer {token}"}
        self._client = httpx.AsyncClient(timeout=timeout_seconds, headers=self._headers)

    async def __aenter__(self) -> "MetaTemplateClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self._client.aclose()

    async def list_templates(self) -> list[dict[str, Any]]:
        templates: list[dict[str, Any]] = []
        next_url: str | None = self._url
        params: dict[str, Any] | None = {"limit": 100}

        while next_url:
            response = await self._client.get(next_url, params=params)
            params = None  # paging.next already contains the cursor
            self._raise_for_meta_error(response, operation="read templates")
            payload = response.json()
            page = payload.get("data", [])
            if not isinstance(page, list):
                raise ScriptError("Meta returned an invalid template list")
            templates.extend(item for item in page if isinstance(item, dict))
            paging = payload.get("paging")
            next_url = paging.get("next") if isinstance(paging, dict) else None

        return templates

    async def create_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self._client.post(self._url, json=payload)
        self._raise_for_meta_error(response, operation=f"create {payload.get('name')}")
        result = response.json()
        return result if isinstance(result, dict) else {}

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
    parser.add_argument("--target-location", type=_location_code, default=DEFAULT_TARGET_LOCATION)
    parser.add_argument("--language", default=DEFAULT_LANGUAGE)
    parser.add_argument("--address", default=DEFAULT_TARGET_ADDRESS)
    parser.add_argument("--maps-url", default=DEFAULT_TARGET_MAPS_URL)
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


def _confirm(count: int) -> None:
    expected = "DURLACH"
    answer = input(f"Submit {count} template(s) to Meta for review? Type {expected}: ").strip()
    if answer != expected:
        raise ScriptError("confirmation did not match; nothing was submitted")


async def async_main(args: argparse.Namespace) -> int:
    if args.source_location == args.target_location:
        raise ScriptError("source and target locations must be different")
    if args.timeout <= 0:
        raise ScriptError("--timeout must be greater than zero")

    token = _required_env("WHATSAPP_ACCESS_TOKEN")
    waba_id = _required_env("META_WABA_ID")
    api_version = _normalize_api_version(args.api_version)
    source_prefix = _template_prefix(args.source_location)
    target_prefix = _template_prefix(args.target_location)

    async with MetaTemplateClient(
        token=token,
        waba_id=waba_id,
        graph_url=args.graph_url,
        api_version=api_version,
        timeout_seconds=args.timeout,
    ) as client:
        all_templates = await client.list_templates()
        existing = {
            (template.get("name"), template.get("language"))
            for template in all_templates
            if isinstance(template.get("name"), str)
        }
        sources = select_sources(all_templates, source_prefix=source_prefix, language=args.language)

        if not sources:
            raise ScriptError(f"no APPROVED {args.language!r} templates found with prefix {source_prefix!r}")

        prepared: list[PreparedTemplate] = []
        neutral: list[str] = []
        existing_targets: list[str] = []

        for source in sources:
            candidate = prepare_template(
                source,
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                address=args.address,
                maps_url=args.maps_url,
            )
            if candidate is None:
                neutral.append(source["name"])
                continue
            if (candidate.target_name, args.language) in existing:
                existing_targets.append(candidate.target_name)
                continue
            prepared.append(candidate)

        mode = "APPLY" if args.apply else "DRY-RUN"
        print(f"Mode: {mode}")
        print(f"Meta Graph API: {api_version}")
        print(f"Source: {source_prefix}* ({args.language}, APPROVED)")
        print(f"Target: {target_prefix}*")
        print(f"Address: {args.address}")
        print(f"Maps: {args.maps_url}")
        print()

        for item in prepared:
            map_note = str(item.replacements.maps_url) if item.replacements.maps_url else "0 (warning)"
            print(
                f"READY  {item.source_name} -> {item.target_name} "
                f"[address={item.replacements.address}, maps={map_note}]"
            )
        for name in existing_targets:
            print(f"SKIP   target already exists: {name}")
        for name in neutral:
            print(f"SKIP   no Karlsruhe address (location-neutral): {name}")

        print()
        print(
            f"Summary: ready={len(prepared)}, existing={len(existing_targets)}, "
            f"location-neutral={len(neutral)}"
        )

        if not args.apply:
            print("Nothing submitted. Re-run with --apply after reviewing this plan.")
            return 0
        if not prepared:
            print("Nothing to submit.")
            return 0
        if not args.yes:
            _confirm(len(prepared))

        failures = 0
        for item in prepared:
            try:
                result = await client.create_template(item.payload)
            except ScriptError as exc:
                failures += 1
                print(f"FAIL   {item.target_name}: {exc}", file=sys.stderr)
                continue
            template_id = result.get("id", "-")
            status = result.get("status", "submitted")
            category = result.get("category", item.payload.get("category", "-"))
            print(f"SENT   {item.target_name} id={template_id} status={status} category={category}")

        if failures:
            print(f"Completed with {failures} failure(s).", file=sys.stderr)
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

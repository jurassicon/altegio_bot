"""Static guards for the versioned Nginx webhook-logging reference.

The host Nginx lives outside this repository, so these files are the only part of
the proxy fix that CI can defend. Two properties matter and both have already
been violated once:

1. **The reference must not design routing.** An earlier version shipped ready
   ``location = /webhooks/...`` blocks with their own ``proxy_pass``. Copying
   those into production would change which location Nginx selects, silently
   dropping the real body limits, timeouts, rate limiting, allow/deny, headers,
   buffering, retry policy and upstream that the existing block inherits. The
   reference cannot know the real config, so it may only carry logging.

2. **A safe ``access_log`` is not sufficient.** Nginx writes the full request
   line — query string included — into the *error* log on upstream failure or
   timeout, and that format is not configurable. The include therefore has to
   carry an error-log policy too.

Everything here is plain text analysis: no Nginx binary, no network, no host
access required.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_NGINX_DIR = _REPO_ROOT / "deploy" / "nginx"

LOG_FORMATS_FILE = _NGINX_DIR / "kitilash_webhook_log_formats.conf.example"
LOGGING_INCLUDE_FILE = _NGINX_DIR / "kitilash_webhook_logging.inc.example"
RUNBOOK_FILE = _REPO_ROOT / "docs" / "easyweek" / "capture_runbook.md"

# The old, unsafe reference that shipped ready-made routing blocks. It must stay
# deleted so nobody copies it back into production.
REMOVED_ROUTING_REFERENCE = _NGINX_DIR / "kitilash_webhook_safe_logging.conf.example"

# Directives that would make the reference influence routing or request handling.
_FORBIDDEN_DIRECTIVES = (
    "location",
    "proxy_pass",
    "proxy_set_header",
    "rewrite",
    "try_files",
    "return",
    "limit_req",
    "limit_conn",
    "client_max_body_size",
    "client_body_timeout",
    "proxy_connect_timeout",
    "proxy_read_timeout",
    "proxy_send_timeout",
    "proxy_next_upstream",
    "proxy_intercept_errors",
    "upstream",
)

# Nginx variables that carry the query string (and therefore the webhook secret).
_FORBIDDEN_LOG_VARIABLES = (
    "$request",
    "$request_uri",
    "$args",
    "$query_string",
    "$http_referer",
    "$is_args",
)

_REQUIRED_LOG_VARIABLES = (
    "$request_method",
    "$uri",
    "$status",
    "$body_bytes_sent",
    "$request_time",
)

_ALLOWED_LOG_VARIABLES = {
    "$remote_addr",
    "$time_local",
    *_REQUIRED_LOG_VARIABLES,
}


def _strip_inline_comment(line: str) -> str:
    """Strip an Nginx comment without treating a quoted ``#`` as one."""
    quote: str | None = None
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif quote is not None:
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "#":
            return line[:index]
    return line


def _active_config(path: Path) -> str:
    """Return *path* with whole-line and inline comments stripped.

    The reference explains at length WHY ``location``/``proxy_pass`` are banned,
    so a naive substring search would trip over its own documentation. Only
    active Nginx syntax is inspected here.
    """
    lines = [_strip_inline_comment(line) for line in path.read_text().splitlines()]
    return "\n".join(lines)


def _has_variable(text: str, variable: str) -> bool:
    """Match a whole Nginx variable, not a prefix.

    ``$request`` must NOT be reported for ``$request_method`` or ``$request_time``,
    which are exactly the safe variables the format is required to use.
    """
    return re.search(rf"{re.escape(variable)}(?![A-Za-z0-9_])", text) is not None


def _safe_log_format_body() -> str:
    """Return the active text of the ``kitilash_webhook_safe`` log_format."""
    active = _active_config(LOG_FORMATS_FILE)
    match = re.search(r"log_format\s+kitilash_webhook_safe\b(.*?);", active, re.DOTALL)
    assert match is not None, "log_format kitilash_webhook_safe is missing"
    return match.group(1)


def _directive_is_active(text: str, directive: str) -> bool:
    """Find a directive at a statement boundary, including after ``;``."""
    return (
        re.search(
            rf"(?:^|[;{{}}])\s*{re.escape(directive)}\b",
            text,
            re.MULTILINE,
        )
        is not None
    )


def _map_body(source: str, input_variable: str, output_variable: str) -> str:
    """Return the body of a simple top-level Nginx ``map`` block."""
    match = re.search(
        rf"^\s*map\s+{re.escape(input_variable)}\s+{re.escape(output_variable)}\s*"
        r"\{(?P<body>.*?)^\s*\}",
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing map {input_variable} -> {output_variable}"
    return match.group("body")


# ===========================================================================
# The reference files exist and the routing-shaped one is gone
# ===========================================================================


@pytest.mark.parametrize("path", [LOG_FORMATS_FILE, LOGGING_INCLUDE_FILE])
def test_reference_file_exists(path: Path) -> None:
    assert path.is_file(), f"missing Nginx reference: {path}"


def test_old_routing_shaped_reference_is_deleted() -> None:
    """The version that shipped ready location blocks must not come back."""
    assert not REMOVED_ROUTING_REFERENCE.exists(), (
        "the routing-shaped reference is back; a logging reference must not define routes"
    )


# ===========================================================================
# 1. No routing directives anywhere in the reference
# ===========================================================================


@pytest.mark.parametrize("path", [LOG_FORMATS_FILE, LOGGING_INCLUDE_FILE])
@pytest.mark.parametrize("directive", _FORBIDDEN_DIRECTIVES)
def test_reference_has_no_active_routing_directive(path: Path, directive: str) -> None:
    """A logging reference must never influence which location Nginx selects."""
    active = _active_config(path)
    assert not _directive_is_active(active, directive), (
        f"{path.name} declares the routing-affecting directive {directive!r}"
    )


@pytest.mark.parametrize("path", [LOG_FORMATS_FILE, LOGGING_INCLUDE_FILE])
def test_reference_declares_no_generic_webhook_location(path: Path) -> None:
    """Specifically the catch-all that would capture every webhook request."""
    active = _active_config(path)
    assert "^~ /webhook" not in active
    assert not re.search(r"^\s*location\b", active, re.MULTILINE)


def test_logging_include_contains_only_logging_directives() -> None:
    """Every complete statement must be exactly an access_log or error_log."""
    active = _active_config(LOGGING_INCLUDE_FILE)
    statements = [statement.strip() for statement in active.split(";") if statement.strip()]
    assert statements, "the include has no active directives at all"
    assert active.rstrip().endswith(";"), "the include has an unterminated statement"
    for statement in statements:
        directive = re.match(r"[A-Za-z_][A-Za-z0-9_]*", statement)
        assert directive is not None, f"cannot parse include statement: {statement!r}"
        assert directive.group(0) in {"access_log", "error_log"}, f"non-logging directive in the include: {statement!r}"


# ===========================================================================
# 2. The safe log format keeps the query string out
# ===========================================================================


@pytest.mark.parametrize("variable", _REQUIRED_LOG_VARIABLES)
def test_safe_log_format_keeps_useful_fields(variable: str) -> None:
    """Diagnostics must stay possible: method, path, status, size, duration."""
    assert _has_variable(_safe_log_format_body(), variable), f"log_format lost {variable}"


@pytest.mark.parametrize("variable", _FORBIDDEN_LOG_VARIABLES)
def test_safe_log_format_excludes_query_bearing_variables(variable: str) -> None:
    assert not _has_variable(_safe_log_format_body(), variable), (
        f"log_format uses {variable}, which can carry the webhook secret"
    )


def test_safe_log_format_uses_only_allowlisted_variables() -> None:
    """Block arbitrary headers, cookies and future query-bearing variables."""
    variables = set(re.findall(r"\$[A-Za-z0-9_]+", _safe_log_format_body()))
    assert variables == _ALLOWED_LOG_VARIABLES


def test_variable_matcher_does_not_confuse_request_with_request_method() -> None:
    """Guard the guard: the boundary check must not be a naive substring test."""
    assert _has_variable("method=$request_method", "$request_method")
    assert not _has_variable("method=$request_method", "$request")
    assert not _has_variable("request_time=$request_time", "$request")
    assert _has_variable("x=$request y", "$request")
    assert not _has_variable("uri=$uri", "$request_uri")


def test_safe_log_format_uses_uri_not_request_uri() -> None:
    body = _safe_log_format_body()
    assert _has_variable(body, "$uri")
    assert not _has_variable(body, "$request_uri")


# ===========================================================================
# 3. The logging include carries both channels
# ===========================================================================


def test_include_declares_the_safe_access_log() -> None:
    active = _active_config(LOGGING_INCLUDE_FILE)
    assert re.search(
        r"^\s*access_log\s+/var/log/nginx/webhooks_access\.log\s+kitilash_webhook_safe\s*;",
        active,
        re.MULTILINE,
    ), "the include must point the webhook access log at the safe format"


def test_include_declares_a_safe_error_log_policy() -> None:
    """A real error-log file would keep receiving the full request target."""
    active = _active_config(LOGGING_INCLUDE_FILE)
    assert re.search(r"^\s*error_log\s+/dev/null\s+emerg\s*;", active, re.MULTILINE), (
        "the include must discard request-level error logging for webhook routes"
    )
    # No error_log pointing at a persistent file.
    for match in re.finditer(r"^\s*error_log\s+(\S+)", active, re.MULTILINE):
        assert match.group(1) == "/dev/null", f"error_log persists to {match.group(1)!r}"


def test_log_format_file_declares_the_format_in_http_context() -> None:
    active = _active_config(LOG_FORMATS_FILE)
    assert re.search(r"^\s*log_format\s+kitilash_webhook_safe\b", active, re.MULTILINE)


def test_conditional_logging_selector_is_map_based_and_routing_neutral() -> None:
    """Variant B maps webhooks out of the combined log and into the safe log."""
    active = _active_config(LOG_FORMATS_FILE)
    webhook_map = _map_body(active, "$uri", "$kitilash_is_webhook")
    assert re.search(r"^\s*default\s+0\s*;", webhook_map, re.MULTILINE)
    assert re.search(
        rf"^\s*{re.escape('~^/webhooks?(?:/|$)')}\s+1\s*;",
        webhook_map,
        re.MULTILINE,
    )

    inverse_map = _map_body(
        active,
        "$kitilash_is_webhook",
        "$kitilash_is_not_webhook",
    )
    assert re.search(r"^\s*default\s+1\s*;", inverse_map, re.MULTILINE)
    assert re.search(r"^\s*1\s+0\s*;", inverse_map, re.MULTILINE)

    instructions = "\n".join(re.sub(r"^\s*# ?", "", line) for line in LOG_FORMATS_FILE.read_text().splitlines())
    assert re.search(
        r"access_log\s+/var/log/nginx/access\.log\s+combined\s+"
        r"if=\$kitilash_is_not_webhook\s*;",
        instructions,
    )
    assert re.search(
        r"access_log\s+/var/log/nginx/webhooks_access\.log\s+"
        r"kitilash_webhook_safe\s+if=\$kitilash_is_webhook\s*;",
        instructions,
    )


# ===========================================================================
# 4. The runbook states the operator contract
# ===========================================================================


def _runbook() -> str:
    return RUNBOOK_FILE.read_text()


def test_runbook_requires_inspecting_the_effective_config() -> None:
    text = _runbook()
    assert text.count("nginx -T") >= 2, "the runbook must inspect effective config before and after reload"
    assert "nginx -t" in text, "the runbook must require a config test before reload"
    assert "systemctl reload nginx" in text


def test_runbook_references_the_split_logging_examples() -> None:
    text = _runbook()
    assert "kitilash_webhook_log_formats.conf.example" in text
    assert "kitilash_webhook_logging.inc.example" in text
    assert "kitilash_webhook_safe_logging.conf.example" not in text


def test_runbook_forbids_copying_a_generic_webhook_location() -> None:
    text = _runbook()
    assert re.search(
        r"(?:Не|Нельзя).{0,180}location \^~ /webhook",
        text,
        re.DOTALL,
    ), "the runbook must explicitly forbid copying the generic catch-all"
    for marker in (
        "modifier/path",
        "proxy_pass",
        "body limits",
        "timeouts",
        "rate limiting",
        "buffering",
        "upstream",
    ):
        assert marker in text, f"the runbook does not require preserving {marker}"


def test_runbook_requires_production_route_parity() -> None:
    text = _runbook()
    parity = text[text.index("Production route-parity") :]
    for endpoint in (
        "GET  /health",
        "POST /webhooks/easyweek",
        "POST /webhooks/altegio",
        "GET  /webhook/whatsapp",
    ):
        assert endpoint in parity
    for comparison in (
        "HTTP status",
        "response body contract",
        "headers",
        "timeout",
        "body-size behaviour",
        "доступность upstream",
    ):
        assert comparison in parity
    assert parity.count("→ 403") >= 3


def test_runbook_explains_safe_error_log_scope_choice() -> None:
    text = _runbook()
    assert "error_log /dev/null emerg;" in text
    assert "server_name api.kitilash.com" in text
    assert "server-level suppression" in text
    assert "production-specific locations" in text
    assert "безопасный scope для `error_log` не выбран" in text


def test_runbook_has_distinct_normal_path_markers_and_log_searches() -> None:
    text = _runbook()
    for marker in (
        "EW_LOG_LEAK_TEST",
        "ALT_LOG_LEAK_TEST",
        "WA_LOG_LEAK_TEST",
    ):
        assert marker in text
    assert "/var/log/nginx/" in text
    assert "journalctl -u nginx" in text
    assert "docker compose -p altegio_bot logs" in text
    assert "not found" in text


def test_runbook_describes_isolated_failure_path() -> None:
    text = _runbook()
    failure = text[text.index("Failure-path marker test") :]
    for marker in (
        "localhost-only Nginx",
        "disposable Nginx container",
        "127.0.0.1:1",
        "FAILURE_LOG_LEAK_TEST",
        "502",
        "<temporary-nginx-log-directory>",
        "not found",
        "удалить disposable container",
        "403",
    ):
        assert marker in failure, f"failure-path procedure lost {marker!r}"


def test_runbook_orders_rotation_after_every_logging_gate() -> None:
    text = _runbook()
    parity_index = text.index("Production route-parity")
    normal_index = text.index("Normal-path marker tests")
    failure_index = text.index("Failure-path marker test")
    rotation_index = text.index("Ротация потенциально раскрытых секретов")
    assert parity_index < normal_index < failure_index < rotation_index


def test_runbook_forbids_copying_exact_locations_from_reference() -> None:
    text = _runbook()
    assert re.search(
        r"(?:Не|Нельзя).{0,220}exact locations",
        text,
        re.DOTALL,
    )


def test_runbook_describes_both_marker_tests() -> None:
    """A normal 403 exercises only the application path, not the error log."""
    text = _runbook()
    assert "EW_LOG_LEAK_TEST" in text, "normal-path marker test is missing"
    assert "hub.verify_token" in text, "the WhatsApp verification route must be covered"
    lowered = text.lower()
    assert "failure" in lowered and "upstream" in lowered, "failure-path marker test is missing"
    assert "502" in text, "the failure-path test should describe the proxy-level failure"


def test_runbook_does_not_claim_access_log_alone_closes_the_leak() -> None:
    text = _runbook()
    assert "error_log" in text, "the runbook must address the error-log channel"
    assert "error_log /dev/null emerg;" in text, "the runbook must state the exact safe error-log policy"


def test_runbook_requires_secret_rotation() -> None:
    text = _runbook()
    for secret_name in ("EASYWEEK_WEBHOOK_SECRET", "ALTEGIO_WEBHOOK_SECRET"):
        assert secret_name in text, f"the runbook must require rotating {secret_name}"
    assert "WHATSAPP_WEBHOOK_VERIFY_TOKEN" in text

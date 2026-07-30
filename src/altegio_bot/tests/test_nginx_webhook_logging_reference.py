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

# The selector answers "must this request stay out of the combined log?", NOT
# "is this a webhook". True means "safe log only".
_SAFE_LOG_SELECTOR_CASES = {
    # --- Canonical webhook paths, protected with or without a query.
    "webhook-root": ("/webhook", True),
    "webhooks-root": ("/webhooks", True),
    "whatsapp-path": ("/webhook/whatsapp", True),
    "easyweek-path": ("/webhooks/easyweek", True),
    "webhook-root-query": ("/webhook?x=1", True),
    "webhooks-root-query": ("/webhooks?x=1", True),
    "easyweek-query": ("/webhooks/easyweek?token=x", True),
    "altegio-query": ("/webhooks/altegio?secret=x", True),
    "whatsapp-query": ("/webhook/whatsapp?hub.verify_token=x", True),
    # --- URI-normalization bypasses: the RAW target looks like neither a webhook
    # path nor a literal secret key, but Nginx routes the NORMALISED URI to a
    # webhook and the application percent-decodes the parameter name itself.
    "percent-encoded-w": ("/%77ebhooks/easyweek?to%6ben=x", True),
    "percent-encoded-t": ("/%77ebhooks/easyweek?%74oken=x", True),
    "dot-segment-encoded-secret": ("/foo/../webhooks/altegio?sec%72et=x", True),
    "encoded-hub-verify-token": ("/%77ebhook/whatsapp?hub%2Everify_token=x", True),
    "encoded-slash": ("/webhooks%2Feasyweek?token=x", True),
    "dot-segment": ("/foo/../webhooks/easyweek?token=x", True),
    "encoded-userguid": ("/%77ebhooks/altegio?user%47uid=x", True),
    # --- Ordinary query-bearing requests. Intentional positives: their query is
    # dropped from the access log so no unknown parameter name can leak.
    "health-ordinary": ("/health?ordinary=value", True),
    "other-page": ("/other?page=2", True),
    "other-not-secret": ("/other?notsecret=x", True),
    "other-my-token": ("/other?mytoken=x", True),
    "other-secret-value": ("/other?foo=secret", True),
    "health-token": ("/health?token=x", True),
    "empty-query": ("/other?", True),
    # --- No query and not a webhook path: the combined log is still allowed.
    "site-root": ("/", False),
    "health": ("/health", False),
    "control": ("/control", False),
    "api-status": ("/api/status", False),
    "webhook-prefix-collision": ("/webhookevil", False),
    "webhooks-dash-collision": ("/webhooks-old", False),
    "webhook-underscore-collision": ("/webhook_backup", False),
    "nested-webhook": ("/api/webhook", False),
    "nested-easyweek": ("/api/webhooks/easyweek", False),
    "internal-destination": ("/internal-handler", False),
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


def _variables(text: str) -> list[str]:
    """Return variables, normalising Nginx ``$name`` and ``${name}`` syntax."""
    matches = re.finditer(
        r"\$(?:\{(?P<braced>[A-Za-z0-9_]+)\}|(?P<plain>[A-Za-z0-9_]+))",
        text,
    )
    return [f"${match.group('braced') or match.group('plain')}" for match in matches]


def _has_variable(text: str, variable: str) -> bool:
    """Match a whole Nginx variable, not a prefix.

    ``$request`` must NOT be reported for ``$request_method`` or ``$request_time``,
    which are exactly the safe variables the format is required to use.
    """
    return variable in _variables(text)


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


def _map_entries(body: str) -> list[tuple[str, str]]:
    """Parse a simple map body into exact selector/result pairs."""
    entries: list[tuple[str, str]] = []
    for statement in body.split(";"):
        tokens = statement.split()
        if not tokens:
            continue
        assert len(tokens) == 2, "map entries must have one selector and one result"
        entries.append((tokens[0], tokens[1]))
    return entries


def _safe_log_selector_patterns() -> list[tuple[str, bool]]:
    """Return the positive regexes of the original-request selector.

    Each entry is ``(pattern, case_insensitive)``: Nginx spells a
    case-insensitive regex ``~*`` and a case-sensitive one ``~``.
    """
    active = _active_config(LOG_FORMATS_FILE)
    body = _map_body(active, "$request_uri", "$kitilash_needs_safe_log")
    patterns: list[tuple[str, bool]] = []
    for selector, result in _map_entries(body):
        if result != "1" or not selector.startswith("~"):
            continue
        if selector.startswith("~*"):
            patterns.append((selector[2:], True))
        else:
            patterns.append((selector[1:], False))
    assert patterns, "safe-log selector must have at least one positive regex"
    return patterns


def _selector_matches(request_target: str) -> bool:
    """Evaluate the selector exactly the way Nginx would: first match wins."""
    for pattern, case_insensitive in _safe_log_selector_patterns():
        flags = re.IGNORECASE if case_insensitive else 0
        if re.search(pattern, request_target, flags) is not None:
            return True
    return False


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


def test_logging_include_has_exactly_the_two_safe_statements() -> None:
    """An additional combined access log would reintroduce the query leak."""
    statements = [
        " ".join(statement.split())
        for statement in _active_config(LOGGING_INCLUDE_FILE).split(";")
        if statement.strip()
    ]
    assert statements == [
        "access_log /var/log/nginx/webhooks_access.log kitilash_webhook_safe",
        "error_log /dev/null emerg",
    ]


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
    variables = set(_variables(_safe_log_format_body()))
    assert variables == _ALLOWED_LOG_VARIABLES


def test_variable_matcher_does_not_confuse_request_with_request_method() -> None:
    """Guard the guard: the boundary check must not be a naive substring test."""
    assert _has_variable("method=$request_method", "$request_method")
    assert not _has_variable("method=$request_method", "$request")
    assert not _has_variable("request_time=$request_time", "$request")
    assert _has_variable("x=$request y", "$request")
    assert not _has_variable("uri=$uri", "$request_uri")
    assert _has_variable("uri=${request_uri}", "$request_uri")
    assert not _has_variable("method=${request_method}", "$request")


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


def test_logging_include_never_uses_original_request_uri() -> None:
    """The secret-bearing request target is only valid as the boolean map input."""
    assert not _has_variable(
        _active_config(LOGGING_INCLUDE_FILE),
        "$request_uri",
    )


def test_log_format_file_declares_the_format_in_http_context() -> None:
    active = _active_config(LOG_FORMATS_FILE)
    assert re.search(r"^\s*log_format\s+kitilash_webhook_safe\b", active, re.MULTILINE)


def test_safe_log_selector_uses_original_request_uri() -> None:
    """Rewrites must not move a secret-bearing request back into combined."""
    active = _active_config(LOG_FORMATS_FILE)
    _map_body(active, "$request_uri", "$kitilash_needs_safe_log")
    assert not re.search(
        r"^\s*map\s+\$uri\s+\$kitilash_needs_safe_log\b",
        active,
        re.MULTILINE,
    ), "$uri is mutable and must never be the classification source"
    assert _variables(active).count("$request_uri") == 1


def test_safe_log_selector_declares_canonical_path_and_any_query() -> None:
    """The fail-safe is "a query exists", not a list of parameter names.

    Enumerating secret keys was unsound: a raw query key can be percent-encoded
    (``to%6ben``), and the application — not Nginx — decodes it. Only "this
    target carries a query string" is stable under every encoding.
    """
    assert _safe_log_selector_patterns() == [
        (r"^/webhooks?(?:/|\?|$)", False),
        (r"\?", False),
    ]


@pytest.mark.parametrize("case_id", _SAFE_LOG_SELECTOR_CASES)
def test_safe_log_selector_classification(case_id: str) -> None:
    """Exercise the Nginx selector regexes without echoing request targets."""
    request_target, expected = _SAFE_LOG_SELECTOR_CASES[case_id]
    if _selector_matches(request_target) is not expected:
        # Never print the target: these carry marker-shaped secrets by design.
        pytest.fail(f"safe-log selector misclassified case_id={case_id!r}", pytrace=False)


def test_every_query_bearing_target_is_excluded_from_combined() -> None:
    """The security contract itself, independent of the case table above.

    Any request target containing ``?`` must select the safe log, whatever its
    path shape or parameter spelling.
    """
    for target in (
        "/health?ordinary=value",
        "/other?page=2",
        "/webhooks/easyweek?token=x",
        "/%77ebhooks/easyweek?to%6ben=x",
        "/foo/../webhooks/altegio?sec%72et=x",
        "/%77ebhook/whatsapp?hub%2Everify_token=x",
        "/api/status?%74oken=x",
        "/deeply/unknown/route?whatever",
    ):
        assert _selector_matches(target), "a query-bearing target was left in the combined log"


def test_query_free_non_webhook_targets_still_use_the_combined_log() -> None:
    """The broad rule must not silently disable ordinary access logging."""
    for target in ("/", "/health", "/control", "/api/status"):
        assert not _selector_matches(target), "a query-free non-webhook request lost its combined entry"


def test_canonical_webhook_paths_are_safe_even_without_a_query() -> None:
    for target in ("/webhook", "/webhooks", "/webhook/whatsapp", "/webhooks/easyweek"):
        assert _selector_matches(target)


def test_known_secret_keys_remain_covered_as_regressions() -> None:
    """Kept as regression cases only — the boundary is now "query exists".

    These keys are what the codebase actually authenticates with (token →
    EasyWeek, secret + userGuid → Altegio, hub.verify_token → Meta), so they must
    never regress, but the protection no longer depends on enumerating them.
    """
    for key in ("token", "secret", "userGuid", "hub.verify_token"):
        assert _selector_matches(f"/anything?{key}=marker")
        assert _selector_matches(f"/anything?a=1&{key}=marker")
        # And the same key percent-encoded, which no literal list would catch.
        assert _selector_matches(f"/anything?%74{key[1:]}=marker")


def test_conditional_logging_selector_is_map_based_and_routing_neutral() -> None:
    """Variant B maps query-bearing requests out of the combined log."""
    active = _active_config(LOG_FORMATS_FILE)
    safe_log_map = _map_body(active, "$request_uri", "$kitilash_needs_safe_log")
    assert _map_entries(safe_log_map) == [
        ("default", "0"),
        ("~^/webhooks?(?:/|\\?|$)", "1"),
        ("~\\?", "1"),
    ]

    inverse_map = _map_body(
        active,
        "$kitilash_needs_safe_log",
        "$kitilash_can_use_combined_log",
    )
    assert _map_entries(inverse_map) == [
        ("default", "1"),
        ("1", "0"),
    ]

    reference = LOG_FORMATS_FILE.read_text()
    example = reference[reference.index("# Then, inside") : reference.index("# CAVEAT:")]
    uncommented = "\n".join(re.sub(r"^\s*# ?", "", line) for line in example.splitlines())
    access_logs = [
        " ".join(match.group(0).split())
        for match in re.finditer(
            r"^\s*access_log\b.*?;",
            uncommented,
            re.MULTILINE | re.DOTALL,
        )
    ]
    assert access_logs == [
        ("access_log /var/log/nginx/access.log combined if=$kitilash_can_use_combined_log;"),
        ("access_log /var/log/nginx/webhooks_access.log kitilash_webhook_safe if=$kitilash_needs_safe_log;"),
    ]


def test_selector_variable_names_are_confined_to_the_logging_layer() -> None:
    """Renaming the map must not have reached routing or application code."""
    stale = ("$kitilash_is_webhook", "$kitilash_is_not_webhook")
    for path in (LOG_FORMATS_FILE, LOGGING_INCLUDE_FILE):
        text = path.read_text()
        for name in stale:
            assert name not in text, f"{path.name} still references the old {name}"


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


def test_runbook_distinguishes_selector_input_from_safe_logged_path() -> None:
    text = re.sub(r"\s+", " ", _runbook())
    for contract in (
        "`$uri` изменяется",
        "`$request_uri` сохраняет исходный",
        "source для boolean `map`",
        "`$request_uri` никогда не добавляется в `log_format`",
        "safe format продолжает логировать текущий безопасный `$uri`",
    ):
        assert contract in text


def test_runbook_requires_following_every_internal_redirect_destination() -> None:
    text = re.sub(r"\s+", " ", _runbook())
    for destination in (
        "rewrite",
        "named locations",
        "`try_files`",
        "`index`",
        "`error_page`",
        "internal redirects",
        "regex locations",
    ):
        assert destination in text
    assert "access_log ... combined;" in text
    assert "error_log /var/log/nginx/...;" in text
    assert "security DoD не выполнен" in text


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


def test_runbook_describes_internal_redirect_marker_test() -> None:
    text = _runbook()
    internal_redirect = text[
        text.index("Internal-redirect marker test") : text.index("Ротация потенциально раскрытых секретов")
    ]
    for marker in (
        "disposable Nginx container",
        "тот же `$request_uri` map",
        "INTERNAL_REDIRECT_MARKER",
        "/webhooks/easyweek",
        "/internal-handler",
        "non-webhook control request",
        "combined log",
        "safe webhook log",
        "uri=/internal-handler",
        "error log не содержит marker",
        "удалить disposable container",
        "not found",
    ):
        assert marker in internal_redirect


def test_runbook_orders_rotation_after_every_logging_gate() -> None:
    text = _runbook()
    parity_index = text.index("Production route-parity")
    normal_index = text.index("Normal-path marker tests")
    failure_index = text.index("Failure-path marker test")
    internal_redirect_index = text.index("Internal-redirect marker test")
    rotation_index = text.index("Ротация потенциально раскрытых секретов")
    assert parity_index < normal_index < failure_index < internal_redirect_index < rotation_index


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


# ===========================================================================
# 5. $request_uri is confined to the boolean selector
# ===========================================================================


def test_request_uri_is_only_the_map_source() -> None:
    """The raw target may classify, but must never be recorded anywhere.

    $request_uri carries the secret, so exactly one active occurrence is allowed
    in the whole reference: the input of the boolean map.
    """
    active = _active_config(LOG_FORMATS_FILE)
    assert _variables(active).count("$request_uri") == 1, (
        "$request_uri may appear only once, as the selector map source"
    )
    assert re.search(
        r"^\s*map\s+\$request_uri\s+\$kitilash_needs_safe_log\s*\{",
        active,
        re.MULTILINE,
    ), "the single occurrence must be the map source"


def test_request_uri_absent_from_safe_log_format() -> None:
    assert not _has_variable(_safe_log_format_body(), "$request_uri")


def test_request_uri_absent_from_logging_include() -> None:
    assert not _has_variable(_active_config(LOGGING_INCLUDE_FILE), "$request_uri")


@pytest.mark.parametrize("path", [LOG_FORMATS_FILE, LOGGING_INCLUDE_FILE])
def test_no_access_log_directive_uses_request_uri(path: Path) -> None:
    """Neither an access_log FORMAT nor its PATH may contain the raw target."""
    active = _active_config(path)
    for match in re.finditer(r"^\s*access_log\b[^;]*;", active, re.MULTILINE):
        statement = match.group(0)
        assert not _has_variable(statement, "$request_uri")
        assert not _has_variable(statement, "$request")
        assert not _has_variable(statement, "$args")
        assert not _has_variable(statement, "$query_string")


def test_commented_conditional_access_log_examples_are_also_clean() -> None:
    """The Variant B example an operator copies must be safe as written."""
    reference = LOG_FORMATS_FILE.read_text()
    example = reference[reference.index("# Then, inside") : reference.index("# CAVEAT:")]
    uncommented = "\n".join(re.sub(r"^\s*# ?", "", line) for line in example.splitlines())
    for match in re.finditer(r"^\s*access_log\b[^;]*;", uncommented, re.MULTILINE):
        statement = match.group(0)
        for forbidden in ("$request_uri", "$request", "$args", "$query_string"):
            assert not _has_variable(statement, forbidden), f"example access_log leaks {forbidden}"


def test_runbook_documents_uri_normalization_risk() -> None:
    """The operator must know raw target != routed URI, and not hand-roll it."""
    text = _runbook()
    assert "$request_uri" in text
    assert "--path-as-is" in text, "the runbook must require a non-normalising client"
    lowered = text.lower()
    assert "percent" in lowered or "%77" in text, "percent-encoding risk must be documented"
    assert "dot" in lowered or "/../" in text, "dot-segment risk must be documented"


def test_runbook_documents_the_broad_query_logging_contract() -> None:
    """The trade-off has to be written down where the operator will read it."""
    text = _runbook()
    for marker in (
        "to%6ben",
        "%74oken",
        "sec%72et",
        "hub%2Everify_token",
        "/health?ordinary=",
        "analytics",
        "trade-off",
        "$kitilash_needs_safe_log",
    ):
        assert marker in text, f"the runbook does not document {marker!r}"
    lowered = text.lower()
    assert "перечисление имён секретных ключей" in lowered, (
        "the runbook must say that enumerating secret parameter names is unsound"
    )


def test_runbook_states_the_mandatory_ci_commands() -> None:
    """A plain ``uv run pytest -q`` must never be presented as sufficient."""
    text = re.sub(r"\s+", " ", _runbook())
    assert "ALTEGIO_REQUIRE_NGINX_LOGTEST=1" in text
    assert "--ignore=src/altegio_bot/tests/test_nginx_webhook_logging_integration.py" in text

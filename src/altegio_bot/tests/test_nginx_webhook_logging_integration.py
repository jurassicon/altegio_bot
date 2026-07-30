"""Disposable-Nginx proof that request secrets never reach the combined log.

The static guards in ``test_nginx_webhook_logging_reference.py`` only read the
reference as text. They cannot tell whether *Nginx itself* classifies a
percent-encoded or dot-segment request the way we expect, because that depends on
Nginx's own URI normalisation — which is exactly the mechanism that produced the
bypass this test exists for:

    $request_uri is the RAW target, but the location is chosen from the
    NORMALISED URI, and the query is percent-decoded by the *application*.
    ``/%77ebhooks/easyweek?to%6ben=…`` is routed as a webhook and read by the
    handler as a plain ``token``, while a raw regex sees neither a webhook path
    nor a literal ``token=``. The selector therefore keys off "a query exists".

So this runs a real, digest-pinned Nginx in a throwaway container, bound to
loopback, writing into a temporary log directory, using the SAME log_format, the
SAME selector maps and the SAME conditional access_log / error_log policy as the
versioned reference. Production Nginx and the production API are never touched.

The pass criterion is never an HTTP status: it is the absence of the marker from
the combined log, the safe log, the error log, and the whole log directory. The
statuses are still validated as *transport* evidence — a connection failure must
not masquerade as a clean run.

By default the test skips when Docker is unavailable. CI runs it in mandatory
mode, where a missing Docker/image fails instead of skipping::

    ALTEGIO_REQUIRE_NGINX_LOGTEST=1 uv run pytest -q \\
      src/altegio_bot/tests/test_nginx_webhook_logging_integration.py

Because it has that dedicated gate, the general application suite excludes it::

    uv run pytest -q \\
      --ignore=src/altegio_bot/tests/test_nginx_webhook_logging_integration.py
"""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

# Immutable pin. A tag alone is mutable — the registry can move nginx:1.27-alpine
# to a different build, silently changing the URI-normalisation behaviour this
# test exists to characterise. The digest is the multi-arch OCI index, so the
# same reference resolves on the amd64 GitHub runner and on arm64 locally.
NGINX_IMAGE = "nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10"
REQUIRE_ENV = "ALTEGIO_REQUIRE_NGINX_LOGTEST"

_REPO_ROOT = Path(__file__).resolve().parents[3]
_REFERENCE = _REPO_ROOT / "deploy" / "nginx" / "kitilash_webhook_log_formats.conf.example"

# Request targets that must never reach the combined log. The raw form is
# preserved on the wire via `curl --path-as-is`; each case gets its own marker.
_SECRET_CASES: dict[str, str] = {
    # Canonical webhook routes.
    "canonical-easyweek": "/webhooks/easyweek?token=",
    "canonical-altegio": "/webhooks/altegio?secret=",
    "whatsapp-verify": "/webhook/whatsapp?hub.verify_token=",
    # Path normalisation: raw target is not a webhook path, routed URI is.
    "percent-encoded-path": "/%77ebhooks/easyweek?token=",
    "encoded-slash": "/webhooks%2Feasyweek?token=",
    "dot-segment": "/foo/../webhooks/easyweek?token=",
    # Query-key encoding: the parameter NAME is percent-encoded, so no literal
    # key list could ever match the raw target.
    "encoded-key-token-mid": "/%77ebhooks/easyweek?to%6ben=",
    "encoded-key-token-head": "/%77ebhooks/easyweek?%74oken=",
    "encoded-key-secret": "/foo/../webhooks/altegio?sec%72et=",
    "encoded-key-hub-verify": "/%77ebhook/whatsapp?hub%2Everify_token=",
    # Ordinary, non-webhook, query-bearing requests: covered by the same rule.
    "ordinary-health-query": "/health?ordinary=",
    "ordinary-page-query": "/other?page=",
}

# A query string must never appear in the safe format, whatever it contains.
_FORBIDDEN_IN_SAFE_LOG = ("token=", "secret=", "userGuid=", "hub.verify_token", "?")


def _mandatory() -> bool:
    return os.environ.get(REQUIRE_ENV, "").strip() == "1"


def _unavailable(reason: str) -> None:
    """Skip normally, but FAIL when the mandatory gate is requested."""
    if _mandatory():
        pytest.fail(f"{REQUIRE_ENV}=1 but the Nginx log test cannot run: {reason}", pytrace=False)
    pytest.skip(reason)


def is_valid_http_status(value: str) -> bool:
    """True only for a real HTTP status line code.

    ``curl`` writes ``%{http_code}`` as ``000`` when no HTTP response was
    received at all — a refused connection, a TLS failure, a timeout. ``000`` is
    a digit string, so ``str.isdigit()`` accepts it and a "no marker found"
    assertion would then pass without any request ever having been served.
    """
    if len(value) != 3 or not value.isdigit():
        return False
    return 100 <= int(value) <= 599


def _run(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    """Run a command, turning a timeout into a non-zero result instead of raising.

    A slow ``docker pull`` must surface through the normal availability gate
    (skip, or fail under ``ALTEGIO_REQUIRE_NGINX_LOGTEST=1``) rather than as an
    unhandled ``TimeoutExpired`` in fixture setup.
    """
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(args, returncode=124, stdout="", stderr=f"timeout after {timeout}s")


def _image_present() -> bool:
    """Inspect by the immutable reference, never by a bare tag."""
    return _run("docker", "image", "inspect", NGINX_IMAGE, timeout=60).returncode == 0


def _reference_selector_block() -> str:
    """Extract the active log_format + map definitions from the reference.

    Reusing the reference verbatim is the point: if the shipped selector ever
    stops protecting a case, this integration test fails with it.
    """
    lines: list[str] = []
    for raw in _REFERENCE.read_text().splitlines():
        stripped = raw.split("#", 1)[0] if not raw.lstrip().startswith("#") else ""
        if stripped.strip():
            lines.append(stripped)
    block = "\n".join(lines)
    assert "log_format kitilash_webhook_safe" in block, "reference lost its safe log_format"
    assert "map $request_uri $kitilash_needs_safe_log" in block, "reference lost its selector map"
    return block


def _nginx_conf(log_dir: str) -> str:
    """Build a disposable config: reference logging + throwaway test routing.

    The routing here exists ONLY so the container has something to match; it is
    never copied to production, which is why the versioned reference itself
    carries no routing.
    """
    return f"""
worker_processes 1;
error_log {log_dir}/startup_error.log emerg;
pid /tmp/nginx-logtest.pid;
events {{ worker_connections 64; }}
http {{
    access_log off;
    log_format combined_test '$remote_addr "$request" $status';

{_reference_selector_block()}

    server {{
        listen 8080;
        server_name localhost;

        # Conditional access logging exactly as the reference documents it.
        access_log {log_dir}/combined_access.log combined_test
            if=$kitilash_can_use_combined_log;
        access_log {log_dir}/webhooks_access.log kitilash_webhook_safe
            if=$kitilash_needs_safe_log;

        # Safe error-log policy from the logging-only include.
        error_log /dev/null emerg;

        location / {{
            return 200 "ok\\n";
        }}
    }}
}}
"""


def _request(raw_target: str) -> tuple[int, str]:
    """Send one raw request target, returning ``(curl_returncode, status)``."""
    result = _run(
        "curl",
        "-sS",
        "--path-as-is",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        f"http://127.0.0.1:18080{raw_target}",
        timeout=20,
    )
    return result.returncode, result.stdout.strip()


@pytest.fixture(scope="module")
def nginx_logs() -> Path:  # type: ignore[misc]
    """Start a disposable Nginx, replay every case, yield the log directory."""
    if shutil.which("docker") is None:
        _unavailable("docker CLI not found")
    if shutil.which("curl") is None:
        _unavailable("curl not found")
    if _run("docker", "info", timeout=60).returncode != 0:
        _unavailable("docker daemon is not usable")

    tmp_root = Path(os.environ.get("TMPDIR", "/tmp")) / f"nginx-logtest-{uuid.uuid4().hex[:8]}"
    log_dir = tmp_root / "logs"
    log_dir.mkdir(parents=True)
    log_dir.chmod(0o777)  # the container's nginx user must be able to write here
    conf_path = tmp_root / "nginx.conf"
    conf_path.write_text(_nginx_conf("/logs"))
    conf_path.chmod(0o644)

    container = f"nginx-logtest-{uuid.uuid4().hex[:8]}"
    # Only pull when the pinned image is missing: a cold pull can take minutes and
    # must not be mistaken for a broken test.
    if not _image_present():
        _run("docker", "pull", NGINX_IMAGE, timeout=600)
    if not _image_present():
        shutil.rmtree(tmp_root, ignore_errors=True)
        _unavailable(f"cannot obtain the digest-pinned image {NGINX_IMAGE}")

    started = _run(
        "docker",
        "run",
        "-d",
        "--name",
        container,
        "-p",
        "127.0.0.1:18080:8080",  # loopback only
        "-v",
        f"{conf_path}:/etc/nginx/nginx.conf:ro",
        "-v",
        f"{log_dir}:/logs",
        NGINX_IMAGE,
        timeout=180,
    )
    if started.returncode != 0:
        shutil.rmtree(tmp_root, ignore_errors=True)
        _unavailable("cannot start the disposable Nginx container")

    try:
        # Wait for readiness. This target carries no marker, so a failure message
        # here cannot disclose one.
        ready = False
        for _ in range(60):
            probe = _run("curl", "-sS", "-o", "/dev/null", "http://127.0.0.1:18080/ready", timeout=15)
            if probe.returncode == 0:
                ready = True
                break
            time.sleep(0.5)
        if not ready:
            logs = _run("docker", "logs", container, timeout=30)
            pytest.fail(f"disposable Nginx never became ready: {logs.stderr[-500:]}", pytrace=False)

        markers: dict[str, str] = {}
        results: dict[str, tuple[int, str]] = {}

        # Control 1 — no query, non-webhook path. Proves the combined log is
        # actually enabled, so a later "marker absent from combined" cannot pass
        # just because nothing is ever written there.
        combined_control = f"CONTROLCOMBINED{secrets.token_hex(8)}"
        markers["__control_combined__"] = combined_control
        results["__control_combined__"] = _request(f"/control/{combined_control}")

        # Control 2 — ordinary non-webhook request WITH a query. Proves the broad
        # query contract: it must leave combined and appear in the safe log with
        # its query stripped.
        query_control = f"CONTROLQUERY{secrets.token_hex(8)}"
        markers["__control_query__"] = query_control
        results["__control_query__"] = _request(f"/control-query?ordinary={query_control}")

        for case_id, raw_target in _SECRET_CASES.items():
            marker = f"NGXLEAK_{case_id.replace('-', '').upper()}_{secrets.token_hex(8)}"
            markers[case_id] = marker
            results[case_id] = _request(f"{raw_target}{marker}")

        time.sleep(1.0)  # let Nginx flush its buffers
        (log_dir / "_markers.txt").write_text(repr(markers))
        (log_dir / "_results.txt").write_text(repr(results))
        yield log_dir
    finally:
        _run("docker", "rm", "-f", container, timeout=120)
        shutil.rmtree(tmp_root, ignore_errors=True)


def _markers(log_dir: Path) -> dict[str, str]:
    import ast

    return ast.literal_eval((log_dir / "_markers.txt").read_text())


def _results(log_dir: Path) -> dict[str, tuple[int, str]]:
    import ast

    return ast.literal_eval((log_dir / "_results.txt").read_text())


def _read(log_dir: Path, name: str) -> str:
    path = log_dir / name
    return path.read_text(errors="replace") if path.exists() else ""


def _all_log_text(log_dir: Path) -> str:
    """Every log file in the directory, excluding our own bookkeeping files."""
    chunks = []
    for path in sorted(log_dir.iterdir()):
        if path.name.startswith("_") or not path.is_file():
            continue
        chunks.append(path.read_text(errors="replace"))
    return "\n".join(chunks)


# ===========================================================================
# Transport evidence: every case really produced an HTTP response
# ===========================================================================


_ALL_CASES = ["__control_combined__", "__control_query__", *_SECRET_CASES]


@pytest.mark.parametrize("case_id", _ALL_CASES)
def test_curl_transport_succeeded(nginx_logs: Path, case_id: str) -> None:
    """A refused connection or timeout must fail, not look like a clean run."""
    returncode, status = _results(nginx_logs)[case_id]
    # Report the case id and the transport facts only — never the raw target.
    assert returncode == 0, f"case_id={case_id!r} curl transport failed (rc={returncode}, status={status!r})"


@pytest.mark.parametrize("case_id", _ALL_CASES)
def test_recorded_http_status_is_in_range(nginx_logs: Path, case_id: str) -> None:
    """Document what Nginx actually did, and reject ``000`` as "no response".

    A given Nginx build may reject an encoded form before routing (e.g. 400).
    That is acceptable and must not be reported as "reached the webhook
    handler" — the pass criterion is marker absence, which the tests below
    cover. This test only pins that a real HTTP response came back.
    """
    returncode, status = _results(nginx_logs)[case_id]
    assert is_valid_http_status(status), (
        f"case_id={case_id!r} produced no HTTP response (rc={returncode}, status={status!r})"
    )


def test_every_case_was_actually_sent(nginx_logs: Path) -> None:
    assert set(_results(nginx_logs)) == set(_ALL_CASES)


# ===========================================================================
# Controls: combined is alive, and a query alone is enough to leave it
# ===========================================================================


def test_query_free_control_request_is_written_to_the_combined_log(nginx_logs: Path) -> None:
    """Without this, every "not in combined" assertion could pass vacuously."""
    control = _markers(nginx_logs)["__control_combined__"]
    assert control in _read(nginx_logs, "combined_access.log"), "the combined log is not receiving ordinary requests"
    assert control not in _read(nginx_logs, "webhooks_access.log")


def test_ordinary_query_control_leaves_the_combined_log(nginx_logs: Path) -> None:
    """The broad contract: a plain non-webhook request with a query is safe-logged."""
    control = _markers(nginx_logs)["__control_query__"]
    combined = _read(nginx_logs, "combined_access.log")
    safe = _read(nginx_logs, "webhooks_access.log")
    assert control not in combined, "an ordinary query-bearing request reached the combined log"
    assert "control-query" not in combined, "the query-bearing control produced a combined-log entry"
    assert "uri=/control-query" in safe, "the query-bearing control is missing from the safe log"
    assert control not in safe, "the safe log recorded the query string"


# ===========================================================================
# Every marker-bearing case stays out of the unsafe channels
# ===========================================================================


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_marker_never_reaches_the_combined_log(nginx_logs: Path, case_id: str) -> None:
    marker = _markers(nginx_logs)[case_id]
    assert marker not in _read(nginx_logs, "combined_access.log"), f"case_id={case_id!r} leaked into the combined log"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_case_has_no_combined_entry_at_all(nginx_logs: Path, case_id: str) -> None:
    """Not merely marker-free: the request must not be logged there at all."""
    marker = _markers(nginx_logs)[case_id]
    prefix = marker.split("_")[1]  # the case tag, without the random part
    assert prefix not in _read(nginx_logs, "combined_access.log"), f"case_id={case_id!r} produced a combined-log entry"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_marker_never_reaches_the_safe_log(nginx_logs: Path, case_id: str) -> None:
    """The safe log records the request, but never its query string."""
    marker = _markers(nginx_logs)[case_id]
    assert marker not in _read(nginx_logs, "webhooks_access.log")


def test_safe_log_recorded_the_requests_without_query(nginx_logs: Path) -> None:
    safe = _read(nginx_logs, "webhooks_access.log")
    assert safe.strip(), "the safe webhook log captured nothing at all"
    for forbidden in _FORBIDDEN_IN_SAFE_LOG:
        assert forbidden not in safe, f"the safe log contains {forbidden!r}"
    # It must still be useful: method and a path are present.
    assert "method=POST" in safe or "method=GET" in safe
    assert "uri=/" in safe


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_marker_never_reaches_any_error_log(nginx_logs: Path, case_id: str) -> None:
    marker = _markers(nginx_logs)[case_id]
    for name in ("startup_error.log", "error.log", "webhooks_error.log"):
        assert marker not in _read(nginx_logs, name), f"case_id={case_id!r} leaked into {name}"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_marker_absent_from_the_whole_log_directory(nginx_logs: Path, case_id: str) -> None:
    """Catch-all: no file Nginx wrote may contain the marker."""
    marker = _markers(nginx_logs)[case_id]
    assert marker not in _all_log_text(nginx_logs), f"case_id={case_id!r} leaked into some log file"


# ===========================================================================
# The image pin and the mandatory gate are themselves regressions
# ===========================================================================


def test_nginx_image_is_pinned_by_immutable_digest() -> None:
    """A tag can be repointed in the registry; a digest cannot."""
    assert "@sha256:" in NGINX_IMAGE, "the test image must be pinned by digest, not by a mutable tag"
    digest = NGINX_IMAGE.split("@sha256:", 1)[1]
    assert len(digest) == 64, "a sha256 digest is 64 hex characters"
    assert all(char in "0123456789abcdef" for char in digest), "digest must be lowercase hex"


def test_container_and_registry_calls_all_use_the_pinned_reference() -> None:
    """No code path — pull, inspect or run — may fall back to a bare tag.

    Checked over the parsed AST rather than the raw text, so this guard does not
    trip over its own assertion messages.
    """
    import ast
    import re

    image_reference = re.compile(r"nginx[:@]\S+")
    tree = ast.parse(Path(__file__).read_text())
    image_literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and image_reference.fullmatch(node.value)
    ]
    assert image_literals == [NGINX_IMAGE], f"an unpinned image literal is used: {image_literals}"

    # pull, inspect and run must each reference the constant, never a literal.
    uses = [node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == "NGINX_IMAGE"]
    assert len(uses) >= 3, "pull, inspect and run must all use the pinned constant"


def test_mandatory_mode_helper_fails_instead_of_skipping(monkeypatch) -> None:
    """With the gate on, an unavailable Docker must FAIL the run, not skip it.

    ``pytest.fail``/``pytest.skip`` raise ``BaseException`` subclasses, so this
    guard has to catch ``BaseException`` to observe which one was chosen.
    """
    monkeypatch.setenv(REQUIRE_ENV, "1")
    with pytest.raises(BaseException) as exc_info:  # noqa: B017,PT011 - see docstring
        _unavailable("simulated missing docker")
    assert type(exc_info.value).__name__ == "Failed"
    assert "simulated missing docker" in str(exc_info.value)


def test_default_mode_helper_skips(monkeypatch) -> None:
    """Without the gate, a missing Docker is a skip so local runs stay usable."""
    monkeypatch.delenv(REQUIRE_ENV, raising=False)
    with pytest.raises(BaseException) as exc_info:  # noqa: B017,PT011 - see docstring above
        _unavailable("simulated missing docker")
    assert type(exc_info.value).__name__ == "Skipped"


# ===========================================================================
# The status validator, unit-tested without Docker
# ===========================================================================


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("200", True),
        ("400", True),
        ("599", True),
        ("100", True),
        ("000", False),  # curl: no HTTP response at all
        ("0", False),
        ("99", False),
        ("600", False),
        ("", False),
        ("abc", False),
        ("20", False),
        ("2000", False),
        (" 200", False),
    ],
)
def test_http_status_validator(value: str, expected: bool) -> None:
    assert is_valid_http_status(value) is expected

"""Disposable-Nginx proof that webhook secrets never reach the combined log.

The static guards in ``test_nginx_webhook_logging_reference.py`` only read the
reference as text. They cannot tell whether *Nginx itself* classifies a
percent-encoded or dot-segment request the way we expect, because that depends on
Nginx's own URI normalisation — which is exactly the mechanism that produced the
bypass this test exists for:

    $request_uri is the RAW target, but the location is chosen from the
    NORMALISED URI. ``/%77ebhooks/easyweek?token=…`` can be routed as a webhook
    while a canonical path regex on the raw target says "not a webhook", sending
    the secret to the combined log.

So this runs a real, pinned Nginx in a throwaway container, bound to loopback,
writing into a temporary log directory, using the SAME log_format, the SAME
selector maps and the SAME conditional access_log / error_log policy as the
versioned reference. Production Nginx and the production API are never touched.

The pass criterion is never an HTTP status: it is the absence of the marker from
the combined log, the safe log, the error log, and the whole log directory.

By default the test skips when Docker is unavailable. In CI or when verifying the
fix, run it in mandatory mode so a missing Docker/image fails instead of skipping::

    ALTEGIO_REQUIRE_NGINX_LOGTEST=1 uv run pytest -q \\
      src/altegio_bot/tests/test_nginx_webhook_logging_integration.py
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

# Pinned so a silent upstream change cannot alter normalisation behaviour.
NGINX_IMAGE = "nginx:1.27-alpine"
REQUIRE_ENV = "ALTEGIO_REQUIRE_NGINX_LOGTEST"

_REPO_ROOT = Path(__file__).resolve().parents[3]
_REFERENCE = _REPO_ROOT / "deploy" / "nginx" / "kitilash_webhook_log_formats.conf.example"

# Secret-bearing request targets. Each is exercised with its own marker; the raw
# form is preserved on the wire via `curl --path-as-is`.
_SECRET_CASES: dict[str, str] = {
    "canonical-easyweek": "/webhooks/easyweek?token=",
    "percent-encoded-w": "/%77ebhooks/easyweek?token=",
    "encoded-slash": "/webhooks%2Feasyweek?token=",
    "dot-segment": "/foo/../webhooks/easyweek?token=",
    "canonical-altegio": "/webhooks/altegio?secret=",
    "whatsapp-verify": "/webhook/whatsapp?hub.verify_token=",
    "non-webhook-path-with-token": "/health?token=",
}

_FORBIDDEN_IN_SAFE_LOG = ("token=", "secret=", "userGuid=", "hub.verify_token", "?")


def _mandatory() -> bool:
    return os.environ.get(REQUIRE_ENV, "").strip() == "1"


def _unavailable(reason: str) -> None:
    """Skip normally, but FAIL when the mandatory gate is requested."""
    if _mandatory():
        pytest.fail(f"{REQUIRE_ENV}=1 but the Nginx log test cannot run: {reason}", pytrace=False)
    pytest.skip(reason)


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
    assert "map $request_uri $kitilash_is_webhook" in block, "reference lost its selector map"
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
            if=$kitilash_is_not_webhook;
        access_log {log_dir}/webhooks_access.log kitilash_webhook_safe
            if=$kitilash_is_webhook;

        # Safe error-log policy from the logging-only include.
        error_log /dev/null emerg;

        location / {{
            return 200 "ok\\n";
        }}
    }}
}}
"""


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
        _unavailable(f"cannot obtain {NGINX_IMAGE}")

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
        # Wait for readiness.
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

        # Control request FIRST: proves the combined log is actually enabled, so a
        # later "marker absent from combined" cannot pass just because nothing is
        # ever written there.
        control = f"CONTROL_{secrets.token_hex(8)}"
        _run(
            "curl",
            "-sS",
            "--path-as-is",
            "-o",
            "/dev/null",
            f"http://127.0.0.1:18080/control?probe={control}",
            timeout=15,
        )
        markers["__control__"] = control

        statuses: dict[str, str] = {}
        for case_id, raw_target in _SECRET_CASES.items():
            marker = f"NGXLEAK_{case_id.replace('-', '').upper()}_{secrets.token_hex(8)}"
            markers[case_id] = marker
            result = _run(
                "curl",
                "-sS",
                "--path-as-is",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                f"http://127.0.0.1:18080{raw_target}{marker}",
                timeout=20,
            )
            statuses[case_id] = result.stdout.strip() or "no-response"

        time.sleep(1.0)  # let Nginx flush its buffers
        (log_dir / "_markers.txt").write_text(repr(markers))
        (log_dir / "_statuses.txt").write_text(repr(statuses))
        yield log_dir
    finally:
        _run("docker", "rm", "-f", container, timeout=120)
        shutil.rmtree(tmp_root, ignore_errors=True)


def _markers(log_dir: Path) -> dict[str, str]:
    import ast

    return ast.literal_eval((log_dir / "_markers.txt").read_text())


def _statuses(log_dir: Path) -> dict[str, str]:
    import ast

    return ast.literal_eval((log_dir / "_statuses.txt").read_text())


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
# Control: the combined log really is active
# ===========================================================================


def test_control_request_is_written_to_the_combined_log(nginx_logs: Path) -> None:
    """Without this, every "not in combined" assertion could pass vacuously."""
    control = _markers(nginx_logs)["__control__"]
    combined = _read(nginx_logs, "combined_access.log")
    assert control in combined, "the combined log is not receiving ordinary requests"
    assert control not in _read(nginx_logs, "webhooks_access.log")


# ===========================================================================
# Every secret-bearing case stays out of the unsafe channels
# ===========================================================================


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_secret_marker_never_reaches_the_combined_log(nginx_logs: Path, case_id: str) -> None:
    marker = _markers(nginx_logs)[case_id]
    combined = _read(nginx_logs, "combined_access.log")
    assert marker not in combined, f"case_id={case_id!r} leaked its secret into the combined log"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_secret_request_has_no_combined_entry_at_all(nginx_logs: Path, case_id: str) -> None:
    """Not merely marker-free: the request must not be logged there at all."""
    marker = _markers(nginx_logs)[case_id]
    prefix = marker.split("_")[1]  # the case tag, without the random part
    combined = _read(nginx_logs, "combined_access.log")
    assert prefix not in combined, f"case_id={case_id!r} produced a combined-log entry"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_secret_marker_never_reaches_the_safe_log(nginx_logs: Path, case_id: str) -> None:
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
def test_secret_marker_never_reaches_any_error_log(nginx_logs: Path, case_id: str) -> None:
    marker = _markers(nginx_logs)[case_id]
    for name in ("startup_error.log", "error.log", "webhooks_error.log"):
        assert marker not in _read(nginx_logs, name), f"case_id={case_id!r} leaked into {name}"


@pytest.mark.parametrize("case_id", list(_SECRET_CASES))
def test_secret_marker_absent_from_the_whole_log_directory(nginx_logs: Path, case_id: str) -> None:
    """Catch-all: no file Nginx wrote may contain the marker."""
    marker = _markers(nginx_logs)[case_id]
    assert marker not in _all_log_text(nginx_logs), f"case_id={case_id!r} leaked into some log file"


def test_recorded_status_per_case_is_reported_not_asserted(nginx_logs: Path) -> None:
    """Document what Nginx actually did with each raw form.

    A given Nginx build may reject an encoded form before routing (e.g. 400). That
    is acceptable and must not be reported as "reached the webhook handler" — the
    pass criterion is marker absence, which the tests above cover. This test only
    pins that every case produced *some* HTTP response, so a silent connection
    failure cannot masquerade as a clean run.
    """
    statuses = _statuses(nginx_logs)
    assert set(statuses) == set(_SECRET_CASES)
    for case_id, status in statuses.items():
        assert status.isdigit(), f"case_id={case_id!r} produced no HTTP status ({status})"


# ===========================================================================
# The mandatory gate itself
# ===========================================================================


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

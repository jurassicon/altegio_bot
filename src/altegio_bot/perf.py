from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Generator

logger = logging.getLogger("altegio_bot.perf")


def _perf_enabled() -> bool:
    return os.environ.get("PERF_LOGGING_ENABLED", "").lower() == "true"


@contextmanager
def perf_log(component: str, operation: str, **extra: Any) -> Generator[dict[str, Any], None, None]:
    """Context manager that measures duration and writes a structured JSON perf log.

    Enabled only when ``PERF_LOGGING_ENABLED=true`` in the environment.
    Exceptions propagate normally; the log is written with ``status="error"``.

    Yields a mutable dict that callers can populate with additional fields after
    the context is entered (e.g. after an initial DB load):

        with perf_log("worker", "op", job_id=1) as ctx:
            job = await _load_job(session, job_id)
            ctx.update(job_type=job.job_type, company_id=job.company_id)
    """
    deferred: dict[str, Any] = {}
    if not _perf_enabled():
        yield deferred
        return

    start = time.perf_counter()
    status = "ok"
    try:
        yield deferred
    except Exception:
        status = "error"
        raise
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        record: dict[str, Any] = {
            "kind": "perf",
            "component": component,
            "operation": operation,
            "duration_ms": round(duration_ms, 2),
            "status": status,
            **extra,
            **deferred,
        }
        logger.info(json.dumps(record, ensure_ascii=False))

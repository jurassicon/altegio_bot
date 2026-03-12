"""Ops-cabinet router – read-only HTML dashboard.

All routes on `router` are protected by require_ops_auth.
`login_router` is public (login / logout pages).
"""

from __future__ import annotations

import json
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import text

from altegio_bot.db import SessionLocal
from altegio_bot.settings import settings

from .auth import (
    SESSION_COOKIE,
    SESSION_MAX_AGE,
    check_session_token,
    make_session_token,
    require_ops_auth,
)

router = APIRouter(prefix="/ops", dependencies=[Depends(require_ops_auth)])

# Public router – no auth dependency (login / logout)
login_router = APIRouter(prefix="/ops")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COMPANIES = {758285: "Karlsruhe", 1271200: "Rastatt"}

MARKETING_JOB_TYPES = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "newsletter_new_clients_monthly",
)


def _local_tz() -> ZoneInfo:
    try:
        return ZoneInfo(settings.ops_local_tz)
    except Exception:
        return ZoneInfo("Europe/Berlin")


def _fmt_dt(dt: datetime | None, tz: ZoneInfo | None = None) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    if tz:
        local = dt.astimezone(tz)
        return f"{utc_str} / {local.strftime('%Y-%m-%d %H:%M')} loc"
    return utc_str


def _ago(dt: datetime | None) -> str:
    """Human readable 'X minutes ago'."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    secs = int(delta.total_seconds())
    if secs < 0:
        return "in the future"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _esc(s: Any) -> str:
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _status_badge(status: str | None) -> str:
    if not status:
        return ""
    colors = {
        "queued": "secondary",
        "processing": "primary",
        "running": "primary",
        "done": "success",
        "sent": "success",
        "delivered": "success",
        "read": "info",
        "failed": "danger",
        "canceled": "warning",
        "received": "secondary",
        "ignored": "warning",
        "processed": "success",
    }
    color = colors.get(status, "secondary")
    return f'<span class="badge bg-{color}">{_esc(status)}</span>'


_NAV = """
<nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
  <div class="container-fluid">
    <a class="navbar-brand fw-bold" href="/ops/monitoring">🤖 Ops</a>
    <button class="navbar-toggler" type="button"
            data-bs-toggle="collapse" data-bs-target="#navmenu">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navmenu">
      <ul class="navbar-nav me-auto">
        <li class="nav-item">
          <a class="nav-link" href="/ops/monitoring">📊 Monitoring</a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="/ops/queue">📋 Queue</a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="/ops/history">📨 History</a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="/ops/whatsapp/inbox">💬 WA Events</a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="/ops/optouts">🚫 Opt-outs</a>
        </li>
      </ul>
      <form method="post" action="/ops/logout" class="d-flex">
        <button type="submit" class="btn btn-outline-light btn-sm">Logout</button>
      </form>
    </div>
  </div>
</nav>
"""


def _page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_esc(title)} – Ops</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
    crossorigin="anonymous">
  <style>
    pre {{ white-space: pre-wrap; word-break: break-all; font-size: .8rem; }}
    .table-sm td, .table-sm th {{ font-size: .85rem; }}
    .warn {{ background-color: #fff3cd !important; }}
    .stuck {{ background-color: #f8d7da !important; }}
  </style>
</head>
<body class="bg-light">
{_NAV}
<div class="container-fluid px-4">
{body}
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"
  integrity="sha384-YvpcrYf0tY3lHB60NNkmXc4s9bIOgUxi8T/jzmYLqgTB2YNVN4VcNXyG3KvFnuD"
  crossorigin="anonymous"></script>
</body>
</html>"""


def _filter_form(
    action: str,
    fields: list[tuple[str, str, str, str]],
    hidden: dict[str, str] | None = None,
) -> str:
    """
    fields: list of (name, label, type, current_value)
    type: text | select:opt1,opt2,...
    hidden: optional dict of name -> value for hidden inputs (e.g. tab preservation)
    """
    parts = [f'<form method="get" action="{action}" class="row g-2 mb-3 align-items-end">']
    for name, val in (hidden or {}).items():
        parts.append(f'<input type="hidden" name="{_esc(name)}" value="{_esc(val)}">')
    for name, label, ftype, val in fields:
        parts.append('<div class="col-auto">')
        parts.append(f'<label class="form-label small mb-1">{_esc(label)}</label>')
        if ftype.startswith("select:"):
            options_str = ftype[7:]
            options_list = options_str.split(",")
            options = options_list if (options_list and options_list[0] == "") else [""] + options_list
            sel = f'<select name="{name}" class="form-select form-select-sm">'
            for o in options:
                selected = " selected" if o == val else ""
                sel += f'<option value="{_esc(o)}"{selected}>{_esc(o) or "—all—"}</option>'
            sel += "</select>"
            parts.append(sel)
        else:
            parts.append(
                f'<input type="text" name="{name}" class="form-control form-control-sm"'
                f' value="{_esc(val)}" placeholder="{_esc(label)}">'
            )
        parts.append("</div>")
    parts.append('<div class="col-auto"><button type="submit" class="btn btn-primary btn-sm">Filter</button></div>')
    parts.append("</form>")
    return "\n".join(parts)


def _table(cols: list[str], rows: list[list[str]], row_classes: list[str] | None = None) -> str:
    th = "".join(f"<th>{_esc(c)}</th>" for c in cols)
    body_rows = []
    for i, row in enumerate(rows):
        cls = ""
        if row_classes and i < len(row_classes):
            cls = f' class="{row_classes[i]}"'
        cells = "".join(f"<td>{c}</td>" for c in row)
        body_rows.append(f"<tr{cls}>{cells}</tr>")
    body = "\n".join(body_rows)
    return f"""<div class="table-responsive">
<table class="table table-sm table-hover table-bordered align-middle">
  <thead class="table-dark"><tr>{th}</tr></thead>
  <tbody>{body}</tbody>
</table>
</div>"""


def _metric_cards(metrics: list[tuple[str, Any, str]]) -> str:
    """metrics: list of (label, value, color) where color is bootstrap color."""
    cards = []
    for label, value, color in metrics:
        cards.append(
            f'<div class="col-auto">'
            f'<div class="card border-{color} text-center" style="min-width:120px">'
            f'<div class="card-body p-2">'
            f'<div class="fs-4 fw-bold text-{color}">{_esc(str(value))}</div>'
            f'<div class="small text-muted">{_esc(label)}</div>'
            f"</div></div></div>"
        )
    return '<div class="row g-2 mb-3">' + "".join(cards) + "</div>"


_SAFE_IDENTIFIER_RE = __import__("re").compile(r"^[a-zA-Z0-9_]{1,64}$")


def _safe_identifier(value: str) -> bool:
    """Return True if value is safe to use as a SQL identifier/enum value."""
    return bool(_SAFE_IDENTIFIER_RE.match(value))


def _period_params(request: Request) -> tuple[datetime, datetime]:
    """Return (from_dt, to_dt) UTC based on ?period= / ?from_dt= / ?to_dt=."""
    now = datetime.now(timezone.utc)
    period = request.query_params.get("period", "24h")
    from_str = request.query_params.get("from_dt", "")
    to_str = request.query_params.get("to_dt", "")

    if from_str and to_str:
        try:
            from_dt = datetime.fromisoformat(from_str).replace(tzinfo=timezone.utc)
            to_dt = datetime.fromisoformat(to_str).replace(tzinfo=timezone.utc)
            return from_dt, to_dt
        except ValueError:
            pass

    if period == "today":
        from_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return from_dt, now
    if period == "yesterday":
        yesterday = now - timedelta(days=1)
        from_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = from_dt + timedelta(days=1)
        return from_dt, to_dt
    if period == "last_7d":
        return now - timedelta(days=7), now
    if period == "last_30d":
        return now - timedelta(days=30), now
    if period == "this_week":
        days_since_monday = now.weekday()
        monday = now - timedelta(days=days_since_monday)
        from_dt = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        return from_dt, now
    if period == "last_week":
        days_since_monday = now.weekday()
        this_monday = now - timedelta(days=days_since_monday)
        last_monday = this_monday - timedelta(days=7)
        from_dt = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = this_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        return from_dt, to_dt
    if period == "this_month":
        from_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return from_dt, now
    if period == "last_month":
        first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        from_dt = last_day_prev_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        to_dt = first_of_this_month
        return from_dt, to_dt
    if period == "7d":
        return now - timedelta(days=7), now
    # default: 24h
    return now - timedelta(hours=24), now


# ---------------------------------------------------------------------------
# /ops/queue
# ---------------------------------------------------------------------------


@router.get("/queue", response_class=HTMLResponse)
async def ops_queue(request: Request) -> str:
    company_id = request.query_params.get("company_id", "")
    job_type = request.query_params.get("job_type", "")
    status_filter = request.query_params.get("status", "")
    # view: upcoming_7d (default), upcoming_24h, recent_24h, recent_7d
    view = request.query_params.get("view", "")
    if not view:
        view = "recent_24h" if status_filter in ("processing", "failed") else "upcoming_7d"
    tz = _local_tz()
    now = datetime.now(timezone.utc)

    # Compute time bounds based on view
    is_upcoming = view.startswith("upcoming")
    window_hours = 24 if "24h" in view else 7 * 24
    if is_upcoming:
        from_dt = now
        to_dt = now + timedelta(hours=window_hours)
    else:
        from_dt = now - timedelta(hours=window_hours)
        to_dt = now

    async with SessionLocal() as session:
        # Metrics
        metrics_q = await session.execute(
            text("""
                 SELECT COUNT(*) FILTER (WHERE status = 'queued')     AS queued_cnt,
                        COUNT(*) FILTER (WHERE status = 'processing') AS proc_cnt,
                        COUNT(*) FILTER (WHERE status = 'failed')     AS failed_cnt,
                        COUNT(*) FILTER (
                            WHERE status = 'processing'
                                AND locked_at <
                                    now() - (:stuck_min * interval '1 minute')
                            )                                         AS stuck_cnt
                 FROM message_jobs
                 WHERE status IN ('queued', 'processing', 'failed')
                 """),
            {"stuck_min": settings.ops_stuck_minutes},
        )
        m = metrics_q.fetchone()
        queued_cnt = m.queued_cnt or 0
        proc_cnt = m.proc_cnt or 0
        failed_cnt = m.failed_cnt or 0
        stuck_cnt = m.stuck_cnt or 0

        # NOTE: `filters` contains only hardcoded SQL clauses with named
        # placeholders.  All user input is passed via `params` as bound
        # parameters to text(), so there is no SQL injection risk.
        if is_upcoming:
            # Show future queued jobs by default; apply run_at window
            filters = [
                "mj.run_at >= :from_dt AND mj.run_at < :to_dt",
            ]
            if status_filter and _safe_identifier(status_filter):
                filters.append("mj.status = :status_filter")
            else:
                filters.append("mj.status = 'queued'")
        else:
            # Show recently updated jobs (processing/failed) via updated_at
            filters = [
                "mj.status IN (:s_queued, :s_proc, :s_failed)",
                "mj.updated_at >= :from_dt AND mj.updated_at < :to_dt",
            ]
            if status_filter and _safe_identifier(status_filter):
                filters.append("mj.status = :status_filter")

        params: dict[str, Any] = {
            "s_queued": "queued",
            "s_proc": "processing",
            "s_failed": "failed",
            "from_dt": from_dt,
            "to_dt": to_dt,
            "stuck_min": settings.ops_stuck_minutes,
        }
        if status_filter and _safe_identifier(status_filter):
            params["status_filter"] = status_filter

        if company_id:
            filters.append("mj.company_id = :company_id")
            params["company_id"] = int(company_id)
        if job_type and _safe_identifier(job_type):
            filters.append("mj.job_type = :job_type")
            params["job_type"] = job_type

        where = " AND ".join(filters)
        order = "mj.run_at ASC" if is_upcoming else "mj.updated_at DESC NULLS LAST"
        rows_q = await session.execute(
            text(f"""
                SELECT
                  mj.id, mj.company_id, mj.job_type, mj.status,
                  mj.run_at, mj.updated_at, mj.attempts, mj.max_attempts,
                  mj.locked_at, mj.last_error,
                  c.display_name, c.phone_e164
                FROM message_jobs mj
                LEFT JOIN clients c ON c.id = mj.client_id
                WHERE {where}
                ORDER BY {order}
                LIMIT 200
            """),
            params,
        )
        jobs = rows_q.fetchall()

    metrics_html = _metric_cards(
        [
            ("Queued", queued_cnt, "secondary"),
            ("Processing", proc_cnt, "primary"),
            ("Failed", failed_cnt, "danger"),
            ("Stuck", stuck_cnt, "warning" if stuck_cnt == 0 else "danger"),
        ]
    )

    form = _filter_form(
        "/ops/queue",
        [
            ("company_id", "Company", "select:758285,1271200", company_id),
            ("job_type", "Job type", "text", job_type),
            ("status", "Status", "select:queued,processing,failed", status_filter),
            ("view", "View", "select:upcoming_7d,upcoming_24h,recent_24h,recent_7d", view),
        ],
    )

    view_label = view.replace("_", " ")
    cols = ["ID", "Company", "Type", "Status", "Run At", "Attempts", "Client", "Error"]
    table_rows = []
    row_classes = []
    for j in jobs:
        is_stuck = (
            j.status == "processing"
            and j.locked_at is not None
            and (datetime.now(timezone.utc) - j.locked_at.replace(tzinfo=timezone.utc)).total_seconds()
            > settings.ops_stuck_minutes * 60
        )
        company_name = COMPANIES.get(j.company_id, str(j.company_id))
        client_info = ""
        if j.display_name or j.phone_e164:
            client_info = f"{_esc(j.display_name or '')} {_esc(j.phone_e164 or '')}".strip()
        table_rows.append(
            [
                f'<a href="/ops/job/{j.id}">{j.id}</a>',
                _esc(company_name),
                _esc(j.job_type),
                _status_badge(j.status),
                _esc(_fmt_dt(j.run_at, tz)),
                f"{j.attempts}/{j.max_attempts}",
                client_info,
                f'<span class="text-danger small">{_esc((j.last_error or "")[:80])}</span>',
            ]
        )
        row_classes.append("stuck" if is_stuck else "")

    body = f"""
<h4>📋 Queue</h4>
{metrics_html}
{form}
<p class="text-muted small">Showing {len(jobs)} rows · view: {_esc(view_label)}</p>
{_table(cols, table_rows, row_classes)}
"""
    return _page("Queue", body)


# ---------------------------------------------------------------------------
# /ops/history
# ---------------------------------------------------------------------------


@router.get("/history", response_class=HTMLResponse)
async def ops_history(request: Request) -> str:
    company_id = request.query_params.get("company_id", "")
    template_code = request.query_params.get("template_code", "")
    phone = request.query_params.get("phone_e164", "")
    provider_msg_id = request.query_params.get("provider_message_id", "")
    period = request.query_params.get("period", "today")
    from_dt, to_dt = _period_params(request)
    tz = _local_tz()

    async with SessionLocal() as session:
        # NOTE: `filters` contains only hardcoded SQL clauses; user input
        # is exclusively passed as bound parameters via `params`.
        filters = [
            "COALESCE(om.sent_at, om.scheduled_at) >= :from_dt AND COALESCE(om.sent_at, om.scheduled_at) < :to_dt",
        ]
        params: dict[str, Any] = {
            "from_dt": from_dt,
            "to_dt": to_dt,
        }

        if company_id:
            filters.append("om.company_id = :company_id")
            params["company_id"] = int(company_id)
        if template_code and _safe_identifier(template_code):
            filters.append("om.template_code = :template_code")
            params["template_code"] = template_code
        if phone:
            phone_digits = re.sub(r"\D", "", phone.strip())
            if phone_digits:
                filters.append(
                    "(replace(om.phone_e164, '+', '') ILIKE :phone_pattern "
                    "OR :phone_digits ILIKE '%' || replace(om.phone_e164, '+', '') || '%')"
                )
                params["phone_pattern"] = f"%{phone_digits}%"
                params["phone_digits"] = phone_digits
        if provider_msg_id:
            filters.append("om.provider_message_id ILIKE :pmid")
            params["pmid"] = f"%{provider_msg_id}%"

        where = " AND ".join(filters)

        rows_q = await session.execute(
            text(f"""
                SELECT
                  om.id,
                  COALESCE(om.sent_at, om.scheduled_at) AS ts,
                  om.company_id,
                  om.phone_e164,
                  om.template_code,
                  om.meta ->> 'send_type' AS send_type,
                  om.meta ->> 'template'  AS meta_template,
                  om.status,
                  om.provider_message_id,
                  om.error,
                  -- last delivery status from whatsapp_events
                  ws.wa_status,
                  ws.wa_err_code
                FROM outbox_messages om
                LEFT JOIN LATERAL (
                  SELECT
                    payload #>> '{{entry,0,changes,0,value,statuses,0,status}}' AS wa_status,
                    payload #>> '{{entry,0,changes,0,value,statuses,0,errors,0,code}}' AS wa_err_code
                  FROM whatsapp_events we
                  WHERE
                    payload #>> '{{entry,0,changes,0,value,statuses,0,id}}'
                      = om.provider_message_id
                    AND om.provider_message_id IS NOT NULL
                  ORDER BY we.received_at DESC
                  LIMIT 1
                ) ws ON true
                WHERE {where}
                ORDER BY COALESCE(om.sent_at, om.scheduled_at) DESC
                LIMIT 200
            """),
            params,
        )
        rows = rows_q.fetchall()

    form = _filter_form(
        "/ops/history",
        [
            ("company_id", "Company", "select:758285,1271200", company_id),
            ("template_code", "Template", "text", template_code),
            ("phone_e164", "Phone", "text", phone),
            ("provider_message_id", "Provider Msg ID", "text", provider_msg_id),
            (
                "period",
                "Period",
                "select:today,yesterday,last_7d,last_30d,this_week,last_week,this_month,last_month",
                period,
            ),
        ],
    )

    cols = [
        "ID",
        "Sent At",
        "Company",
        "Phone",
        "Template",
        "Send",
        "Status",
        "WA Delivery",
        "Provider Msg ID",
        "Error",
    ]
    table_rows = []
    row_classes = []
    for r in rows:
        company_name = COMPANIES.get(r.company_id, str(r.company_id))
        wa_delivery = _status_badge(r.wa_status) if r.wa_status else ""
        if r.wa_err_code:
            wa_delivery += f' <span class="text-danger small">{_esc(r.wa_err_code)}</span>'
        row_class = "warn" if r.status == "failed" else ""
        # Show send type badge
        send_type = r.send_type or ""
        send_badge = (
            '<span class="badge bg-primary">tpl</span>'
            if send_type == "template"
            else ('<span class="badge bg-secondary">txt</span>' if send_type == "text" else "")
        )
        # Template column: show job type + meta template name if available
        tpl_cell = _esc(r.template_code or "")
        if r.meta_template:
            tpl_cell += f'<br><span class="text-muted small">{_esc(r.meta_template)}</span>'
        table_rows.append(
            [
                f'<a href="/ops/outbox/{r.id}">{r.id}</a>',
                _esc(_fmt_dt(r.ts, tz)),
                _esc(company_name),
                _esc(r.phone_e164),
                tpl_cell,
                send_badge,
                _status_badge(r.status),
                wa_delivery,
                _esc((r.provider_message_id or "")[:40]),
                f'<span class="text-danger small">{_esc((r.error or "")[:80])}</span>',
            ]
        )
        row_classes.append(row_class)

    body = f"""
<h4>📨 Outbox History</h4>
{form}
<p class="text-muted small">Showing {len(rows)} rows · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""
    return _page("History", body)


# ---------------------------------------------------------------------------
# /ops/job/{job_id}
# ---------------------------------------------------------------------------


@router.get("/job/{job_id}", response_class=HTMLResponse)
async def ops_job(job_id: int) -> str:
    tz = _local_tz()

    async with SessionLocal() as session:
        job_q = await session.execute(
            text("""
                 SELECT mj.*,
                        c.display_name,
                        c.phone_e164
                 FROM message_jobs mj
                          LEFT JOIN clients c ON c.id = mj.client_id
                 WHERE mj.id = :job_id
                 """),
            {"job_id": job_id},
        )
        job = job_q.fetchone()

        if job is None:
            return _page("Job Not Found", '<div class="alert alert-danger">Job not found.</div>')

        outbox_q = await session.execute(
            text("""
                 SELECT om.id,
                        om.status,
                        om.phone_e164,
                        om.template_code,
                        COALESCE(om.sent_at, om.scheduled_at) AS ts,
                        om.provider_message_id,
                        om.error,
                        ws.wa_status
                 FROM outbox_messages om
                          LEFT JOIN LATERAL (
                     SELECT payload #>>
                            '{entry,0,changes,0,value,statuses,0,status}' AS wa_status
                     FROM whatsapp_events we
                     WHERE
                         payload #>> '{entry,0,changes,0,value,statuses,0,id}'
                             = om.provider_message_id
                       AND om.provider_message_id IS NOT NULL
                     ORDER BY we.received_at DESC
                     LIMIT 1
                     ) ws ON true
                 WHERE om.job_id = :job_id
                 ORDER BY om.id DESC
                 LIMIT 20
                 """),
            {"job_id": job_id},
        )
        outbox_rows = outbox_q.fetchall()

        # --- record history ---
        record_jobs: list = []
        altegio_events: list = []
        altegio_record_id: int | None = None
        if job.record_id:
            rec_q = await session.execute(
                text("SELECT altegio_record_id FROM records WHERE id = :rid"),
                {"rid": job.record_id},
            )
            rec = rec_q.fetchone()
            if rec:
                altegio_record_id = rec.altegio_record_id

            record_jobs_q = await session.execute(
                text("""
                     SELECT mj.id,
                            mj.job_type,
                            mj.status,
                            mj.run_at,
                            mj.created_at,
                            mj.attempts,
                            mj.max_attempts,
                            mj.last_error,
                            c.display_name,
                            c.phone_e164
                     FROM message_jobs mj
                              LEFT JOIN clients c ON c.id = mj.client_id
                     WHERE mj.record_id = :rid
                     ORDER BY mj.created_at ASC
                     """),
                {"rid": job.record_id},
            )
            record_jobs = record_jobs_q.fetchall()

            if altegio_record_id:
                events_q = await session.execute(
                    text("""
                         SELECT id, received_at, event_status, status, error
                         FROM altegio_events
                         WHERE resource = 'record'
                           AND resource_id = :rid
                         ORDER BY received_at ASC
                         """),
                    {"rid": altegio_record_id},
                )
                altegio_events = events_q.fetchall()

    is_stuck = (
        job.status == "processing"
        and job.locked_at is not None
        and (datetime.now(timezone.utc) - job.locked_at.replace(tzinfo=timezone.utc)).total_seconds()
        > settings.ops_stuck_minutes * 60
    )
    company_name = COMPANIES.get(job.company_id, str(job.company_id))

    payload_json = ""
    try:
        payload_json = json.dumps(job.payload, indent=2, ensure_ascii=False)
    except Exception:
        payload_json = str(job.payload)

    stuck_badge = ' <span class="badge bg-danger">STUCK</span>' if is_stuck else ""

    details = f"""
<div class="card mb-3">
  <div class="card-header">
    Job #{job_id} {_status_badge(job.status)}{stuck_badge}
  </div>
  <div class="card-body">
    <dl class="row mb-0">
      <dt class="col-sm-3">Company</dt>
      <dd class="col-sm-9">{_esc(company_name)}</dd>
      <dt class="col-sm-3">Job Type</dt>
      <dd class="col-sm-9">{_esc(job.job_type)}</dd>
      <dt class="col-sm-3">Status</dt>
      <dd class="col-sm-9">{_status_badge(job.status)}</dd>
      <dt class="col-sm-3">Run At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(job.run_at, tz))}</dd>
      <dt class="col-sm-3">Attempts</dt>
      <dd class="col-sm-9">{job.attempts} / {job.max_attempts}</dd>
      <dt class="col-sm-3">Locked At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(job.locked_at, tz))}</dd>
      <dt class="col-sm-3">Created At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(job.created_at, tz))}</dd>
      <dt class="col-sm-3">Updated At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(job.updated_at, tz))}</dd>
      <dt class="col-sm-3">Client</dt>
      <dd class="col-sm-9">
        {_esc(job.display_name or "")}
        {
        (f'<a href="/ops/history?phone_e164={_esc(job.phone_e164)}">{_esc(job.phone_e164)}</a>')
        if job.phone_e164
        else ""
    }
      </dd>
      <dt class="col-sm-3">Record ID</dt>
      <dd class="col-sm-9">
        {f'<a href="/ops/record/{job.record_id}">{job.record_id}</a>' if job.record_id else ""}
        {(f'<span class="text-muted small">(altegio: {altegio_record_id})</span>') if altegio_record_id else ""}
      </dd>
      <dt class="col-sm-3">Last Error</dt>
      <dd class="col-sm-9 text-danger">{_esc(job.last_error or "")}</dd>
    </dl>
  </div>
</div>
<div class="card mb-3">
  <div class="card-header">Payload</div>
  <div class="card-body">
    <pre>{_esc(payload_json)}</pre>
  </div>
</div>
"""

    cols = ["ID", "Status", "WA Delivery", "Phone", "Template", "Sent At", "Error"]
    outbox_table_rows = []
    for r in outbox_rows:
        outbox_table_rows.append(
            [
                f'<a href="/ops/outbox/{r.id}">{r.id}</a>',
                _status_badge(r.status),
                _status_badge(r.wa_status) if r.wa_status else "",
                _esc(r.phone_e164),
                _esc(r.template_code),
                _esc(_fmt_dt(r.ts, tz)),
                f'<span class="text-danger small">{_esc((r.error or "")[:80])}</span>',
            ]
        )

    outbox_section = f"""
<h5>Related Outbox Messages (last 20)</h5>
{_table(cols, outbox_table_rows) if outbox_table_rows else '<p class="text-muted">No outbox messages.</p>'}
"""

    # --- Record Altegio events section ---
    altegio_events_section = ""
    if altegio_events:
        ev_cols = ["Event ID", "Received At", "Event Status", "Processing Status", "Error"]
        ev_rows = []
        for e in altegio_events:
            ev_rows.append(
                [
                    str(e.id),
                    _esc(_fmt_dt(e.received_at, tz)),
                    _status_badge(e.event_status) if e.event_status else "",
                    _status_badge(e.status),
                    f'<span class="text-danger small">{_esc((e.error or "")[:80])}</span>',
                ]
            )
        altegio_events_section = f"""
<h5>Altegio Events for this Record (altegio_record_id: {_esc(str(altegio_record_id))})</h5>
{_table(ev_cols, ev_rows)}
"""

    # --- All jobs for same record section ---
    record_jobs_section = ""
    if record_jobs:
        rj_cols = ["ID", "Type", "Status", "Run At", "Created At", "Attempts", "Error"]
        rj_rows = []
        rj_classes = []
        for rj in record_jobs:
            row_id = (
                f'<a href="/ops/job/{rj.id}"><strong>{rj.id}</strong></a>'
                if rj.id == job_id
                else f'<a href="/ops/job/{rj.id}">{rj.id}</a>'
            )
            rj_rows.append(
                [
                    row_id,
                    _esc(rj.job_type),
                    _status_badge(rj.status),
                    _esc(_fmt_dt(rj.run_at, tz)),
                    _esc(_fmt_dt(rj.created_at, tz)),
                    f"{rj.attempts}/{rj.max_attempts}",
                    f'<span class="text-danger small">{_esc((rj.last_error or "")[:80])}</span>',
                ]
            )
            rj_classes.append("table-active" if rj.id == job_id else "")
        record_jobs_section = f"""
<h5>All Jobs for this Record</h5>
{_table(rj_cols, rj_rows, rj_classes)}
"""

    body = f"""
<h4>📋 Job #{job_id}</h4>
{details}
{altegio_events_section}
{record_jobs_section}
{outbox_section}
"""
    return _page(f"Job {job_id}", body)


# ---------------------------------------------------------------------------
# /ops/record/{record_id}  – full timeline for a booking record
# ---------------------------------------------------------------------------


@router.get("/record/{record_id}", response_class=HTMLResponse)
async def ops_record(record_id: int) -> str:
    tz = _local_tz()

    async with SessionLocal() as session:
        rec_q = await session.execute(
            text("""
                 SELECT r.*, c.display_name, c.phone_e164
                 FROM records r
                          LEFT JOIN clients c ON c.id = r.client_id
                 WHERE r.id = :rid
                 """),
            {"rid": record_id},
        )
        rec = rec_q.fetchone()

        if rec is None:
            return _page("Record Not Found", '<div class="alert alert-danger">Record not found.</div>')

        events_q = await session.execute(
            text("""
                 SELECT id, received_at, event_status, status, error
                 FROM altegio_events
                 WHERE resource = 'record'
                   AND resource_id = :altegio_rid
                 ORDER BY received_at ASC
                 """),
            {"altegio_rid": rec.altegio_record_id},
        )
        altegio_events = events_q.fetchall()

        jobs_q = await session.execute(
            text("""
                 SELECT mj.id,
                        mj.job_type,
                        mj.status,
                        mj.run_at,
                        mj.created_at,
                        mj.attempts,
                        mj.max_attempts,
                        mj.last_error
                 FROM message_jobs mj
                 WHERE mj.record_id = :rid
                 ORDER BY mj.created_at ASC
                 """),
            {"rid": record_id},
        )
        jobs = jobs_q.fetchall()

        outbox_q = await session.execute(
            text("""
                 SELECT om.id,
                        om.status,
                        om.phone_e164,
                        om.template_code,
                        COALESCE(om.sent_at, om.scheduled_at) AS ts,
                        om.provider_message_id,
                        om.error,
                        ws.wa_status
                 FROM outbox_messages om
                          LEFT JOIN LATERAL (
                     SELECT payload #>>
                            '{entry,0,changes,0,value,statuses,0,status}' AS wa_status
                     FROM whatsapp_events we
                     WHERE
                         payload #>> '{entry,0,changes,0,value,statuses,0,id}'
                             = om.provider_message_id
                       AND om.provider_message_id IS NOT NULL
                     ORDER BY we.received_at DESC
                     LIMIT 1
                     ) ws ON true
                 WHERE om.record_id = :rid
                 ORDER BY om.id DESC
                 LIMIT 50
                 """),
            {"rid": record_id},
        )
        outbox_rows = outbox_q.fetchall()

    company_name = COMPANIES.get(rec.company_id, str(rec.company_id))
    phone_link = (
        f'<a href="/ops/history?phone_e164={_esc(rec.phone_e164)}">{_esc(rec.phone_e164)}</a>' if rec.phone_e164 else ""
    )

    details = f"""
<div class="card mb-3">
  <div class="card-header">Record #{record_id}</div>
  <div class="card-body">
    <dl class="row mb-0">
      <dt class="col-sm-3">Company</dt>
      <dd class="col-sm-9">{_esc(company_name)}</dd>
      <dt class="col-sm-3">Altegio Record ID</dt>
      <dd class="col-sm-9">{_esc(str(rec.altegio_record_id))}</dd>
      <dt class="col-sm-3">Client</dt>
      <dd class="col-sm-9">{_esc(rec.display_name or "")} {phone_link}</dd>
      <dt class="col-sm-3">Starts At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(rec.starts_at, tz))}</dd>
      <dt class="col-sm-3">Ends At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(rec.ends_at, tz))}</dd>
      <dt class="col-sm-3">Deleted</dt>
      <dd class="col-sm-9">{"Yes" if rec.is_deleted else "No"}</dd>
      <dt class="col-sm-3">Staff</dt>
      <dd class="col-sm-9">{_esc(rec.staff_name or "")}</dd>
    </dl>
  </div>
</div>
"""

    ev_cols = ["Event ID", "Received At", "Event Status", "Processing Status", "Error"]
    ev_rows = []
    for e in altegio_events:
        ev_rows.append(
            [
                str(e.id),
                _esc(_fmt_dt(e.received_at, tz)),
                _status_badge(e.event_status) if e.event_status else "",
                _status_badge(e.status),
                f'<span class="text-danger small">{_esc((e.error or "")[:80])}</span>',
            ]
        )

    events_section = f"""
<h5>Altegio Events</h5>
{_table(ev_cols, ev_rows) if ev_rows else '<p class="text-muted">No altegio events found.</p>'}
"""

    j_cols = ["ID", "Type", "Status", "Run At", "Created At", "Attempts", "Error"]
    j_rows = []
    for jb in jobs:
        j_rows.append(
            [
                f'<a href="/ops/job/{jb.id}">{jb.id}</a>',
                _esc(jb.job_type),
                _status_badge(jb.status),
                _esc(_fmt_dt(jb.run_at, tz)),
                _esc(_fmt_dt(jb.created_at, tz)),
                f"{jb.attempts}/{jb.max_attempts}",
                f'<span class="text-danger small">{_esc((jb.last_error or "")[:80])}</span>',
            ]
        )

    jobs_section = f"""
<h5>Message Jobs</h5>
{_table(j_cols, j_rows) if j_rows else '<p class="text-muted">No jobs found.</p>'}
"""

    om_cols = ["ID", "Status", "WA Delivery", "Phone", "Template", "Sent At", "Error"]
    om_rows = []
    for r in outbox_rows:
        om_rows.append(
            [
                f'<a href="/ops/outbox/{r.id}">{r.id}</a>',
                _status_badge(r.status),
                _status_badge(r.wa_status) if r.wa_status else "",
                _esc(r.phone_e164),
                _esc(r.template_code),
                _esc(_fmt_dt(r.ts, tz)),
                f'<span class="text-danger small">{_esc((r.error or "")[:80])}</span>',
            ]
        )

    outbox_section = f"""
<h5>Outbox Messages</h5>
{_table(om_cols, om_rows) if om_rows else '<p class="text-muted">No outbox messages.</p>'}
"""

    body = f"""
<h4>📅 Record #{record_id}</h4>
{details}
{events_section}
{jobs_section}
{outbox_section}
"""
    return _page(f"Record {record_id}", body)


# ---------------------------------------------------------------------------
# /ops/outbox/{outbox_id}
# ---------------------------------------------------------------------------


@router.get("/outbox/{outbox_id}", response_class=HTMLResponse)
async def ops_outbox(outbox_id: int) -> str:
    tz = _local_tz()

    async with SessionLocal() as session:
        om_q = await session.execute(
            text("""
                 SELECT om.*, ws.display_phone AS sender_phone
                 FROM outbox_messages om
                          LEFT JOIN whatsapp_senders ws ON ws.id = om.sender_id
                 WHERE om.id = :oid
                 """),
            {"oid": outbox_id},
        )
        om = om_q.fetchone()

        if om is None:
            return _page("Not Found", '<div class="alert alert-danger">Not found.</div>')

        delivery_q = await session.execute(
            text("""
                 SELECT we.id,
                        we.received_at,
                        payload #>>
                        '{entry,0,changes,0,value,statuses,0,status}'                      AS wa_status,
                        payload #>>
                        '{entry,0,changes,0,value,statuses,0,errors,0,code}'               AS err_code,
                        payload #>>
                        '{entry,0,changes,0,value,statuses,0,errors,0,error_data,details}' AS err_details
                 FROM whatsapp_events we
                 WHERE payload #>> '{entry,0,changes,0,value,statuses,0,id}'
                     = :msg_id
                   AND :msg_id IS NOT NULL
                 ORDER BY we.received_at DESC
                 LIMIT 20
                 """),
            {"msg_id": om.provider_message_id},
        )
        deliveries = delivery_q.fetchall()

    company_name = COMPANIES.get(om.company_id, str(om.company_id))

    body_text = ""
    try:
        body_text = om.body[:500] if om.body else ""
    except Exception:
        pass

    meta_json = ""
    meta_dict: dict = {}
    try:
        meta_dict = dict(om.meta) if om.meta else {}
        meta_json = json.dumps(meta_dict, indent=2, ensure_ascii=False)
    except Exception:
        meta_json = str(om.meta)

    send_type = meta_dict.get("send_type", "")
    meta_template = meta_dict.get("template", "")

    details = f"""
<div class="card mb-3">
  <div class="card-header">
    Outbox #{outbox_id} {_status_badge(om.status)}
  </div>
  <div class="card-body">
    <dl class="row mb-0">
      <dt class="col-sm-3">Company</dt>
      <dd class="col-sm-9">{_esc(company_name)}</dd>
      <dt class="col-sm-3">Phone</dt>
      <dd class="col-sm-9">{_esc(om.phone_e164)}</dd>
      <dt class="col-sm-3">Send Type</dt>
      <dd class="col-sm-9">{_esc(send_type) or '<span class="text-muted">—</span>'}</dd>
      <dt class="col-sm-3">Job Template</dt>
      <dd class="col-sm-9">{_esc(om.template_code)}</dd>
      <dt class="col-sm-3">Meta Template</dt>
      <dd class="col-sm-9">{_esc(meta_template) or '<span class="text-muted">—</span>'}</dd>
      <dt class="col-sm-3">Language</dt>
      <dd class="col-sm-9">{_esc(om.language)}</dd>
      <dt class="col-sm-3">Sender</dt>
      <dd class="col-sm-9">{_esc(om.sender_phone or str(om.sender_id or ""))}</dd>
      <dt class="col-sm-3">Status</dt>
      <dd class="col-sm-9">{_status_badge(om.status)}</dd>
      <dt class="col-sm-3">Provider Msg ID</dt>
      <dd class="col-sm-9"><code>{_esc(om.provider_message_id or "")}</code></dd>
      <dt class="col-sm-3">Scheduled At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(om.scheduled_at, tz))}</dd>
      <dt class="col-sm-3">Sent At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(om.sent_at, tz))}</dd>
      <dt class="col-sm-3">Error</dt>
      <dd class="col-sm-9 text-danger">{_esc(om.error or "")}</dd>
      <dt class="col-sm-3">Job ID</dt>
      <dd class="col-sm-9">
        {f'<a href="/ops/job/{om.job_id}">{om.job_id}</a>' if om.job_id else ""}
      </dd>
    </dl>
  </div>
</div>
"""

    if body_text:
        details += f"""
<div class="card mb-3">
  <div class="card-header">Message Body</div>
  <div class="card-body"><pre>{_esc(body_text)}</pre></div>
</div>
"""

    if meta_json and meta_json != "{}":
        details += f"""
<div class="card mb-3">
  <div class="card-header">Meta</div>
  <div class="card-body"><pre>{_esc(meta_json)}</pre></div>
</div>
"""

    cols = ["WA Status", "Received At", "Err Code", "Err Details"]
    delivery_rows = []
    for d in deliveries:
        delivery_rows.append(
            [
                _status_badge(d.wa_status) if d.wa_status else "",
                _esc(_fmt_dt(d.received_at, tz)),
                _esc(d.err_code or ""),
                _esc((d.err_details or "")[:120]),
            ]
        )

    delivery_section = f"""
<h5>WhatsApp Delivery Statuses</h5>
{_table(cols, delivery_rows) if delivery_rows else '<p class="text-muted">No delivery events.</p>'}
"""

    body = f"""
<h4>📨 Outbox #{outbox_id}</h4>
{details}
{delivery_section}
"""
    return _page(f"Outbox {outbox_id}", body)


# ---------------------------------------------------------------------------
# /ops/whatsapp/inbox
# ---------------------------------------------------------------------------


def _detect_cmd(payload: dict | None) -> str:
    if not payload:
        return ""
    try:
        entry0 = (payload.get("entry") or [])[0]
        changes0 = (entry0.get("changes") or [])[0]
        value = changes0.get("value") or {}
        messages = value.get("messages") or []
        if not messages:
            return ""
        body = (messages[0].get("text") or {}).get("body") or ""
        body_up = body.strip().upper()
        if body_up == "STOP":
            return "stop"
        if body_up == "START":
            return "start"
    except Exception:
        pass
    return ""


def _wa_tabs_html(active_tab: str, base_params: str) -> str:
    """Render Bootstrap nav-tabs for Inbox / Delivery."""
    tabs = [
        ("inbox", "📥 Inbox", "Входящие сообщения"),
        ("delivery", "📬 Delivery", "Статусы доставки"),
    ]
    items = []
    for tab_id, label, title in tabs:
        params = f"tab={tab_id}"
        if base_params:
            params = f"{base_params}&tab={tab_id}"
        active = " active" if tab_id == active_tab else ""
        items.append(
            f'<li class="nav-item">'
            f'<a class="nav-link{active}" href="/ops/whatsapp/inbox?{params}"'
            f' title="{_esc(title)}">{label}</a>'
            f"</li>"
        )
    return '<ul class="nav nav-tabs mb-3">' + "".join(items) + "</ul>"


@router.get("/whatsapp/inbox", response_class=HTMLResponse)
async def ops_wa_inbox(request: Request) -> str:
    tab = request.query_params.get("tab", "inbox")
    if tab not in ("inbox", "delivery"):
        tab = "inbox"
    pni_filter = request.query_params.get("pni", "")
    from_filter = request.query_params.get("wa_from", "")
    status_filter = request.query_params.get("status", "")
    only_cmds = request.query_params.get("only_commands", "")
    period = request.query_params.get("period", "24h")
    from_dt, to_dt = _period_params(request)
    tz = _local_tz()

    # Build base params string (without tab) for tab links
    base_parts = []
    if pni_filter:
        base_parts.append(f"pni={_esc(pni_filter)}")
    if from_filter:
        base_parts.append(f"wa_from={_esc(from_filter)}")
    if status_filter:
        base_parts.append(f"status={_esc(status_filter)}")
    if only_cmds:
        base_parts.append(f"only_commands={_esc(only_cmds)}")
    if period and period != "24h":
        base_parts.append(f"period={_esc(period)}")
    base_params_str = "&".join(base_parts)

    async with SessionLocal() as session:
        # NOTE: all user inputs go into `params` as bound parameters;
        # `filters` contains only hardcoded SQL clauses.
        filters = ["we.received_at >= :from_dt AND we.received_at < :to_dt"]
        params: dict[str, Any] = {
            "from_dt": from_dt,
            "to_dt": to_dt,
        }

        if pni_filter:
            filters.append("we.payload #>> '{entry,0,changes,0,value,metadata,phone_number_id}' = :pni")
            params["pni"] = pni_filter

        if tab == "inbox":
            # Only inbound messages: messages[0].id must exist
            filters.append("we.payload #> '{entry,0,changes,0,value,messages,0,id}' IS NOT NULL")
            if from_filter:
                filters.append("we.payload #>> '{entry,0,changes,0,value,messages,0,from}' ILIKE :wa_from")
                params["wa_from"] = f"%{from_filter}%"
            if status_filter and _safe_identifier(status_filter):
                filters.append("we.status = :status_filter")
                params["status_filter"] = status_filter
            if only_cmds == "1":
                filters.append(
                    "upper(trim(we.payload #>> '{entry,0,changes,0,value,messages,0,text,body}')) IN ('STOP','START')"
                )

            where = " AND ".join(filters)
            rows_q = await session.execute(
                text(f"""
                    SELECT
                      we.id,
                      we.received_at,
                      we.status,
                      we.error,
                      we.payload
                    FROM whatsapp_events we
                    WHERE {where}
                    ORDER BY we.received_at DESC
                    LIMIT 200
                """),
                params,
            )
            rows = rows_q.fetchall()

        else:
            # delivery tab: status events from whatsapp_events + fallback from outbox_messages
            # Базовые фильтры для whatsapp_events (received_at) копируются в wa_filters
            wa_filters = list(filters)
            wa_filters.append("we.payload #> '{entry,0,changes,0,value,statuses,0,id}' IS NOT NULL")

            # Базовые фильтры для outbox_messages (sent_at/scheduled_at)
            om_filters = [
                "COALESCE(om.sent_at, om.scheduled_at) >= :from_dt",
                "COALESCE(om.sent_at, om.scheduled_at) < :to_dt",
                "om.provider_message_id IS NOT NULL",
                "om.status IN ('sent', 'failed')",
            ]

            if pni_filter:
                om_filters.append("ws.phone_number_id = :pni")

            if status_filter and status_filter in {"sent", "delivered", "read", "failed"}:
                wa_filters.append("we.payload #>> '{entry,0,changes,0,value,statuses,0,status}' = :status_filter")
                params["status_filter"] = status_filter
                if status_filter in {"sent", "failed"}:
                    # outbox_messages can represent sent/failed; filter to match
                    om_filters.append("om.status = :status_filter")
                else:
                    # delivered/read only come from webhook statuses, not outbox_messages
                    om_filters.append("FALSE")

            wa_where = " AND ".join(wa_filters)
            om_where = " AND ".join(om_filters)

            rows_q = await session.execute(
                text(f"""
                            WITH wa_statuses AS (
                                SELECT
                                  we.payload #>> '{{entry,0,changes,0,value,statuses,0,id}}' AS status_msg_id,
                                  we.payload #>> '{{entry,0,changes,0,value,statuses,0,status}}' AS status_value,
                                  we.payload #>> '{{entry,0,changes,0,value,metadata,phone_number_id}}' AS pni,
                                  we.payload #>> '{{entry,0,changes,0,value,statuses,0,errors,0,code}}' AS err_code,
                                  we.payload #>> '{{entry,0,changes,0,value,statuses,0,errors,0,title}}' AS err_details,
                                  max(we.received_at) AS received_at,
                                  count(*) AS cnt
                                FROM whatsapp_events we
                                WHERE {wa_where}
                                GROUP BY
                                  status_msg_id,
                                  status_value,
                                  pni,
                                  err_code,
                                  err_details
                            ),
                            om_fallback AS (
                                SELECT
                                  om.provider_message_id AS status_msg_id,
                                  om.status AS status_value,
                                  ws.phone_number_id AS pni,
                                  NULL::text AS err_code,
                                  om.error AS err_details,
                                  COALESCE(om.sent_at, om.scheduled_at) AS received_at,
                                  1::bigint AS cnt
                                FROM outbox_messages om
                                LEFT JOIN whatsapp_senders ws ON ws.id = om.sender_id
                                LEFT JOIN wa_statuses wa ON wa.status_msg_id = om.provider_message_id
                                WHERE {om_where}
                                  AND wa.status_msg_id IS NULL
                            )
                            SELECT * FROM wa_statuses
                            UNION ALL
                            SELECT * FROM om_fallback
                            ORDER BY received_at DESC
                            LIMIT 200
                        """),
                params,
            )
            rows = rows_q.fetchall()

    def _pni_from_payload(p: dict) -> str:
        try:
            return p["entry"][0]["changes"][0]["value"]["metadata"]["phone_number_id"]
        except Exception:
            return ""

    def _from_from_payload(p: dict) -> str:
        try:
            return p["entry"][0]["changes"][0]["value"]["messages"][0]["from"]
        except Exception:
            return ""

    def _body_from_payload(p: dict) -> str:
        try:
            return p["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"]
        except Exception:
            return ""

    tabs_html = _wa_tabs_html(tab, base_params_str)

    if tab == "inbox":
        form = _filter_form(
            "/ops/whatsapp/inbox",
            [
                ("pni", "Phone Number ID", "text", pni_filter),
                ("wa_from", "From (phone)", "text", from_filter),
                ("status", "Status", "select:received,ignored,processed,failed", status_filter),
                ("only_commands", "Only STOP/START", "select:,1", only_cmds),
                ("period", "Period", "select:24h,today,7d", period),
            ],
        )
        cols = ["ID", "Received At", "PNI", "From", "Body", "Cmd", "Status", "Error"]
        table_rows = []
        row_classes = []
        for r in rows:
            payload = r.payload or {}
            pni = _pni_from_payload(payload)
            wa_from = _from_from_payload(payload)
            msg_body = _body_from_payload(payload)
            cmd = _detect_cmd(payload)
            cmd_badge = ""
            if cmd == "stop":
                cmd_badge = '<span class="badge bg-danger">STOP</span>'
            elif cmd == "start":
                cmd_badge = '<span class="badge bg-success">START</span>'
            row_class = "warn" if r.status == "ignored" else ("stuck" if r.status == "failed" else "")
            table_rows.append(
                [
                    str(r.id),
                    _esc(_fmt_dt(r.received_at, tz)),
                    _esc(pni[:30]),
                    _esc(wa_from),
                    _esc((msg_body or "")[:60]),
                    cmd_badge,
                    _status_badge(r.status),
                    f'<span class="small text-muted">{_esc((r.error or "")[:80])}</span>',
                ]
            )
            row_classes.append(row_class)

        body = f"""
<h4>💬 WhatsApp Events</h4>
{tabs_html}
{form}
<p class="text-muted small">Showing {len(rows)} rows · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""
    else:
        form = _filter_form(
            "/ops/whatsapp/inbox",
            [
                ("pni", "Phone Number ID", "text", pni_filter),
                ("period", "Period", "select:24h,today,7d", period),
            ],
            hidden={"tab": "delivery"},
        )
        cols = ["Status Msg ID", "Status", "PNI", "Err Code", "Err Details", "Last Seen", "Count"]
        table_rows = []
        row_classes = []
        for r in rows:
            status_badge = _status_badge(r.status_value) if r.status_value else ""
            err_html = f'<span class="text-danger small">{_esc(r.err_code or "")}</span>'
            row_class = "stuck" if r.err_code else ""
            table_rows.append(
                [
                    _esc((r.status_msg_id or "")[:60]),
                    status_badge,
                    _esc((r.pni or "")[:30]),
                    err_html,
                    _esc((r.err_details or "")[:80]),
                    _esc(_fmt_dt(r.received_at, tz)),
                    _esc(str(r.cnt)),
                ]
            )
            row_classes.append(row_class)

        body = f"""
<h4>💬 WhatsApp Events</h4>
{tabs_html}
{form}
<p class="text-muted small">Showing {len(rows)} aggregated statuses · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""

    return _page("WA Events", body)


# ---------------------------------------------------------------------------
# /ops/optouts
# ---------------------------------------------------------------------------


@router.get("/optouts", response_class=HTMLResponse)
async def ops_optouts(request: Request) -> str:
    company_id = request.query_params.get("company_id", "")
    phone = request.query_params.get("phone_e164", "")
    period = request.query_params.get("period", "")
    from_dt_raw = request.query_params.get("from_dt", "")
    to_dt_raw = request.query_params.get("to_dt", "")
    tz = _local_tz()

    async with SessionLocal() as session:
        # NOTE: `filters` contains only hardcoded SQL clauses;
        # user input is passed exclusively via bound `params`.
        filters = ["c.wa_opted_out = true"]
        params: dict[str, Any] = {}

        if company_id:
            filters.append("c.company_id = :company_id")
            params["company_id"] = int(company_id)
        if phone:
            phone_digits = re.sub(r"\D", "", phone.strip())
            if phone_digits:
                filters.append(
                    "(replace(c.phone_e164, '+', '') ILIKE :phone_pattern "
                    "OR :phone_digits ILIKE '%' || replace(c.phone_e164, '+', '') || '%')"
                )
                params["phone_pattern"] = f"%{phone_digits}%"
                params["phone_digits"] = phone_digits

        now = datetime.now(timezone.utc)
        if period == "today":
            params["from_dt"] = now.replace(hour=0, minute=0, second=0, microsecond=0)
            params["to_dt"] = now
            filters.append("c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt")
        elif period == "7d":
            params["from_dt"] = now - timedelta(days=7)
            params["to_dt"] = now
            filters.append("c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt")
        elif from_dt_raw and to_dt_raw:
            try:
                params["from_dt"] = datetime.fromisoformat(from_dt_raw).replace(tzinfo=timezone.utc)
                params["to_dt"] = datetime.fromisoformat(to_dt_raw).replace(tzinfo=timezone.utc)
                filters.append("c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt")
            except ValueError:
                pass

        where = " AND ".join(filters)
        rows_q = await session.execute(
            text(f"""
                SELECT * FROM (
                  SELECT DISTINCT ON (c.company_id, c.phone_e164)
                    c.id, c.company_id, c.display_name, c.phone_e164,
                    c.wa_opted_out_at, c.wa_opt_out_reason,
                    -- last STOP command event
                    (
                      SELECT we.id
                      FROM whatsapp_events we
                      WHERE
                        upper(trim(
                          we.payload #>> '{{entry,0,changes,0,value,messages,0,text,body}}'
                        )) = 'STOP'
                        AND we.payload #>> '{{entry,0,changes,0,value,messages,0,from}}'
                            LIKE '%' || replace(c.phone_e164, '+', '') || '%'
                      ORDER BY we.received_at DESC
                      LIMIT 1
                    ) AS last_stop_event_id
                  FROM clients c
                  WHERE {where}
                  ORDER BY c.company_id, c.phone_e164, c.wa_opted_out_at DESC NULLS LAST
                ) sub
                ORDER BY sub.wa_opted_out_at DESC NULLS LAST
                LIMIT 200
            """),
            params,
        )
        rows = rows_q.fetchall()

        # Counts per company
        counts_q = await session.execute(
            text("""
                 SELECT company_id, COUNT(*) AS cnt
                 FROM clients
                 WHERE wa_opted_out = true
                 GROUP BY company_id
                 """)
        )
        counts = counts_q.fetchall()

    counts_html = '<ul class="list-inline">'
    for cnt in counts:
        cname = COMPANIES.get(cnt.company_id, str(cnt.company_id))
        counts_html += f'<li class="list-inline-item"><span class="badge bg-danger">{cname}: {cnt.cnt}</span></li>'
    counts_html += "</ul>"

    form = _filter_form(
        "/ops/optouts",
        [
            ("company_id", "Company", "select:758285,1271200", company_id),
            ("phone_e164", "Phone", "text", phone),
            ("period", "Period", "select:,today,7d", period),
        ],
    )

    cols = [
        "ID",
        "Company",
        "Name",
        "Phone",
        "Opted Out At",
        "Reason",
        "Last STOP Event",
    ]
    table_rows = []
    for r in rows:
        company_name = COMPANIES.get(r.company_id, str(r.company_id))
        stop_link = ""
        if r.last_stop_event_id:
            stop_link = f'<a href="/ops/whatsapp/inbox">#{r.last_stop_event_id}</a>'
        table_rows.append(
            [
                str(r.id),
                _esc(company_name),
                _esc(r.display_name or ""),
                _esc(r.phone_e164 or ""),
                _esc(_fmt_dt(r.wa_opted_out_at, tz)),
                _esc(r.wa_opt_out_reason or ""),
                stop_link,
            ]
        )

    body = f"""
<h4>🚫 Opt-outs</h4>
{counts_html}
{form}
<p class="text-muted small">Showing {len(rows)} opted-out clients</p>
{_table(cols, table_rows)}
"""
    return _page("Opt-outs", body)


# ---------------------------------------------------------------------------
# /ops/monitoring
# ---------------------------------------------------------------------------


@router.get("/monitoring", response_class=HTMLResponse)
async def ops_monitoring() -> str:
    tz = _local_tz()
    now = datetime.now(timezone.utc)
    window_24h = now - timedelta(hours=24)
    window_1h = now - timedelta(hours=1)
    window_15m = now - timedelta(minutes=15)
    window_5m = now - timedelta(minutes=5)
    stuck_threshold = timedelta(minutes=settings.ops_stuck_minutes)

    async with SessionLocal() as session:
        # --- Webhook Ingress ---
        ingress_q = await session.execute(
            text("""
                 SELECT MAX(received_at)                            AS last_altegio,
                        COUNT(*) FILTER (WHERE received_at >= :w5)  AS altegio_5m,
                        COUNT(*) FILTER (WHERE received_at >= :w15) AS altegio_15m,
                        COUNT(*) FILTER (WHERE received_at >= :w1h) AS altegio_1h
                 FROM altegio_events
                 """),
            {"w5": window_5m, "w15": window_15m, "w1h": window_1h},
        )
        altegio_ing = ingress_q.fetchone()

        wa_ingress_q = await session.execute(
            text("""
                 SELECT MAX(received_at) AS last_wa,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w5 AND status != 'ignored'
                            )            AS wa_5m,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w5 AND status = 'ignored'
                            )            AS wa_ignored_5m,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w15 AND status != 'ignored'
                            )            AS wa_15m,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w15 AND status = 'ignored'
                            )            AS wa_ignored_15m,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w1h AND status != 'ignored'
                            )            AS wa_1h,
                        COUNT(*) FILTER (
                            WHERE received_at >= :w1h AND status = 'ignored'
                            )            AS wa_ignored_1h
                 FROM whatsapp_events
                 """),
            {"w5": window_5m, "w15": window_15m, "w1h": window_1h},
        )
        wa_ing = wa_ingress_q.fetchone()

        # Chatwoot-specific ingress metrics
        cw_ingress_q = await session.execute(
            text("""
                 SELECT MAX(received_at)                            AS last_cw,
                        COUNT(*) FILTER (WHERE received_at >= :w5)  AS cw_5m,
                        COUNT(*) FILTER (WHERE received_at >= :w15) AS cw_15m,
                        COUNT(*) FILTER (WHERE received_at >= :w1h) AS cw_1h
                 FROM whatsapp_events
                 WHERE chatwoot_conversation_id IS NOT NULL
                 """),
            {"w5": window_5m, "w15": window_15m, "w1h": window_1h},
        )
        cw_ing = cw_ingress_q.fetchone()

        # --- Queue Health ---
        queue_q = await session.execute(
            text("""
                 SELECT company_id,
                        COUNT(*) FILTER (WHERE status = 'queued')     AS queued_cnt,
                        COUNT(*) FILTER (WHERE status = 'processing') AS proc_cnt,
                        COUNT(*) FILTER (
                            WHERE status = 'failed' AND updated_at >= :w24h
                            )                                         AS failed_24h,
                        COUNT(*) FILTER (
                            WHERE status = 'processing'
                                AND locked_at < :stuck_ts
                            )                                         AS stuck_cnt
                 FROM message_jobs
                 GROUP BY company_id
                 ORDER BY company_id
                 """),
            {
                "w24h": window_24h,
                "stuck_ts": now - stuck_threshold,
            },
        )
        queue_rows = queue_q.fetchall()

        queue_total_q = await session.execute(
            text("""
                 SELECT COUNT(*) FILTER (WHERE status = 'queued')     AS queued_cnt,
                        COUNT(*) FILTER (WHERE status = 'processing') AS proc_cnt,
                        COUNT(*) FILTER (
                            WHERE status = 'failed' AND updated_at >= :w24h
                            )                                         AS failed_24h,
                        COUNT(*) FILTER (
                            WHERE status = 'processing'
                                AND locked_at < :stuck_ts
                            )                                         AS stuck_cnt
                 FROM message_jobs
                 """),
            {
                "w24h": window_24h,
                "stuck_ts": now - stuck_threshold,
            },
        )
        qt = queue_total_q.fetchone()

        # --- Outbox Health ---
        outbox_q = await session.execute(
            text("""
                 SELECT COUNT(*) FILTER (WHERE status = 'sent')   AS sent_cnt,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_cnt,
                        COUNT(*)                                  AS total_cnt
                 FROM outbox_messages
                 WHERE created_at >= :w24h
                 """),
            {"w24h": window_24h},
        )
        ob = outbox_q.fetchone()

        # Top outbox errors
        top_errors_q = await session.execute(
            text("""
                 SELECT COALESCE(error, 'unknown') AS err,
                        COUNT(*)                   AS cnt
                 FROM outbox_messages
                 WHERE status = 'failed'
                   AND created_at >= :w24h
                 GROUP BY err
                 ORDER BY cnt DESC
                 LIMIT 5
                 """),
            {"w24h": window_24h},
        )
        top_errors = top_errors_q.fetchall()

        # Top WA delivery errors
        wa_errors_q = await session.execute(
            text("""
                 SELECT payload #>>
                        '{entry,0,changes,0,value,statuses,0,errors,0,code}' AS err_code,
                        COUNT(*)                                             AS cnt
                 FROM whatsapp_events
                 WHERE received_at >= :w24h
                   AND payload #>>
                       '{entry,0,changes,0,value,statuses,0,status}' = 'failed'
                 GROUP BY err_code
                 ORDER BY cnt DESC
                 LIMIT 5
                 """),
            {"w24h": window_24h},
        )
        wa_errors = wa_errors_q.fetchall()

        # --- Opt-out stats ---
        optout_q = await session.execute(
            text("""
                 SELECT company_id, COUNT(*) AS cnt
                 FROM clients
                 WHERE wa_opted_out = true
                 GROUP BY company_id
                 ORDER BY company_id
                 """)
        )
        optout_rows = optout_q.fetchall()

        # Last STOP/START events
        last_cmds_q = await session.execute(
            text("""
                 SELECT we.id,
                        we.received_at,
                        we.status,
                        upper(trim(
                                we.payload #>>
                                '{entry,0,changes,0,value,messages,0,text,body}'
                              ))                                    AS cmd,
                        we.payload #>>
                        '{entry,0,changes,0,value,messages,0,from}' AS wa_from
                 FROM whatsapp_events we
                 WHERE upper(trim(
                         we.payload #>>
                         '{entry,0,changes,0,value,messages,0,text,body}'
                             )) IN ('STOP', 'START')
                 ORDER BY we.received_at DESC
                 LIMIT 20
                 """)
        )
        last_cmds = last_cmds_q.fetchall()

        # --- Queue breakdown by job type ---
        queue_by_type_q = await session.execute(
            text("""
                 SELECT job_type,
                        COUNT(*) FILTER (WHERE status = 'queued')     AS queued_cnt,
                        COUNT(*) FILTER (WHERE status = 'processing') AS proc_cnt,
                        COUNT(*) FILTER (
                            WHERE status = 'failed' AND updated_at >= :w24h
                            )                                         AS failed_24h
                 FROM message_jobs
                 GROUP BY job_type
                 ORDER BY queued_cnt DESC, job_type
                 """),
            {"w24h": window_24h},
        )
        queue_by_type = queue_by_type_q.fetchall()

        # --- Scheduled Reminders (next 15 min) ---
        next_15m = now + timedelta(minutes=15)
        scheduled_q = await session.execute(
            text("""
                 SELECT job_type,
                        COUNT(*)    AS cnt,
                        MIN(run_at) AS next_run
                 FROM message_jobs
                 WHERE status = 'queued'
                   AND run_at >= :now
                   AND run_at <= :next_15m
                 GROUP BY job_type
                 ORDER BY next_run
                 """),
            {"now": now, "next_15m": next_15m},
        )
        scheduled_rows = scheduled_q.fetchall()

        # --- Outbox status distribution (24h) ---
        outbox_status_q = await session.execute(
            text("""
                 SELECT status, COUNT(*) AS cnt
                 FROM outbox_messages
                 WHERE created_at >= :w24h
                 GROUP BY status
                 ORDER BY cnt DESC
                 """),
            {"w24h": window_24h},
        )
        outbox_status_rows = outbox_status_q.fetchall()

        # Outbox throughput (last 1h)
        outbox_speed_q = await session.execute(
            text("""
                 SELECT COUNT(*) AS sent_1h
                 FROM outbox_messages
                 WHERE status IN ('sent', 'delivered', 'read')
                   AND sent_at >= :w1h
                 """),
            {"w1h": window_1h},
        )
        ob_speed = outbox_speed_q.fetchone()

        # --- Upcoming Reminders (next 24h and 7d) ---
        upcoming_reminders_q = await session.execute(
            text("""
                 SELECT company_id,
                        job_type,
                        COUNT(*) FILTER (
                            WHERE run_at >= :now AND run_at < :next_24h
                            ) AS next_24h_cnt,
                        COUNT(*) FILTER (
                            WHERE run_at >= :now AND run_at < :next_7d
                            ) AS next_7d_cnt
                 FROM message_jobs
                 WHERE status = 'queued'
                   AND job_type IN ('reminder_24h', 'reminder_2h')
                 GROUP BY company_id, job_type
                 ORDER BY company_id, job_type
                 """),
            {
                "now": now,
                "next_24h": now + timedelta(hours=24),
                "next_7d": now + timedelta(days=7),
            },
        )
        upcoming_reminder_rows = upcoming_reminders_q.fetchall()

        # --- Missing Reminders ---
        # reminder_24h: records with starts_at in [now+24h, now+8d]
        # without a queued/done reminder_24h job
        missing_24h_q = await session.execute(
            text("""
                 SELECT r.company_id, COUNT(*) AS missing_cnt
                 FROM records r
                 WHERE r.starts_at >= :h24
                   AND r.starts_at < :d8
                   AND r.is_deleted = false
                   AND r.client_id IS NOT NULL
                   AND NOT EXISTS (SELECT 1
                                   FROM message_jobs mj
                                   WHERE mj.record_id = r.id
                                     AND mj.job_type = 'reminder_24h'
                                     AND mj.status IN ('queued', 'done', 'processing'))
                 GROUP BY r.company_id
                 ORDER BY r.company_id
                 """),
            {
                "h24": now + timedelta(hours=24),
                "d8": now + timedelta(days=8),
            },
        )
        missing_24h_rows = missing_24h_q.fetchall()

        # reminder_2h: records with starts_at in [now+2h, now+26h]
        # without a queued/done reminder_2h job
        missing_2h_q = await session.execute(
            text("""
                 SELECT r.company_id, COUNT(*) AS missing_cnt
                 FROM records r
                 WHERE r.starts_at >= :h2
                   AND r.starts_at < :h26
                   AND r.is_deleted = false
                   AND r.client_id IS NOT NULL
                   AND NOT EXISTS (SELECT 1
                                   FROM message_jobs mj
                                   WHERE mj.record_id = r.id
                                     AND mj.job_type = 'reminder_2h'
                                     AND mj.status IN ('queued', 'done', 'processing'))
                 GROUP BY r.company_id
                 ORDER BY r.company_id
                 """),
            {
                "h2": now + timedelta(hours=2),
                "h26": now + timedelta(hours=26),
            },
        )
        missing_2h_rows = missing_2h_q.fetchall()

        # --- Opt-out Impact ---
        optout_impact_q = await session.execute(
            text("""
                 SELECT mj.company_id, COUNT(*) AS optout_queued_cnt
                 FROM message_jobs mj
                          JOIN clients c ON c.id = mj.client_id
                 WHERE mj.status = 'queued'
                   AND mj.job_type = ANY (
                     ARRAY ['review_3d','repeat_10d','comeback_3d',
                         'newsletter_new_clients_monthly']
                     )
                   AND c.wa_opted_out = true
                 GROUP BY mj.company_id
                 ORDER BY mj.company_id
                 """),
        )
        optout_impact_rows = optout_impact_q.fetchall()

        # --- Problematic tasks ---
        overdue_q = await session.execute(
            text("""
                 SELECT COUNT(*) AS overdue_cnt
                 FROM message_jobs
                 WHERE status = 'queued'
                   AND run_at < :now
                 """),
            {"now": now},
        )
        overdue_row = overdue_q.fetchone()

        locked_q = await session.execute(
            text("""
                 SELECT COUNT(*) AS locked_cnt
                 FROM message_jobs
                 WHERE locked_at IS NOT NULL
                   AND status = 'queued'
                 """)
        )
        locked_row = locked_q.fetchone()

        no_outbox_q = await session.execute(
            text("""
                 SELECT COUNT(*) AS no_outbox_cnt
                 FROM message_jobs mj
                 WHERE mj.status = 'done'
                   AND mj.updated_at >= :w24h
                   AND NOT EXISTS (SELECT 1
                                   FROM outbox_messages om
                                   WHERE om.job_id = mj.id)
                 """),
            {"w24h": window_24h},
        )
        no_outbox_row = no_outbox_q.fetchone()

    # ---- Build HTML ----

    # 1) API Health
    health_html = f"""
<div class="card mb-3 border-success">
  <div class="card-header">🟢 API Health</div>
  <div class="card-body">
    <p class="mb-1"><strong>Status:</strong> Running ✓</p>
    <p class="mb-0 text-muted small">Current time (UTC): {now.strftime("%Y-%m-%d %H:%M:%S")}</p>
  </div>
</div>
"""

    # 2) Webhook Ingress
    altegio_last = altegio_ing.last_altegio
    wa_last = wa_ing.last_wa
    altegio_warn = altegio_last is None or (now - altegio_last.replace(tzinfo=timezone.utc)).total_seconds() > 15 * 60
    wa_warn = wa_last is None or (now - wa_last.replace(tzinfo=timezone.utc)).total_seconds() > 15 * 60

    from altegio_bot.settings import settings as _settings

    cw_enabled = _settings.chatwoot_enabled
    cw_last = cw_ing.last_cw if cw_ing else None

    cw_row_html = ""
    if cw_enabled:
        cw_warn = cw_last is None or (now - cw_last.replace(tzinfo=timezone.utc)).total_seconds() > 15 * 60
        cw_row_html = (
            f'<tr class="{"warn" if cw_warn else ""}">'
            f"<td>Chatwoot</td>"
            f"<td>{_esc(_fmt_dt(cw_last, tz))} ({_esc(_ago(cw_last))})</td>"
            f"<td>{cw_ing.cw_5m if cw_ing else 0}</td>"
            f"<td>{cw_ing.cw_15m if cw_ing else 0}</td>"
            f"<td>{cw_ing.cw_1h if cw_ing else 0}</td>"
            f"</tr>"
        )

    ingress_html = f"""
<div class="card mb-3 {"border-warning" if altegio_warn or wa_warn else "border-success"}">
  <div class="card-header">📡 Webhook Ingress</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Source</th><th>Last Event</th>
        <th>5m</th><th>15m</th><th>1h</th>
      </tr></thead>
      <tbody>
        <tr class="{"warn" if altegio_warn else ""}">
          <td>Altegio</td>
          <td>{_esc(_fmt_dt(altegio_last, tz))} ({_esc(_ago(altegio_last))})</td>
          <td>{altegio_ing.altegio_5m}</td>
          <td>{altegio_ing.altegio_15m}</td>
          <td>{altegio_ing.altegio_1h}</td>
        </tr>
        <tr class="{"warn" if wa_warn else ""}">
          <td>WhatsApp</td>
          <td>{_esc(_fmt_dt(wa_last, tz))} ({_esc(_ago(wa_last))})</td>
          <td>{wa_ing.wa_5m} (+{wa_ing.wa_ignored_5m} ign)</td>
          <td>{wa_ing.wa_15m} (+{wa_ing.wa_ignored_15m} ign)</td>
          <td>{wa_ing.wa_1h} (+{wa_ing.wa_ignored_1h} ign)</td>
        </tr>
        {cw_row_html}
      </tbody>
    </table>
  </div>
</div>
"""

    # 3) Queue Health
    total_stuck = qt.stuck_cnt or 0
    queue_border = "border-danger" if total_stuck > 0 or (qt.failed_24h or 0) > 0 else "border-success"
    queue_rows_html = ""
    for qr in queue_rows:
        cname = COMPANIES.get(qr.company_id, str(qr.company_id))
        queue_rows_html += (
            f"<tr>"
            f"<td>{_esc(cname)}</td>"
            f"<td>{qr.queued_cnt}</td>"
            f"<td>{qr.proc_cnt}</td>"
            f"<td>{qr.failed_24h}</td>"
            f'<td class="{"text-danger fw-bold" if qr.stuck_cnt > 0 else ""}">'
            f"{qr.stuck_cnt}</td>"
            f"</tr>"
        )
    queue_html = f"""
<div class="card mb-3 {queue_border}">
  <div class="card-header">📋 Queue Health (24h)</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Company</th><th>Queued</th><th>Processing</th>
        <th>Failed (24h)</th><th>Stuck</th>
      </tr></thead>
      <tbody>
        {queue_rows_html}
        <tr class="table-secondary fw-bold">
          <td>TOTAL</td>
          <td>{qt.queued_cnt}</td><td>{qt.proc_cnt}</td>
          <td>{qt.failed_24h}</td>
          <td class="{"text-danger" if total_stuck > 0 else ""}">{total_stuck}</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>
"""

    # 3b) Queue breakdown by job type
    queue_by_type_html = ""
    for qt_row in queue_by_type:
        queue_by_type_html += (
            f"<tr>"
            f"<td>{_esc(qt_row.job_type)}</td>"
            f"<td>{qt_row.queued_cnt}</td>"
            f"<td>{qt_row.proc_cnt}</td>"
            f"<td>{qt_row.failed_24h}</td>"
            f"</tr>"
        )
    queue_by_type_section = f"""
<div class="card mb-3">
  <div class="card-header">📊 Queue by Job Type</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Job Type</th><th>Queued</th><th>Processing</th><th>Failed (24h)</th>
      </tr></thead>
      <tbody>
        {queue_by_type_html or '<tr><td colspan="4" class="text-muted">No jobs</td></tr>'}
      </tbody>
    </table>
  </div>
</div>
"""

    # 3c) Scheduled Reminders (next 15 min)
    scheduled_html = ""
    for sr in scheduled_rows:
        next_run_str = _fmt_dt(sr.next_run, tz)
        scheduled_html += f"<tr><td>{_esc(sr.job_type)}</td><td>{sr.cnt}</td><td>{_esc(next_run_str)}</td></tr>"
    scheduled_section = f"""
<div class="card mb-3">
  <div class="card-header">⏰ Scheduled Reminders (next 15 min)</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Job Type</th><th>Count</th><th>Next Run</th>
      </tr></thead>
      <tbody>
        {
        scheduled_html
        or ('<tr><td colspan="3" class="text-muted">No reminders scheduled in the next 15 minutes</td></tr>')
    }
      </tbody>
    </table>
  </div>
</div>
"""

    # 3d) Upcoming reminders (next 24h / 7d)
    upcoming_rem_html = ""
    for ur in upcoming_reminder_rows:
        cname = COMPANIES.get(ur.company_id, str(ur.company_id))
        upcoming_rem_html += (
            f"<tr>"
            f"<td>{_esc(cname)}</td>"
            f"<td>{_esc(ur.job_type)}</td>"
            f"<td>{ur.next_24h_cnt}</td>"
            f"<td>{ur.next_7d_cnt}</td>"
            f"</tr>"
        )
    upcoming_reminders_section = f"""
<div class="card mb-3">
  <div class="card-header">🔔 Upcoming Reminders</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Company</th><th>Job Type</th><th>Next 24h</th><th>Next 7d</th>
      </tr></thead>
      <tbody>
        {upcoming_rem_html or '<tr><td colspan="4" class="text-muted">No upcoming reminders</td></tr>'}
      </tbody>
    </table>
    <p class="mb-0 mt-1">
      <a href="/ops/queue?view=upcoming_7d&job_type=reminder_24h" class="small">
        → View reminder_24h queue
      </a>
      &nbsp;·&nbsp;
      <a href="/ops/queue?view=upcoming_7d&job_type=reminder_2h" class="small">
        → View reminder_2h queue
      </a>
    </p>
  </div>
</div>
"""

    # 3e) Missing reminders
    missing_24h_by_company: dict[int, int] = {r.company_id: r.missing_cnt for r in missing_24h_rows}
    missing_2h_by_company: dict[int, int] = {r.company_id: r.missing_cnt for r in missing_2h_rows}
    all_company_ids = sorted(set(missing_24h_by_company) | set(missing_2h_by_company))
    missing_rem_html = ""
    for cid in all_company_ids:
        cname = COMPANIES.get(cid, str(cid))
        m24 = missing_24h_by_company.get(cid, 0)
        m2 = missing_2h_by_company.get(cid, 0)
        cls24 = "text-danger fw-bold" if m24 > 0 else "text-success"
        cls2 = "text-danger fw-bold" if m2 > 0 else "text-success"
        missing_rem_html += f'<tr><td>{_esc(cname)}</td><td class="{cls24}">{m24}</td><td class="{cls2}">{m2}</td></tr>'
    total_missing_24h = sum(missing_24h_by_company.values())
    total_missing_2h = sum(missing_2h_by_company.values())
    missing_border = "border-danger" if (total_missing_24h > 0 or total_missing_2h > 0) else "border-success"
    missing_reminders_section = f"""
<div class="card mb-3 {missing_border}">
  <div class="card-header">⚠️ Missing Reminders</div>
  <div class="card-body">
    <p class="small text-muted mb-1">
      reminder_24h: records with appointment in 24h–8d without a reminder job.
      reminder_2h: records with appointment in 2h–26h (next 24h window) without a reminder job.
    </p>
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Company</th>
        <th>Missing reminder_24h (24h–8d)</th>
        <th>Missing reminder_2h (2h–26h)</th>
      </tr></thead>
      <tbody>
        {missing_rem_html or '<tr><td colspan="3" class="text-success">No missing reminders ✓</td></tr>'}
      </tbody>
    </table>
  </div>
</div>
"""

    # 3f) Opt-out impact
    optout_impact_html = ""
    total_impact = 0
    for oi in optout_impact_rows:
        cname = COMPANIES.get(oi.company_id, str(oi.company_id))
        optout_impact_html += (
            f'<li class="list-group-item d-flex justify-content-between align-items-center">'
            f"{_esc(cname)}"
            f'<span class="badge bg-danger rounded-pill">{oi.optout_queued_cnt}</span>'
            f"</li>"
        )
        total_impact += oi.optout_queued_cnt
    optout_impact_border = "border-danger" if total_impact > 0 else "border-success"
    impact_note = (
        '<div class="alert alert-danger mb-2 py-1 small">'
        f"⚠️ {total_impact} queued marketing job(s) for opted-out clients!"
        "</div>"
        if total_impact > 0
        else ""
    )
    optout_impact_section = f"""
<div class="card mb-3 {optout_impact_border}">
  <div class="card-header">🚫 Opt-out Impact (queued marketing jobs)</div>
  <div class="card-body">
    {impact_note}
    <ul class="list-group list-group-flush">
      {
        optout_impact_html
        or '<li class="list-group-item text-success">No marketing jobs queued for opted-out clients ✓</li>'
    }
    </ul>
    <p class="small text-muted mt-1 mb-0">
      Affected job types: review_3d, repeat_10d, comeback_3d, newsletter_new_clients_monthly
    </p>
  </div>
</div>
"""

    # 4) Outbox Health
    ob_total = ob.total_cnt or 0
    ob_sent = ob.sent_cnt or 0
    ob_failed = ob.failed_cnt or 0
    fail_rate = round(ob_failed / ob_total * 100, 1) if ob_total > 0 else 0.0
    ob_warn = ob_failed > settings.ops_failed_warning_threshold
    ob_border = "border-danger" if ob_warn else "border-success"

    top_errors_html = ""
    for e in top_errors:
        top_errors_html += (
            f'<li class="list-group-item d-flex justify-content-between">'
            f'<span class="text-danger small">{_esc(e.err[:80])}</span>'
            f'<span class="badge bg-danger">{e.cnt}</span>'
            f"</li>"
        )

    wa_errors_html = ""
    for e in wa_errors:
        wa_errors_html += (
            f'<li class="list-group-item d-flex justify-content-between">'
            f'<span class="text-danger small">{_esc(e.err_code or "unknown")}</span>'
            f'<span class="badge bg-warning text-dark">{e.cnt}</span>'
            f"</li>"
        )

    _STATUS_BADGE_COLORS = {
        "queued": "secondary",
        "sending": "primary",
        "sent": "success",
        "delivered": "success",
        "read": "info",
        "failed": "danger",
    }
    outbox_status_html = ""
    for row in outbox_status_rows:
        color = _STATUS_BADGE_COLORS.get(row.status, "secondary")
        outbox_status_html += (
            f'<li class="list-group-item d-flex justify-content-between align-items-center">'
            f"{_esc(row.status)}"
            f'<span class="badge bg-{color} rounded-pill">{row.cnt}</span>'
            f"</li>"
        )

    ob_sent_1h = ob_speed.sent_1h if ob_speed else 0

    outbox_html = f"""
<div class="card mb-3 {ob_border}">
  <div class="card-header">📨 Outbox Health (24h)</div>
  <div class="card-body">
    <div class="row g-2 mb-2">
      <div class="col-auto">
        <span class="badge bg-success fs-6">Sent: {ob_sent}</span>
      </div>
      <div class="col-auto">
        <span class="badge bg-danger fs-6">Failed: {ob_failed}</span>
      </div>
      <div class="col-auto">
        <span class="badge bg-secondary fs-6">Total: {ob_total}</span>
      </div>
      <div class="col-auto">
        <span class="badge bg-{"danger" if ob_warn else "info"} fs-6">
          Fail rate: {fail_rate}%
        </span>
      </div>
      <div class="col-auto">
        <span class="badge bg-primary fs-6">Throughput (1h): {ob_sent_1h}</span>
      </div>
    </div>
    <div class="row g-3">
      <div class="col-md-4">
        <p class="fw-bold mb-1">Status Distribution (24h)</p>
        <ul class="list-group list-group-flush">
          {outbox_status_html or '<li class="list-group-item text-muted">No messages</li>'}
        </ul>
      </div>
      <div class="col-md-4">
        <p class="fw-bold mb-1">Top Outbox Errors</p>
        <ul class="list-group list-group-flush">
          {top_errors_html or '<li class="list-group-item text-muted">None</li>'}
        </ul>
      </div>
      <div class="col-md-4">
        <p class="fw-bold mb-1">Top WA Delivery Errors</p>
        <ul class="list-group list-group-flush">
          {wa_errors_html or '<li class="list-group-item text-muted">None</li>'}
        </ul>
      </div>
    </div>
  </div>
</div>
"""

    # 5) Opt-out
    optout_rows_html = ""
    for o in optout_rows:
        cname = COMPANIES.get(o.company_id, str(o.company_id))
        optout_rows_html += (
            f'<li class="list-inline-item"><span class="badge bg-danger">{_esc(cname)}: {o.cnt}</span></li>'
        )

    last_cmds_rows = []
    for cmd in last_cmds:
        badge = (
            '<span class="badge bg-danger">STOP</span>'
            if cmd.cmd == "STOP"
            else '<span class="badge bg-success">START</span>'
        )
        last_cmds_rows.append(
            [
                str(cmd.id),
                _esc(_fmt_dt(cmd.received_at, tz)),
                _esc(cmd.wa_from or ""),
                badge,
                _status_badge(cmd.status),
            ]
        )

    optout_html = f"""
<div class="card mb-3">
  <div class="card-header">🚫 Opt-out Summary</div>
  <div class="card-body">
    <p>
      <strong>Opted out:</strong>
      <ul class="list-inline mb-1">
        {optout_rows_html or '<li class="list-inline-item text-muted">None</li>'}
      </ul>
    </p>
    <p><a href="/ops/optouts">→ View all opt-outs</a></p>
    <h6>Last 20 STOP/START Events</h6>
    {
        _table(["ID", "Received At", "From", "Cmd", "Status"], last_cmds_rows)
        if last_cmds_rows
        else '<p class="text-muted">No commands yet.</p>'
    }
  </div>
</div>
"""

    # 6) Problematic tasks
    overdue_cnt = overdue_row.overdue_cnt if overdue_row else 0
    locked_cnt = locked_row.locked_cnt if locked_row else 0
    no_outbox_cnt = no_outbox_row.no_outbox_cnt if no_outbox_row else 0
    problems_border = "border-danger" if overdue_cnt > 0 or locked_cnt > 0 else "border-success"
    overdue_cls = "bg-danger" if overdue_cnt > 0 else "bg-success"
    locked_cls = "bg-danger" if locked_cnt > 0 else "bg-success"
    no_outbox_cls = "bg-warning text-dark" if no_outbox_cnt > 0 else "bg-success"
    problems_html = f"""
<div class="card mb-3 {problems_border}">
  <div class="card-header">🔍 Problematic Tasks</div>
  <div class="card-body">
    <div class="row g-3">
      <div class="col-auto">
        <span class="badge {overdue_cls} fs-6">
          Overdue (queued past run_at): {overdue_cnt}
        </span>
      </div>
      <div class="col-auto">
        <span class="badge {locked_cls} fs-6">
          Locked (queued + locked_at set): {locked_cnt}
        </span>
      </div>
      <div class="col-auto">
        <span class="badge {no_outbox_cls} fs-6">
          Done without outbox (24h): {no_outbox_cnt}
        </span>
      </div>
    </div>
  </div>
</div>
"""

    warnings = []
    if altegio_warn:
        _al_ts = (
            altegio_last.replace(tzinfo=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M") if altegio_last else "—"
        )
        warnings.append(f"⚠️ Last Altegio webhook > 15 minutes ago at {_al_ts}")
    if wa_warn:
        _wa_ts = wa_last.replace(tzinfo=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M") if wa_last else "—"
        warnings.append(f"⚠️ Last WhatsApp webhook > 15 minutes ago at {_wa_ts}")
    if total_stuck > 0:
        warnings.append(f"⚠️ {total_stuck} stuck processing job(s)")
    if ob_warn:
        warnings.append(
            f"⚠️ {ob_failed} failed outbox messages in last 24h (threshold: {settings.ops_failed_warning_threshold})"
        )
    if overdue_cnt > 0:
        warnings.append(f"⚠️ {overdue_cnt} overdue queued job(s) past their run_at")
    if locked_cnt > 0:
        warnings.append(f"⚠️ {locked_cnt} job(s) locked but still in queued status")
    if total_missing_24h > 0:
        warnings.append(f"⚠️ {total_missing_24h} record(s) missing reminder_24h job")
    if total_missing_2h > 0:
        warnings.append(f"⚠️ {total_missing_2h} record(s) missing reminder_2h job")
    if total_impact > 0:
        warnings.append(f"⚠️ {total_impact} queued marketing job(s) for opted-out clients")

    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{w}</li>" for w in warnings)
        warnings_html = f'<div class="alert alert-warning"><ul class="mb-0">{items}</ul></div>'

    body = f"""
<h4>📊 Monitoring</h4>
<p class="text-muted small">
  Refreshed: {now.strftime("%Y-%m-%d %H:%M:%S UTC")} /
  {now.astimezone(tz).strftime("%Y-%m-%d %H:%M")} local
  &nbsp;·&nbsp;
  <a href="/ops/monitoring">🔄 Refresh</a>
</p>
{warnings_html}
{health_html}
{ingress_html}
{queue_html}
{queue_by_type_section}
{scheduled_section}
{upcoming_reminders_section}
{missing_reminders_section}
{optout_impact_section}
{outbox_html}
{problems_html}
{optout_html}
"""
    return _page("Monitoring", body)


# ---------------------------------------------------------------------------
# /ops/ → redirect to monitoring
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def ops_index() -> HTMLResponse:
    return HTMLResponse(
        '<meta http-equiv="refresh" content="0;url=/ops/monitoring">',
        status_code=302,
        headers={"Location": "/ops/monitoring"},
    )


# ---------------------------------------------------------------------------
# /ops/login  /ops/logout  (public – no auth dependency)
# ---------------------------------------------------------------------------


def _login_page(error: str = "") -> str:
    error_html = f'<div class="alert alert-danger mt-3">{_esc(error)}</div>' if error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Ops Login</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
    crossorigin="anonymous">
</head>
<body class="bg-light d-flex justify-content-center align-items-center min-vh-100">
  <div class="card shadow-sm p-4" style="min-width:320px">
    <h5 class="card-title mb-3 text-center">🤖 Ops Login</h5>
    {error_html}
    <form method="post" action="/ops/login">
      <div class="mb-3">
        <label class="form-label">Username</label>
        <input type="text" name="username" class="form-control"
               autocomplete="username" required autofocus>
      </div>
      <div class="mb-3">
        <label class="form-label">Password</label>
        <input type="password" name="password" class="form-control"
               autocomplete="current-password" required>
      </div>
      <button type="submit" class="btn btn-primary w-100">Login</button>
    </form>
  </div>
</body>
</html>"""


@login_router.get("/login", response_class=HTMLResponse)
async def ops_login_get(request: Request) -> Response:
    ops_user = settings.ops_user
    ops_pass = settings.ops_pass
    signing_key = settings.ops_secret or ops_pass
    # If already authenticated via session cookie, skip the login page
    if ops_user:
        session = request.cookies.get(SESSION_COOKIE, "")
        if session and check_session_token(session, ops_user, signing_key):
            return RedirectResponse("/ops/monitoring", status_code=302)
    return HTMLResponse(_login_page())


@login_router.post("/login", response_class=HTMLResponse)
async def ops_login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
) -> Response:
    import posixpath

    ops_user = settings.ops_user
    ops_pass = settings.ops_pass
    signing_key = settings.ops_secret or ops_pass

    next_url = request.query_params.get("next", "/ops/monitoring")
    # Prevent open-redirect: normalise and allow only /ops/* paths
    next_url = posixpath.normpath(next_url)
    if not next_url.startswith("/ops/"):
        next_url = "/ops/monitoring"

    valid = bool(
        ops_user
        and secrets.compare_digest(username.encode(), ops_user.encode())
        and secrets.compare_digest(password.encode(), ops_pass.encode())
    )

    if not valid:
        return HTMLResponse(_login_page(error="Invalid username or password"), status_code=401)

    token = make_session_token(ops_user, signing_key)
    response: Response = RedirectResponse(next_url, status_code=303)
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=settings.env != "dev",
    )
    return response


@login_router.post("/logout")
async def ops_logout() -> Response:
    response: Response = RedirectResponse("/ops/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response

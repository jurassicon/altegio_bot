"""Ops-cabinet router – read-only HTML dashboard.

All routes are protected by require_ops_auth dependency.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import text

from altegio_bot.db import SessionLocal
from altegio_bot.settings import settings

from .auth import require_ops_auth

router = APIRouter(prefix='/ops', dependencies=[Depends(require_ops_auth)])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COMPANIES = {758285: 'Karlsruhe', 1271200: 'Rastatt'}

MARKETING_JOB_TYPES = (
    'review_3d',
    'repeat_10d',
    'comeback_3d',
    'newsletter_new_clients_monthly',
)


def _local_tz() -> ZoneInfo:
    try:
        return ZoneInfo(settings.ops_local_tz)
    except Exception:
        return ZoneInfo('Europe/Berlin')


def _fmt_dt(dt: datetime | None, tz: ZoneInfo | None = None) -> str:
    if dt is None:
        return ''
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    if tz:
        local = dt.astimezone(tz)
        return f'{utc_str} / {local.strftime("%Y-%m-%d %H:%M")} loc'
    return utc_str


def _ago(dt: datetime | None) -> str:
    """Human readable 'X minutes ago'."""
    if dt is None:
        return ''
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    secs = int(delta.total_seconds())
    if secs < 0:
        return 'in the future'
    if secs < 60:
        return f'{secs}s ago'
    if secs < 3600:
        return f'{secs // 60}m ago'
    if secs < 86400:
        return f'{secs // 3600}h ago'
    return f'{secs // 86400}d ago'


def _esc(s: Any) -> str:
    if s is None:
        return ''
    return (
        str(s)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
    )


def _status_badge(status: str | None) -> str:
    if not status:
        return ''
    colors = {
        'queued': 'secondary',
        'processing': 'primary',
        'running': 'primary',
        'done': 'success',
        'sent': 'success',
        'delivered': 'success',
        'read': 'info',
        'failed': 'danger',
        'canceled': 'warning',
        'received': 'secondary',
        'ignored': 'warning',
        'processed': 'success',
    }
    color = colors.get(status, 'secondary')
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
          <a class="nav-link" href="/ops/whatsapp/inbox">💬 WA Inbox</a>
        </li>
        <li class="nav-item">
          <a class="nav-link" href="/ops/optouts">🚫 Opt-outs</a>
        </li>
      </ul>
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


def _filter_form(action: str, fields: list[tuple[str, str, str, str]]) -> str:
    """
    fields: list of (name, label, type, current_value)
    type: text | select:opt1,opt2,...
    """
    parts = [
        f'<form method="get" action="{action}" '
        f'class="row g-2 mb-3 align-items-end">'
    ]
    for name, label, ftype, val in fields:
        parts.append('<div class="col-auto">')
        parts.append(f'<label class="form-label small mb-1">{_esc(label)}</label>')
        if ftype.startswith('select:'):
            options_str = ftype[7:]
            options = [''] + options_str.split(',')
            sel = f'<select name="{name}" class="form-select form-select-sm">'
            for o in options:
                selected = ' selected' if o == val else ''
                sel += f'<option value="{_esc(o)}"{selected}>{_esc(o) or "—all—"}</option>'
            sel += '</select>'
            parts.append(sel)
        else:
            parts.append(
                f'<input type="text" name="{name}" class="form-control form-control-sm"'
                f' value="{_esc(val)}" placeholder="{_esc(label)}">'
            )
        parts.append('</div>')
    parts.append(
        '<div class="col-auto">'
        '<button type="submit" class="btn btn-primary btn-sm">Filter</button>'
        '</div>'
    )
    parts.append('</form>')
    return '\n'.join(parts)


def _table(cols: list[str], rows: list[list[str]], row_classes: list[str] | None = None) -> str:
    th = ''.join(f'<th>{_esc(c)}</th>' for c in cols)
    body_rows = []
    for i, row in enumerate(rows):
        cls = ''
        if row_classes and i < len(row_classes):
            cls = f' class="{row_classes[i]}"'
        cells = ''.join(f'<td>{c}</td>' for c in row)
        body_rows.append(f'<tr{cls}>{cells}</tr>')
    body = '\n'.join(body_rows)
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
            f'</div></div></div>'
        )
    return '<div class="row g-2 mb-3">' + ''.join(cards) + '</div>'


_SAFE_IDENTIFIER_RE = __import__('re').compile(r'^[a-zA-Z0-9_]{1,64}$')


def _safe_identifier(value: str) -> bool:
    """Return True if value is safe to use as a SQL identifier/enum value."""
    return bool(_SAFE_IDENTIFIER_RE.match(value))


def _period_params(request: Request) -> tuple[datetime, datetime]:
    """Return (from_dt, to_dt) UTC based on ?period= / ?from_dt= / ?to_dt=."""
    now = datetime.now(timezone.utc)
    period = request.query_params.get('period', '24h')
    from_str = request.query_params.get('from_dt', '')
    to_str = request.query_params.get('to_dt', '')

    if from_str and to_str:
        try:
            from_dt = datetime.fromisoformat(from_str).replace(tzinfo=timezone.utc)
            to_dt = datetime.fromisoformat(to_str).replace(tzinfo=timezone.utc)
            return from_dt, to_dt
        except ValueError:
            pass

    if period == 'today':
        from_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return from_dt, now
    if period == '7d':
        return now - timedelta(days=7), now
    # default: 24h
    return now - timedelta(hours=24), now


# ---------------------------------------------------------------------------
# /ops/queue
# ---------------------------------------------------------------------------

@router.get('/queue', response_class=HTMLResponse)
async def ops_queue(request: Request) -> str:
    company_id = request.query_params.get('company_id', '')
    job_type = request.query_params.get('job_type', '')
    status_filter = request.query_params.get('status', '')
    period = request.query_params.get('period', '7d')
    from_dt, to_dt = _period_params(request)
    tz = _local_tz()

    async with SessionLocal() as session:
        # Metrics
        metrics_q = await session.execute(
            text("""
                SELECT
                  COUNT(*) FILTER (WHERE status='queued')       AS queued_cnt,
                  COUNT(*) FILTER (WHERE status='processing')   AS proc_cnt,
                  COUNT(*) FILTER (WHERE status='failed')       AS failed_cnt,
                  COUNT(*) FILTER (
                    WHERE status='processing'
                    AND locked_at < now() - (:stuck_min * interval '1 minute')
                  ) AS stuck_cnt
                FROM message_jobs
                WHERE status IN ('queued','processing','failed')
            """),
            {'stuck_min': settings.ops_stuck_minutes},
        )
        m = metrics_q.fetchone()
        queued_cnt = m.queued_cnt or 0
        proc_cnt = m.proc_cnt or 0
        failed_cnt = m.failed_cnt or 0
        stuck_cnt = m.stuck_cnt or 0

        # NOTE: `filters` contains only hardcoded SQL clauses with named
        # placeholders.  All user input is passed via `params` as bound
        # parameters to text(), so there is no SQL injection risk.
        filters = [
            'mj.status IN (:s_queued, :s_proc, :s_failed)',
            'mj.run_at >= :from_dt AND mj.run_at < :to_dt',
        ]
        params: dict[str, Any] = {
            's_queued': 'queued',
            's_proc': 'processing',
            's_failed': 'failed',
            'from_dt': from_dt,
            'to_dt': to_dt,
            'stuck_min': settings.ops_stuck_minutes,
        }

        if company_id:
            filters.append('mj.company_id = :company_id')
            params['company_id'] = int(company_id)
        if job_type and _safe_identifier(job_type):
            filters.append('mj.job_type = :job_type')
            params['job_type'] = job_type
        if status_filter and _safe_identifier(status_filter):
            filters.append('mj.status = :status_filter')
            params['status_filter'] = status_filter

        where = ' AND '.join(filters)
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
                ORDER BY
                  CASE WHEN mj.status='queued' THEN mj.run_at END ASC NULLS LAST,
                  CASE WHEN mj.status IN ('processing','failed')
                       THEN mj.updated_at END DESC NULLS LAST
                LIMIT 200
            """),
            params,
        )
        jobs = rows_q.fetchall()

    metrics_html = _metric_cards([
        ('Queued', queued_cnt, 'secondary'),
        ('Processing', proc_cnt, 'primary'),
        ('Failed', failed_cnt, 'danger'),
        ('Stuck', stuck_cnt, 'warning' if stuck_cnt == 0 else 'danger'),
    ])

    form = _filter_form('/ops/queue', [
        ('company_id', 'Company', 'select:758285,1271200', company_id),
        ('job_type', 'Job type', 'text', job_type),
        ('status', 'Status', 'select:queued,processing,failed', status_filter),
        ('period', 'Period', 'select:24h,today,7d', period),
    ])

    cols = ['ID', 'Company', 'Type', 'Status', 'Run At', 'Attempts', 'Client', 'Error']
    table_rows = []
    row_classes = []
    for j in jobs:
        is_stuck = (
            j.status == 'processing'
            and j.locked_at is not None
            and (datetime.now(timezone.utc) - j.locked_at.replace(tzinfo=timezone.utc)).total_seconds()
            > settings.ops_stuck_minutes * 60
        )
        company_name = COMPANIES.get(j.company_id, str(j.company_id))
        client_info = ''
        if j.display_name or j.phone_e164:
            client_info = f'{_esc(j.display_name or "")} {_esc(j.phone_e164 or "")}'.strip()
        table_rows.append([
            f'<a href="/ops/job/{j.id}">{j.id}</a>',
            _esc(company_name),
            _esc(j.job_type),
            _status_badge(j.status),
            _esc(_fmt_dt(j.run_at, tz)),
            f'{j.attempts}/{j.max_attempts}',
            client_info,
            f'<span class="text-danger small">{_esc((j.last_error or "")[:80])}</span>',
        ])
        row_classes.append('stuck' if is_stuck else '')

    body = f"""
<h4>📋 Queue</h4>
{metrics_html}
{form}
<p class="text-muted small">Showing {len(jobs)} rows · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""
    return _page('Queue', body)


# ---------------------------------------------------------------------------
# /ops/history
# ---------------------------------------------------------------------------

@router.get('/history', response_class=HTMLResponse)
async def ops_history(request: Request) -> str:
    company_id = request.query_params.get('company_id', '')
    template_code = request.query_params.get('template_code', '')
    phone = request.query_params.get('phone_e164', '')
    provider_msg_id = request.query_params.get('provider_message_id', '')
    period = request.query_params.get('period', '24h')
    from_dt, to_dt = _period_params(request)
    tz = _local_tz()

    async with SessionLocal() as session:
        # NOTE: `filters` contains only hardcoded SQL clauses; user input
        # is exclusively passed as bound parameters via `params`.
        filters = [
            'om.created_at >= :from_dt AND om.created_at < :to_dt',
        ]
        params: dict[str, Any] = {
            'from_dt': from_dt,
            'to_dt': to_dt,
        }

        if company_id:
            filters.append('om.company_id = :company_id')
            params['company_id'] = int(company_id)
        if template_code and _safe_identifier(template_code):
            filters.append('om.template_code = :template_code')
            params['template_code'] = template_code
        if phone:
            filters.append('om.phone_e164 ILIKE :phone')
            params['phone'] = f'%{phone}%'
        if provider_msg_id:
            filters.append('om.provider_message_id ILIKE :pmid')
            params['pmid'] = f'%{provider_msg_id}%'

        where = ' AND '.join(filters)

        rows_q = await session.execute(
            text(f"""
                SELECT
                  om.id,
                  COALESCE(om.sent_at, om.scheduled_at) AS ts,
                  om.company_id,
                  om.phone_e164,
                  om.template_code,
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
                ORDER BY om.created_at DESC
                LIMIT 200
            """),
            params,
        )
        rows = rows_q.fetchall()

    form = _filter_form('/ops/history', [
        ('company_id', 'Company', 'select:758285,1271200', company_id),
        ('template_code', 'Template', 'text', template_code),
        ('phone_e164', 'Phone', 'text', phone),
        ('provider_message_id', 'Provider Msg ID', 'text', provider_msg_id),
        ('period', 'Period', 'select:24h,today,7d', period),
    ])

    cols = [
        'ID', 'Sent At', 'Company', 'Phone', 'Template',
        'Status', 'WA Delivery', 'Provider Msg ID', 'Error',
    ]
    table_rows = []
    row_classes = []
    for r in rows:
        company_name = COMPANIES.get(r.company_id, str(r.company_id))
        wa_delivery = _status_badge(r.wa_status) if r.wa_status else ''
        if r.wa_err_code:
            wa_delivery += f' <span class="text-danger small">{_esc(r.wa_err_code)}</span>'
        row_class = 'warn' if r.status == 'failed' else ''
        table_rows.append([
            f'<a href="/ops/outbox/{r.id}">{r.id}</a>',
            _esc(_fmt_dt(r.ts, tz)),
            _esc(company_name),
            _esc(r.phone_e164),
            _esc(r.template_code),
            _status_badge(r.status),
            wa_delivery,
            _esc((r.provider_message_id or '')[:40]),
            f'<span class="text-danger small">{_esc((r.error or "")[:80])}</span>',
        ])
        row_classes.append(row_class)

    body = f"""
<h4>📨 Outbox History</h4>
{form}
<p class="text-muted small">Showing {len(rows)} rows · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""
    return _page('History', body)


# ---------------------------------------------------------------------------
# /ops/job/{job_id}
# ---------------------------------------------------------------------------

@router.get('/job/{job_id}', response_class=HTMLResponse)
async def ops_job(job_id: int) -> str:
    tz = _local_tz()

    async with SessionLocal() as session:
        job_q = await session.execute(
            text("""
                SELECT
                  mj.*,
                  c.display_name, c.phone_e164
                FROM message_jobs mj
                LEFT JOIN clients c ON c.id = mj.client_id
                WHERE mj.id = :job_id
            """),
            {'job_id': job_id},
        )
        job = job_q.fetchone()

        if job is None:
            return _page('Job Not Found', '<div class="alert alert-danger">Job not found.</div>')

        outbox_q = await session.execute(
            text("""
                SELECT
                  om.id, om.status, om.phone_e164, om.template_code,
                  COALESCE(om.sent_at, om.scheduled_at) AS ts,
                  om.provider_message_id, om.error,
                  ws.wa_status
                FROM outbox_messages om
                LEFT JOIN LATERAL (
                  SELECT
                    payload #>> '{entry,0,changes,0,value,statuses,0,status}' AS wa_status
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
            {'job_id': job_id},
        )
        outbox_rows = outbox_q.fetchall()

    is_stuck = (
        job.status == 'processing'
        and job.locked_at is not None
        and (datetime.now(timezone.utc) - job.locked_at.replace(tzinfo=timezone.utc)).total_seconds()
        > settings.ops_stuck_minutes * 60
    )
    company_name = COMPANIES.get(job.company_id, str(job.company_id))

    payload_json = ''
    try:
        payload_json = json.dumps(job.payload, indent=2, ensure_ascii=False)
    except Exception:
        payload_json = str(job.payload)

    stuck_badge = (
        ' <span class="badge bg-danger">STUCK</span>'
        if is_stuck else ''
    )

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
        {_esc(job.display_name or '')} {_esc(job.phone_e164 or '')}
      </dd>
      <dt class="col-sm-3">Record ID</dt>
      <dd class="col-sm-9">{_esc(str(job.record_id or ''))}</dd>
      <dt class="col-sm-3">Last Error</dt>
      <dd class="col-sm-9 text-danger">{_esc(job.last_error or '')}</dd>
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

    cols = ['ID', 'Status', 'WA Delivery', 'Phone', 'Template', 'Sent At', 'Error']
    outbox_table_rows = []
    for r in outbox_rows:
        outbox_table_rows.append([
            f'<a href="/ops/outbox/{r.id}">{r.id}</a>',
            _status_badge(r.status),
            _status_badge(r.wa_status) if r.wa_status else '',
            _esc(r.phone_e164),
            _esc(r.template_code),
            _esc(_fmt_dt(r.ts, tz)),
            f'<span class="text-danger small">{_esc((r.error or "")[:80])}</span>',
        ])

    outbox_section = f"""
<h5>Related Outbox Messages (last 20)</h5>
{_table(cols, outbox_table_rows) if outbox_table_rows else '<p class="text-muted">No outbox messages.</p>'}
"""

    body = f"""
<h4>📋 Job #{job_id}</h4>
{details}
{outbox_section}
"""
    return _page(f'Job {job_id}', body)


# ---------------------------------------------------------------------------
# /ops/outbox/{outbox_id}
# ---------------------------------------------------------------------------

@router.get('/outbox/{outbox_id}', response_class=HTMLResponse)
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
            {'oid': outbox_id},
        )
        om = om_q.fetchone()

        if om is None:
            return _page('Not Found', '<div class="alert alert-danger">Not found.</div>')

        delivery_q = await session.execute(
            text("""
                SELECT
                  we.id,
                  we.received_at,
                  payload #>> '{entry,0,changes,0,value,statuses,0,status}' AS wa_status,
                  payload #>> '{entry,0,changes,0,value,statuses,0,errors,0,code}' AS err_code,
                  payload #>> '{entry,0,changes,0,value,statuses,0,errors,0,error_data,details}' AS err_details
                FROM whatsapp_events we
                WHERE
                  payload #>> '{entry,0,changes,0,value,statuses,0,id}'
                    = :msg_id
                  AND :msg_id IS NOT NULL
                ORDER BY we.received_at DESC
                LIMIT 20
            """),
            {'msg_id': om.provider_message_id},
        )
        deliveries = delivery_q.fetchall()

    company_name = COMPANIES.get(om.company_id, str(om.company_id))

    body_text = ''
    try:
        body_text = om.body[:500] if om.body else ''
    except Exception:
        pass

    meta_json = ''
    try:
        meta_json = json.dumps(om.meta, indent=2, ensure_ascii=False)
    except Exception:
        meta_json = str(om.meta)

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
      <dt class="col-sm-3">Template</dt>
      <dd class="col-sm-9">{_esc(om.template_code)}</dd>
      <dt class="col-sm-3">Language</dt>
      <dd class="col-sm-9">{_esc(om.language)}</dd>
      <dt class="col-sm-3">Sender</dt>
      <dd class="col-sm-9">{_esc(om.sender_phone or str(om.sender_id or ''))}</dd>
      <dt class="col-sm-3">Status</dt>
      <dd class="col-sm-9">{_status_badge(om.status)}</dd>
      <dt class="col-sm-3">Provider Msg ID</dt>
      <dd class="col-sm-9"><code>{_esc(om.provider_message_id or '')}</code></dd>
      <dt class="col-sm-3">Scheduled At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(om.scheduled_at, tz))}</dd>
      <dt class="col-sm-3">Sent At</dt>
      <dd class="col-sm-9">{_esc(_fmt_dt(om.sent_at, tz))}</dd>
      <dt class="col-sm-3">Error</dt>
      <dd class="col-sm-9 text-danger">{_esc(om.error or '')}</dd>
      <dt class="col-sm-3">Job ID</dt>
      <dd class="col-sm-9">
        {f'<a href="/ops/job/{om.job_id}">{om.job_id}</a>' if om.job_id else ''}
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

    if meta_json and meta_json != '{}':
        details += f"""
<div class="card mb-3">
  <div class="card-header">Meta</div>
  <div class="card-body"><pre>{_esc(meta_json)}</pre></div>
</div>
"""

    cols = ['WA Status', 'Received At', 'Err Code', 'Err Details']
    delivery_rows = []
    for d in deliveries:
        delivery_rows.append([
            _status_badge(d.wa_status) if d.wa_status else '',
            _esc(_fmt_dt(d.received_at, tz)),
            _esc(d.err_code or ''),
            _esc((d.err_details or '')[:120]),
        ])

    delivery_section = f"""
<h5>WhatsApp Delivery Statuses</h5>
{_table(cols, delivery_rows) if delivery_rows else '<p class="text-muted">No delivery events.</p>'}
"""

    body = f"""
<h4>📨 Outbox #{outbox_id}</h4>
{details}
{delivery_section}
"""
    return _page(f'Outbox {outbox_id}', body)


# ---------------------------------------------------------------------------
# /ops/whatsapp/inbox
# ---------------------------------------------------------------------------

def _detect_cmd(payload: dict | None) -> str:
    if not payload:
        return ''
    try:
        entry0 = (payload.get('entry') or [])[0]
        changes0 = (entry0.get('changes') or [])[0]
        value = changes0.get('value') or {}
        messages = value.get('messages') or []
        if not messages:
            return ''
        body = (messages[0].get('text') or {}).get('body') or ''
        body_up = body.strip().upper()
        if body_up == 'STOP':
            return 'stop'
        if body_up == 'START':
            return 'start'
    except Exception:
        pass
    return ''


@router.get('/whatsapp/inbox', response_class=HTMLResponse)
async def ops_wa_inbox(request: Request) -> str:
    pni_filter = request.query_params.get('pni', '')
    from_filter = request.query_params.get('wa_from', '')
    status_filter = request.query_params.get('status', '')
    only_cmds = request.query_params.get('only_commands', '')
    period = request.query_params.get('period', '24h')
    from_dt, to_dt = _period_params(request)
    tz = _local_tz()

    async with SessionLocal() as session:
        # NOTE: all user inputs go into `params` as bound parameters;
        # `filters` contains only hardcoded SQL clauses.
        filters = ['we.received_at >= :from_dt AND we.received_at < :to_dt']
        params: dict[str, Any] = {
            'from_dt': from_dt,
            'to_dt': to_dt,
        }

        if pni_filter:
            filters.append(
                "we.payload #>> '{entry,0,changes,0,value,metadata,phone_number_id}' "
                '= :pni'
            )
            params['pni'] = pni_filter
        if from_filter:
            filters.append(
                "we.payload #>> '{entry,0,changes,0,value,messages,0,from}' "
                'ILIKE :wa_from'
            )
            params['wa_from'] = f'%{from_filter}%'
        if status_filter and _safe_identifier(status_filter):
            filters.append('we.status = :status_filter')
            params['status_filter'] = status_filter
        if only_cmds == '1':
            filters.append(
                "upper(trim(we.payload #>> '{entry,0,changes,0,value,messages,0,text,body}')) "
                "IN ('STOP','START')"
            )

        where = ' AND '.join(filters)
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

    def _pni_from_payload(p: dict) -> str:
        try:
            return (
                p['entry'][0]['changes'][0]['value']['metadata']['phone_number_id']
            )
        except Exception:
            return ''

    def _from_from_payload(p: dict) -> str:
        try:
            return p['entry'][0]['changes'][0]['value']['messages'][0]['from']
        except Exception:
            return ''

    def _body_from_payload(p: dict) -> str:
        try:
            return p['entry'][0]['changes'][0]['value']['messages'][0]['text']['body']
        except Exception:
            return ''

    form = _filter_form('/ops/whatsapp/inbox', [
        ('pni', 'Phone Number ID', 'text', pni_filter),
        ('wa_from', 'From (phone)', 'text', from_filter),
        ('status', 'Status', 'select:received,ignored,processed,failed', status_filter),
        ('only_commands', 'Only STOP/START', 'select:,1', only_cmds),
        ('period', 'Period', 'select:24h,today,7d', period),
    ])

    cols = ['ID', 'Received At', 'PNI', 'From', 'Body', 'Cmd', 'Status', 'Error']
    table_rows = []
    row_classes = []
    for r in rows:
        payload = r.payload or {}
        pni = _pni_from_payload(payload)
        wa_from = _from_from_payload(payload)
        msg_body = _body_from_payload(payload)
        cmd = _detect_cmd(payload)
        cmd_badge = ''
        if cmd == 'stop':
            cmd_badge = '<span class="badge bg-danger">STOP</span>'
        elif cmd == 'start':
            cmd_badge = '<span class="badge bg-success">START</span>'
        row_class = 'warn' if r.status == 'ignored' else (
            'stuck' if r.status == 'failed' else ''
        )
        table_rows.append([
            str(r.id),
            _esc(_fmt_dt(r.received_at, tz)),
            _esc(pni[:30]),
            _esc(wa_from),
            _esc((msg_body or '')[:60]),
            cmd_badge,
            _status_badge(r.status),
            f'<span class="small text-muted">{_esc((r.error or "")[:80])}</span>',
        ])
        row_classes.append(row_class)

    body = f"""
<h4>💬 WhatsApp Inbox</h4>
{form}
<p class="text-muted small">Showing {len(rows)} rows · period {_esc(period)}</p>
{_table(cols, table_rows, row_classes)}
"""
    return _page('WA Inbox', body)


# ---------------------------------------------------------------------------
# /ops/optouts
# ---------------------------------------------------------------------------

@router.get('/optouts', response_class=HTMLResponse)
async def ops_optouts(request: Request) -> str:
    company_id = request.query_params.get('company_id', '')
    phone = request.query_params.get('phone_e164', '')
    period = request.query_params.get('period', '')
    from_dt_raw = request.query_params.get('from_dt', '')
    to_dt_raw = request.query_params.get('to_dt', '')
    tz = _local_tz()

    async with SessionLocal() as session:
        # NOTE: `filters` contains only hardcoded SQL clauses;
        # user input is passed exclusively via bound `params`.
        filters = ['c.wa_opted_out = true']
        params: dict[str, Any] = {}

        if company_id:
            filters.append('c.company_id = :company_id')
            params['company_id'] = int(company_id)
        if phone:
            filters.append('c.phone_e164 ILIKE :phone')
            params['phone'] = f'%{phone}%'

        now = datetime.now(timezone.utc)
        if period == 'today':
            params['from_dt'] = now.replace(hour=0, minute=0, second=0, microsecond=0)
            params['to_dt'] = now
            filters.append('c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt')
        elif period == '7d':
            params['from_dt'] = now - timedelta(days=7)
            params['to_dt'] = now
            filters.append('c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt')
        elif from_dt_raw and to_dt_raw:
            try:
                params['from_dt'] = datetime.fromisoformat(from_dt_raw).replace(
                    tzinfo=timezone.utc
                )
                params['to_dt'] = datetime.fromisoformat(to_dt_raw).replace(
                    tzinfo=timezone.utc
                )
                filters.append(
                    'c.wa_opted_out_at >= :from_dt AND c.wa_opted_out_at < :to_dt'
                )
            except ValueError:
                pass

        where = ' AND '.join(filters)
        rows_q = await session.execute(
            text(f"""
                SELECT
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
                ORDER BY c.wa_opted_out_at DESC NULLS LAST
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
        counts_html += (
            f'<li class="list-inline-item">'
            f'<span class="badge bg-danger">{cname}: {cnt.cnt}</span>'
            f'</li>'
        )
    counts_html += '</ul>'

    form = _filter_form('/ops/optouts', [
        ('company_id', 'Company', 'select:758285,1271200', company_id),
        ('phone_e164', 'Phone', 'text', phone),
        ('period', 'Period', 'select:,today,7d', period),
    ])

    cols = [
        'ID', 'Company', 'Name', 'Phone', 'Opted Out At', 'Reason', 'Last STOP Event',
    ]
    table_rows = []
    for r in rows:
        company_name = COMPANIES.get(r.company_id, str(r.company_id))
        stop_link = ''
        if r.last_stop_event_id:
            stop_link = (
                f'<a href="/ops/whatsapp/inbox">#{r.last_stop_event_id}</a>'
            )
        table_rows.append([
            str(r.id),
            _esc(company_name),
            _esc(r.display_name or ''),
            _esc(r.phone_e164 or ''),
            _esc(_fmt_dt(r.wa_opted_out_at, tz)),
            _esc(r.wa_opt_out_reason or ''),
            stop_link,
        ])

    body = f"""
<h4>🚫 Opt-outs</h4>
{counts_html}
{form}
<p class="text-muted small">Showing {len(rows)} opted-out clients</p>
{_table(cols, table_rows)}
"""
    return _page('Opt-outs', body)


# ---------------------------------------------------------------------------
# /ops/monitoring
# ---------------------------------------------------------------------------

@router.get('/monitoring', response_class=HTMLResponse)
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
                SELECT
                  MAX(received_at) AS last_altegio,
                  COUNT(*) FILTER (WHERE received_at >= :w5)   AS altegio_5m,
                  COUNT(*) FILTER (WHERE received_at >= :w15)  AS altegio_15m,
                  COUNT(*) FILTER (WHERE received_at >= :w1h)  AS altegio_1h
                FROM altegio_events
            """),
            {'w5': window_5m, 'w15': window_15m, 'w1h': window_1h},
        )
        altegio_ing = ingress_q.fetchone()

        wa_ingress_q = await session.execute(
            text("""
                SELECT
                  MAX(received_at) AS last_wa,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w5 AND status != 'ignored'
                  ) AS wa_5m,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w5 AND status = 'ignored'
                  ) AS wa_ignored_5m,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w15 AND status != 'ignored'
                  ) AS wa_15m,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w15 AND status = 'ignored'
                  ) AS wa_ignored_15m,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w1h AND status != 'ignored'
                  ) AS wa_1h,
                  COUNT(*) FILTER (
                    WHERE received_at >= :w1h AND status = 'ignored'
                  ) AS wa_ignored_1h
                FROM whatsapp_events
            """),
            {'w5': window_5m, 'w15': window_15m, 'w1h': window_1h},
        )
        wa_ing = wa_ingress_q.fetchone()

        # --- Queue Health ---
        queue_q = await session.execute(
            text("""
                SELECT
                  company_id,
                  COUNT(*) FILTER (WHERE status='queued')       AS queued_cnt,
                  COUNT(*) FILTER (WHERE status='processing')   AS proc_cnt,
                  COUNT(*) FILTER (
                    WHERE status='failed' AND updated_at >= :w24h
                  ) AS failed_24h,
                  COUNT(*) FILTER (
                    WHERE status='processing'
                    AND locked_at < :stuck_ts
                  ) AS stuck_cnt
                FROM message_jobs
                GROUP BY company_id
                ORDER BY company_id
            """),
            {
                'w24h': window_24h,
                'stuck_ts': now - stuck_threshold,
            },
        )
        queue_rows = queue_q.fetchall()

        queue_total_q = await session.execute(
            text("""
                SELECT
                  COUNT(*) FILTER (WHERE status='queued')       AS queued_cnt,
                  COUNT(*) FILTER (WHERE status='processing')   AS proc_cnt,
                  COUNT(*) FILTER (
                    WHERE status='failed' AND updated_at >= :w24h
                  ) AS failed_24h,
                  COUNT(*) FILTER (
                    WHERE status='processing'
                    AND locked_at < :stuck_ts
                  ) AS stuck_cnt
                FROM message_jobs
            """),
            {
                'w24h': window_24h,
                'stuck_ts': now - stuck_threshold,
            },
        )
        qt = queue_total_q.fetchone()

        # --- Outbox Health ---
        outbox_q = await session.execute(
            text("""
                SELECT
                  COUNT(*) FILTER (WHERE status='sent') AS sent_cnt,
                  COUNT(*) FILTER (WHERE status='failed') AS failed_cnt,
                  COUNT(*) AS total_cnt
                FROM outbox_messages
                WHERE created_at >= :w24h
            """),
            {'w24h': window_24h},
        )
        ob = outbox_q.fetchone()

        # Top outbox errors
        top_errors_q = await session.execute(
            text("""
                SELECT
                  COALESCE(error, 'unknown') AS err,
                  COUNT(*) AS cnt
                FROM outbox_messages
                WHERE status='failed' AND created_at >= :w24h
                GROUP BY err
                ORDER BY cnt DESC
                LIMIT 5
            """),
            {'w24h': window_24h},
        )
        top_errors = top_errors_q.fetchall()

        # Top WA delivery errors
        wa_errors_q = await session.execute(
            text("""
                SELECT
                  payload #>> '{entry,0,changes,0,value,statuses,0,errors,0,code}' AS err_code,
                  COUNT(*) AS cnt
                FROM whatsapp_events
                WHERE
                  received_at >= :w24h
                  AND payload #>> '{entry,0,changes,0,value,statuses,0,status}' = 'failed'
                GROUP BY err_code
                ORDER BY cnt DESC
                LIMIT 5
            """),
            {'w24h': window_24h},
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
                SELECT
                  we.id,
                  we.received_at,
                  we.status,
                  upper(trim(
                    we.payload #>> '{entry,0,changes,0,value,messages,0,text,body}'
                  )) AS cmd,
                  we.payload #>> '{entry,0,changes,0,value,messages,0,from}' AS wa_from
                FROM whatsapp_events we
                WHERE
                  upper(trim(
                    we.payload #>> '{entry,0,changes,0,value,messages,0,text,body}'
                  )) IN ('STOP', 'START')
                ORDER BY we.received_at DESC
                LIMIT 20
            """)
        )
        last_cmds = last_cmds_q.fetchall()

    # ---- Build HTML ----

    # 1) API Health
    health_html = f"""
<div class="card mb-3 border-success">
  <div class="card-header">🟢 API Health</div>
  <div class="card-body">
    <p class="mb-1"><strong>Status:</strong> Running ✓</p>
    <p class="mb-0 text-muted small">Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S')}</p>
  </div>
</div>
"""

    # 2) Webhook Ingress
    altegio_last = altegio_ing.last_altegio
    wa_last = wa_ing.last_wa
    altegio_warn = (
        altegio_last is None
        or (now - altegio_last.replace(tzinfo=timezone.utc)).total_seconds() > 15 * 60
    )
    wa_warn = (
        wa_last is None
        or (now - wa_last.replace(tzinfo=timezone.utc)).total_seconds() > 15 * 60
    )

    ingress_html = f"""
<div class="card mb-3 {'border-warning' if altegio_warn or wa_warn else 'border-success'}">
  <div class="card-header">📡 Webhook Ingress</div>
  <div class="card-body">
    <table class="table table-sm mb-0">
      <thead><tr>
        <th>Source</th><th>Last Event</th>
        <th>5m</th><th>15m</th><th>1h</th>
      </tr></thead>
      <tbody>
        <tr class="{'warn' if altegio_warn else ''}">
          <td>Altegio</td>
          <td>{_esc(_fmt_dt(altegio_last, tz))} ({_esc(_ago(altegio_last))})</td>
          <td>{altegio_ing.altegio_5m}</td>
          <td>{altegio_ing.altegio_15m}</td>
          <td>{altegio_ing.altegio_1h}</td>
        </tr>
        <tr class="{'warn' if wa_warn else ''}">
          <td>WhatsApp</td>
          <td>{_esc(_fmt_dt(wa_last, tz))} ({_esc(_ago(wa_last))})</td>
          <td>{wa_ing.wa_5m} (+{wa_ing.wa_ignored_5m} ign)</td>
          <td>{wa_ing.wa_15m} (+{wa_ing.wa_ignored_15m} ign)</td>
          <td>{wa_ing.wa_1h} (+{wa_ing.wa_ignored_1h} ign)</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>
"""

    # 3) Queue Health
    total_stuck = qt.stuck_cnt or 0
    queue_border = 'border-danger' if total_stuck > 0 or (qt.failed_24h or 0) > 0 else 'border-success'
    queue_rows_html = ''
    for qr in queue_rows:
        cname = COMPANIES.get(qr.company_id, str(qr.company_id))
        queue_rows_html += (
            f'<tr>'
            f'<td>{_esc(cname)}</td>'
            f'<td>{qr.queued_cnt}</td>'
            f'<td>{qr.proc_cnt}</td>'
            f'<td>{qr.failed_24h}</td>'
            f'<td class="{"text-danger fw-bold" if qr.stuck_cnt > 0 else ""}">'
            f'{qr.stuck_cnt}</td>'
            f'</tr>'
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
          <td class="{'text-danger' if total_stuck > 0 else ''}">{total_stuck}</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>
"""

    # 4) Outbox Health
    ob_total = ob.total_cnt or 0
    ob_sent = ob.sent_cnt or 0
    ob_failed = ob.failed_cnt or 0
    fail_rate = round(ob_failed / ob_total * 100, 1) if ob_total > 0 else 0.0
    ob_warn = ob_failed > settings.ops_failed_warning_threshold
    ob_border = 'border-danger' if ob_warn else 'border-success'

    top_errors_html = ''
    for e in top_errors:
        top_errors_html += (
            f'<li class="list-group-item d-flex justify-content-between">'
            f'<span class="text-danger small">{_esc(e.err[:80])}</span>'
            f'<span class="badge bg-danger">{e.cnt}</span>'
            f'</li>'
        )

    wa_errors_html = ''
    for e in wa_errors:
        wa_errors_html += (
            f'<li class="list-group-item d-flex justify-content-between">'
            f'<span class="text-danger small">{_esc(e.err_code or "unknown")}</span>'
            f'<span class="badge bg-warning text-dark">{e.cnt}</span>'
            f'</li>'
        )

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
        <span class="badge bg-{'danger' if ob_warn else 'info'} fs-6">
          Fail rate: {fail_rate}%
        </span>
      </div>
    </div>
    <div class="row g-3">
      <div class="col-md-6">
        <p class="fw-bold mb-1">Top Outbox Errors</p>
        <ul class="list-group list-group-flush">
          {top_errors_html or '<li class="list-group-item text-muted">None</li>'}
        </ul>
      </div>
      <div class="col-md-6">
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
    optout_rows_html = ''
    for o in optout_rows:
        cname = COMPANIES.get(o.company_id, str(o.company_id))
        optout_rows_html += (
            f'<li class="list-inline-item">'
            f'<span class="badge bg-danger">{_esc(cname)}: {o.cnt}</span>'
            f'</li>'
        )

    last_cmds_rows = []
    for cmd in last_cmds:
        badge = (
            '<span class="badge bg-danger">STOP</span>'
            if cmd.cmd == 'STOP'
            else '<span class="badge bg-success">START</span>'
        )
        last_cmds_rows.append([
            str(cmd.id),
            _esc(_fmt_dt(cmd.received_at, tz)),
            _esc(cmd.wa_from or ''),
            badge,
            _status_badge(cmd.status),
        ])

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
    {_table(['ID','Received At','From','Cmd','Status'], last_cmds_rows)
     if last_cmds_rows else '<p class="text-muted">No commands yet.</p>'}
  </div>
</div>
"""

    warnings = []
    if altegio_warn:
        warnings.append('⚠️ Last Altegio webhook > 15 minutes ago')
    if wa_warn:
        warnings.append('⚠️ Last WhatsApp webhook > 15 minutes ago')
    if total_stuck > 0:
        warnings.append(f'⚠️ {total_stuck} stuck processing job(s)')
    if ob_warn:
        warnings.append(
            f'⚠️ {ob_failed} failed outbox messages in last 24h '
            f'(threshold: {settings.ops_failed_warning_threshold})'
        )

    warnings_html = ''
    if warnings:
        items = ''.join(f'<li>{w}</li>' for w in warnings)
        warnings_html = (
            f'<div class="alert alert-warning"><ul class="mb-0">{items}</ul></div>'
        )

    body = f"""
<h4>📊 Monitoring</h4>
<p class="text-muted small">
  Refreshed: {now.strftime('%Y-%m-%d %H:%M:%S UTC')} /
  {now.astimezone(tz).strftime('%Y-%m-%d %H:%M')} local
  &nbsp;·&nbsp;
  <a href="/ops/monitoring">🔄 Refresh</a>
</p>
{warnings_html}
{health_html}
{ingress_html}
{queue_html}
{outbox_html}
{optout_html}
"""
    return _page('Monitoring', body)


# ---------------------------------------------------------------------------
# /ops/ → redirect to monitoring
# ---------------------------------------------------------------------------

@router.get('', response_class=HTMLResponse)
@router.get('/', response_class=HTMLResponse)
async def ops_index() -> HTMLResponse:
    return HTMLResponse(
        '<meta http-equiv="refresh" content="0;url=/ops/monitoring">',
        status_code=302,
        headers={'Location': '/ops/monitoring'},
    )

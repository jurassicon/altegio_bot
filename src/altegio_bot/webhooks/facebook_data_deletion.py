"""Facebook / Meta data-deletion endpoints.

Meta requires every app that uses Facebook Login (or any Meta product) to
provide one of:

* A **Data Deletion Callback URL** – Meta POSTs a ``signed_request`` here
  when a user asks Facebook to delete all data your app holds about them.
* A **Data Deletion Instructions URL** – a page that explains how users can
  contact you to have their data deleted.

This module provides both:

* ``POST /webhook/facebook/data-deletion`` – the callback endpoint.
* ``GET  /data-deletion``                  – the instructions page.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
from typing import Any

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INSTRUCTIONS_HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Удаление данных / Data Deletion</title>
  <style>
    body {{ font-family: sans-serif; max-width: 680px; margin: 40px auto;
           padding: 0 16px; color: #222; line-height: 1.6; }}
    h1   {{ font-size: 1.4em; }}
    p    {{ margin: .8em 0; }}
    a    {{ color: #1a73e8; }}
  </style>
</head>
<body>
  <h1>Запрос на удаление данных / Data Deletion Request</h1>

  <p><strong>RU:</strong> Если вы хотите удалить данные, которые наше
  приложение получило через Facebook / Meta, отправьте запрос по
  электронной почте: <a href="mailto:info@kitilash.com">info@kitilash.com</a>.
  Укажите ваш Facebook-идентификатор или адрес электронной почты,
  связанный с аккаунтом. Мы обработаем запрос в течение 30 дней.</p>

  <p><strong>EN:</strong> To request deletion of any data our application
  received via Facebook / Meta, please e-mail us at
  <a href="mailto:info@kitilash.com">info@kitilash.com</a>.
  Include your Facebook user ID or the e-mail address linked to your account.
  We will process your request within 30 days.</p>

  <p>
    Политика конфиденциальности / Privacy Policy:
    <a href="https://www.kitilash.com/privacy-policy/">
      https://www.kitilash.com/privacy-policy/
    </a>
  </p>
</body>
</html>
"""


def _parse_signed_request(
    signed_request: str,
    app_secret: str,
) -> dict[str, Any]:
    """Decode and verify a Meta ``signed_request`` parameter.

    The format is ``<base64url_signature>.<base64url_payload>`` where the
    signature is HMAC-SHA256 over the raw payload bytes using the app secret.

    Raises :class:`ValueError` on any parse / verification error.
    """
    try:
        sig_b64, payload_b64 = signed_request.split('.', 1)
    except ValueError:
        raise ValueError('signed_request must contain exactly one "."')

    # base64url → bytes (add padding as needed)
    def _b64_decode(value: str) -> bytes:
        padded = value + '=' * (4 - len(value) % 4)
        return base64.urlsafe_b64decode(padded)

    try:
        sig_bytes = _b64_decode(sig_b64)
        payload_bytes = _b64_decode(payload_b64)
    except Exception as exc:
        raise ValueError(f'base64 decode failed: {exc}') from exc

    expected = hmac.new(
        app_secret.encode('utf-8'),
        payload_bytes,
        hashlib.sha256,
    ).digest()

    if not hmac.compare_digest(sig_bytes, expected):
        raise ValueError('Signature verification failed')

    try:
        return json.loads(payload_bytes.decode('utf-8'))
    except Exception as exc:
        raise ValueError(f'payload JSON decode failed: {exc}') from exc


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get('/data-deletion', response_class=HTMLResponse)
async def data_deletion_instructions() -> HTMLResponse:
    """Human-readable data-deletion instructions page.

    Register this URL as the *Data Deletion Instructions URL* in your
    Facebook app settings.
    """
    return HTMLResponse(content=_INSTRUCTIONS_HTML, status_code=200)


@router.post('/webhook/facebook/data-deletion')
async def facebook_data_deletion_callback(
    signed_request: str = Form(...),
) -> JSONResponse:
    """Meta data-deletion callback endpoint.

    Meta POSTs a ``signed_request`` form field here when a user asks
    Facebook to delete all data your app holds about them.

    Returns the JSON shape required by Meta:
    ``{"url": "<status_url>", "confirmation_code": "<unique_code>"}``
    """
    app_secret = settings.meta_app_secret
    if not app_secret:
        logger.error(
            'META_APP_SECRET is not configured; '
            'cannot verify data-deletion request'
        )
        raise HTTPException(
            status_code=500,
            detail='App secret not configured',
        )

    try:
        payload = _parse_signed_request(signed_request, app_secret)
    except ValueError as exc:
        logger.warning('Invalid signed_request: %s', exc)
        raise HTTPException(status_code=400, detail=str(exc))

    user_id: str | None = payload.get('user_id')
    logger.info(
        'Received data-deletion request for Facebook user_id=%s', user_id
    )

    confirmation_code = secrets.token_hex(16)

    # Build a public status URL so Meta can show users a link to check
    # whether their deletion was processed.
    status_url = (
        f'https://www.kitilash.com/privacy-policy/'
        f'?deletion={confirmation_code}'
    )

    return JSONResponse(
        {
            'url': status_url,
            'confirmation_code': confirmation_code,
        }
    )

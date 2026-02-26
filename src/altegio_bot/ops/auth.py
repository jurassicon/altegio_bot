from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from altegio_bot.settings import settings

_security = HTTPBasic(auto_error=False)


async def require_ops_auth(
    request: Request,
    credentials: HTTPBasicCredentials | None = Depends(_security),
) -> None:
    ops_token = settings.ops_token
    ops_user = settings.ops_user
    ops_pass = settings.ops_pass

    # No auth configured → allow (useful in dev)
    if not ops_token and not ops_user:
        return

    # Token via header or query param
    if ops_token:
        header_tok = request.headers.get('X-Ops-Token', '')
        query_tok = request.query_params.get('token', '')
        if header_tok and secrets.compare_digest(header_tok, ops_token):
            return
        if query_tok and secrets.compare_digest(query_tok, ops_token):
            return

    # HTTP Basic Auth
    if ops_user and credentials:
        user_ok = secrets.compare_digest(
            credentials.username.encode(), ops_user.encode()
        )
        pass_ok = secrets.compare_digest(
            credentials.password.encode(), ops_pass.encode()
        )
        if user_ok and pass_ok:
            return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Unauthorized',
        headers={'WWW-Authenticate': 'Basic realm="Ops"'},
    )

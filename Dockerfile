FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project

COPY alembic.ini /app/
COPY alembic /app/alembic
COPY src /app/src

ENV PYTHONPATH=/app/src
ENV PATH=/app/.venv/bin:$PATH

EXPOSE 8000

CMD ['/app/.venv/bin/uvicorn', 'altegio_bot.main:app', '--host', '0.0.0.0', '--port', '8000']

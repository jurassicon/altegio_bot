FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen

COPY alembic /app/alembic
COPY alembic.ini /app/
COPY src /app/src

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uv", "run", "--env-file", ".env", "uvicorn", "altegio_bot.main:app", "--host", "0.0.0.0", "--port", "8000"]
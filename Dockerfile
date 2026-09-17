FROM python:3.14-slim

WORKDIR /tmp/dark-store-api

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY dark-store-api/pyproject.toml ./
COPY dark-store-api/README.md ./
COPY dark-store-api/app ./app/

RUN pip install --no-cache-dir .

WORKDIR /app

RUN useradd -m appuser \
    && chown -R appuser:appuser /app

EXPOSE 8003

USER appuser

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]

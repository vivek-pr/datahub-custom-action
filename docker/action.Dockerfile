# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash build-essential libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY action/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY action /app/action
COPY ingestion /app/ingestion
COPY scripts /app/scripts

ENV PYTHONPATH=/app

EXPOSE 8081

CMD ["uvicorn", "action.app:app", "--host", "0.0.0.0", "--port", "8081"]

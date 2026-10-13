FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app

RUN useradd --create-home --uid 10001 mock-api \
    && mkdir -p /app/src/batch_ingestion \
    && touch /app/src/batch_ingestion/__init__.py \
    && chown -R mock-api:mock-api /app
WORKDIR /app

COPY --chown=mock-api:mock-api src/batch_ingestion/source_seed.py /app/src/batch_ingestion/source_seed.py
COPY --chown=mock-api:mock-api mock_api /app/mock_api

USER mock-api
EXPOSE 8080

CMD ["python", "-m", "mock_api.server"]

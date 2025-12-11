# pipelines/realtime/Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:/app" \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock* ./

RUN pip install --upgrade pip \
    && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

COPY libs ./libs

COPY feast_store ./apps/feast
COPY apps/realtime/src ./src

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "src/main.py"]

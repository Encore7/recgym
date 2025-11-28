FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:/app" \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./pyproject.toml
RUN pip install --upgrade pip && pip install .

COPY libs ./libs
COPY apps/ingestion/src ./src

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "src/main.py"]

FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src:/app

COPY pyproject.toml ./pyproject.toml
RUN pip install --upgrade pip && pip install .

COPY libs ./libs

COPY apps/ingestion/src ./src

COPY infra/schemas ./infra/schemas

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "src/main.py"]

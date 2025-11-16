FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
# IMPORTANT:
ENV PYTHONPATH=/app/src:/app

# Install dependencies
COPY apps/generator/pyproject.toml ./pyproject.toml
RUN pip install --upgrade pip && pip install .

# Copy shared libs (observability etc)
COPY libs ./libs

# Copy generator service code
COPY apps/generator/src ./src

# Copy schemas
COPY infra/schemas ./infra/schemas

RUN addgroup --system app && adduser --system --ingroup app app
USER app

CMD ["python", "src/main.py"]

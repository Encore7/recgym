# Stage 1 – Builder
FROM python:3.11-slim AS builder

WORKDIR /app
ENV PIP_NO_CACHE_DIR=1

COPY pyproject.toml .
RUN pip install --upgrade pip && pip wheel --wheel-dir /wheels -r <(pipdeptree --freeze) || true

# Stage 2 – Runtime
FROM python:3.11-slim
WORKDIR /app

# create non-root user
RUN addgroup --system app && adduser --system --ingroup app app
USER app

ENV PYTHONUNBUFFERED=1
ENV PATH="/home/app/.local/bin:$PATH"

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*

COPY src/ ./src/
CMD ["python", "-m", "src.main"]

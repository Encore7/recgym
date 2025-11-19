FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install "feast[redis,postgres]>=0.32.0"

COPY features/feast /app/feast

CMD ["feast", "apply", "--repo", "/app/feast"]

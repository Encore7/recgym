FROM apache/flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip

COPY pyproject.toml /app/
WORKDIR /app
RUN pip3 install .

COPY apps/realtime /app/apps/realtime

ENV PYTHONPATH="/app"

CMD ["python3", "apps/realtime/src/main.py"]

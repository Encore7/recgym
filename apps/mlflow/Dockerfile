FROM ghcr.io/mlflow/mlflow:v2.14.1
RUN pip install --no-cache-dir psycopg2-binary

ENV MLFLOW_BACKEND_STORE_URI=postgresql+psycopg2://recgym:recgym@postgres:5432/recgym \
    MLFLOW_DEFAULT_ARTIFACT_ROOT=s3://mlflow/ \
    MLFLOW_S3_ENDPOINT_URL=http://minio:9000

EXPOSE 5000

ENTRYPOINT ["mlflow", "server"]
CMD ["--backend-store-uri", "postgresql+psycopg2://recgym:recgym@postgres:5432/recgym", \
     "--default-artifact-root", "s3://mlflow/", \
     "--host", "0.0.0.0", "--port", "5000"]

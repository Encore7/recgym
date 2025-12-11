from datetime import datetime, timedelta

from feast import FeatureStore


def main() -> None:
    """
    Materialize all batch feature views into Redis online store.

    - Uses feature_store.yaml in current dir.
    - Materializes from (now - 30d) to now.
    """
    store = FeatureStore(repo_path=".")
    end = datetime.utcnow()
    start = end - timedelta(days=30)

    store.materialize(start_date=start, end_date=end)


if __name__ == "__main__":
    main()
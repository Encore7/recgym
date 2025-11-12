import logging

from data.producer import KafkaEventProducer


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    producer = KafkaEventProducer()
    producer.run()


if __name__ == "__main__":
    main()

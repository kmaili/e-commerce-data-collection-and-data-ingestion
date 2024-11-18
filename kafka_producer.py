from confluent_kafka import Producer
import json


class KafkaProducer:
    def __init__(self, bootstrap_servers: str):

        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def produce(self, topic: str, key: str, value: dict):
        """
        Send a message to a Kafka topic.

        :param topic: Name of the Kafka topic.
        :param key: Message key (used for partitioning).
        :param value: Message value (must be serializable to JSON).
        """
        try:
            json_value = json.dumps(value)

            self.producer.produce(topic, key=key, value=json_value)

            self.producer.flush()
            print(f"message produced to topic '{topic}'")
        except Exception as e:
            print(f"Failed to produce message: {e}")




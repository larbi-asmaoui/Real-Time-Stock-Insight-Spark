"""
Kafka Producer Utility for FinanceLake
--------------------------------------
Centralized module to send data from any connector to Kafka.
"""

import json
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(
    filename='logs/kafka_producer.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class FinanceLakeKafkaProducer:
    """
    Handles Kafka message production in a standardized way for FinanceLake.
    """

    def __init__(self, broker="kafka:9092", topic="stock_prices"):
        """
        :param broker: Kafka broker address (default: kafka:9092)
        :param topic: Kafka topic to publish messages to
        """
        self.broker = broker
        self.topic = topic

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.broker],
                # bootstrap_servers= ["localhost:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=30000,
                api_version=(0, 10, 1)
            )
            logging.info(f"[Kafka] Connected to {self.broker}, topic={self.topic}")
        except Exception as e:
            logging.error(f"[ERROR] Could not connect to Kafka: {e}")
            self.producer = None

    def send(self, data: dict):
        """
        Sends a JSON message to the Kafka topic.
        :param data: dictionary to send
        """
        if not self.producer:
            logging.error("[ERROR] Producer not initialized.")
            return

        try:
            self.producer.send(self.topic, value=data)
            logging.info(f"[KAFKA PUSH] record = {data}")
        except Exception as e:
            logging.error(f"[ERROR] Failed to send message: {e}")

    def close(self):
        """Flush and close the producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logging.info("[Kafka] Connection closed.")

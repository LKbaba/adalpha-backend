"""
Kafka Client Service for Confluent Cloud.
Handles all Kafka producer and consumer operations.
"""

import json
import logging
from typing import Optional, Callable, Any
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from app.config import get_settings

logger = logging.getLogger(__name__)


class KafkaClient:
    """
    Kafka client wrapper for Confluent Cloud.
    Provides producer and consumer functionality with connection management.
    """

    def __init__(self):
        self._settings = get_settings()
        self._producer: Optional[Producer] = None
        self._consumer: Optional[Consumer] = None
        self._admin: Optional[AdminClient] = None

    @property
    def producer(self) -> Producer:
        """Lazy initialization of Kafka producer."""
        if self._producer is None:
            config = self._settings.get_producer_config()
            self._producer = Producer(config)
            logger.info("Kafka producer initialized")
        return self._producer

    @property
    def admin(self) -> AdminClient:
        """Lazy initialization of Kafka admin client."""
        if self._admin is None:
            config = self._settings.get_kafka_config()
            self._admin = AdminClient(config)
            logger.info("Kafka admin client initialized")
        return self._admin

    def get_consumer(self, group_id: str = "adalpha-backend") -> Consumer:
        """
        Create a new consumer instance with the given group ID.

        Args:
            group_id: Consumer group identifier

        Returns:
            Consumer: Configured Kafka consumer
        """
        config = self._settings.get_consumer_config(group_id)
        return Consumer(config)

    def get_cluster_metadata(self, timeout: float = 10.0) -> dict:
        """
        Get Kafka cluster metadata to verify connectivity.

        Args:
            timeout: Connection timeout in seconds

        Returns:
            dict: Cluster metadata including topics and broker info
        """
        try:
            metadata = self.admin.list_topics(timeout=timeout)

            topics = [topic for topic in metadata.topics.keys()]
            brokers = list(metadata.brokers.values())

            return {
                "connected": True,
                "cluster_id": metadata.cluster_id,
                "topics": topics,
                "broker_count": len(brokers),
                "brokers": [
                    {"id": b.id, "host": b.host, "port": b.port}
                    for b in brokers
                ]
            }
        except KafkaException as e:
            logger.error(f"Kafka connection error: {e}")
            return {
                "connected": False,
                "error": str(e),
                "topics": [],
                "broker_count": 0
            }
        except Exception as e:
            logger.error(f"Unexpected error getting cluster metadata: {e}")
            return {
                "connected": False,
                "error": str(e),
                "topics": [],
                "broker_count": 0
            }

    def produce_message(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        callback: Optional[Callable] = None,
        headers: Optional[dict] = None
    ) -> bool:
        """
        Send a message to a Kafka topic.

        Args:
            topic: Target topic name
            value: Message payload (will be JSON serialized if dict)
            key: Optional message key
            callback: Optional delivery callback function
            headers: Optional message headers

        Returns:
            bool: True if message was queued successfully
        """
        try:
            # Serialize value to JSON if it's a dict
            if isinstance(value, dict):
                value = json.dumps(value)

            # Convert to bytes
            value_bytes = value.encode("utf-8") if isinstance(value, str) else value
            key_bytes = key.encode("utf-8") if key else None

            # Prepare headers
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode("utf-8") if isinstance(v, str) else v)
                                 for k, v in headers.items()]

            # Produce message
            self.producer.produce(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                headers=kafka_headers,
                callback=callback or self._default_callback
            )

            # Trigger delivery reports
            self.producer.poll(0)

            return True

        except KafkaException as e:
            logger.error(f"Kafka produce error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error producing message: {e}")
            return False

    def flush_producer(self, timeout: float = 10.0) -> int:
        """
        Flush all pending messages in the producer queue.

        Args:
            timeout: Maximum time to wait for flush

        Returns:
            int: Number of messages still in queue (0 = all delivered)
        """
        return self.producer.flush(timeout=timeout)

    def _default_callback(self, err, msg):
        """Default delivery callback for logging."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition={msg.partition()}, offset={msg.offset()}]"
            )

    def close(self):
        """Close all Kafka connections."""
        if self._producer:
            self._producer.flush(timeout=5.0)
            logger.info("Kafka producer closed")
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed")


# Global singleton instance
kafka_client = KafkaClient()

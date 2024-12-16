import json
import pika
from typing import Dict, Any, Optional, List, Callable
from config import RabbitMQConfig
from logging_utils import CorrelatedLogger
from exceptions import MessageBrokerConnectionError, PublishError


class RabbitMQBroker:
    """
    Manages RabbitMQ message broker connections and communication.

    Handles connection establishment, message publishing,
    and provides error handling and logging for message operations.

    Key Responsibilities:
    - Establish and maintain RabbitMQ connections
    - Publish messages with reliability
    - Support topic-based message routing
    """

    def __init__(
        self,
        config: RabbitMQConfig = RabbitMQConfig(),
        logger: Optional[CorrelatedLogger] = None,
    ):
        """
        Initialize RabbitMQ broker with configurable parameters.

        Args:
            config (RabbitMQConfig): Configuration for RabbitMQ connection
            logger (CorrelatedLogger, optional): Logger for tracking broker activities
        """

        self._config = config
        self._host = config.host
        self._port = config.port
        self._username = config.username
        self._password = config.password
        self._virtual_host = config.virtual_host
        self._event_exchange = config.event_exchange

        self._logger = logger or CorrelatedLogger(__name__)

        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = (
            None
        )

        # Connection retry configuration
        self._max_retries = 3
        self._retry_delay = 5  # seconds

    def connect(self) -> None:
        """
        Establish a connection to RabbitMQ with retry mechanism.
        """
        try:
            credentials = pika.PlainCredentials(self._username, self._password)
            parameters = pika.ConnectionParameters(
                host=self._host,
                port=self._port,
                virtual_host=self._virtual_host,
                credentials=credentials,
                connection_attempts=self._max_retries,
                retry_delay=self._retry_delay,
            )

            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self._event_exchange, exchange_type="topic", durable=True
            )

            self._logger.info(
                "Successfully connected to RabbitMQ",
                extra={
                    "host": self._host,
                    "port": self._port,
                    "exchange": self._event_exchange,
                },
            )

        except pika.exceptions.AMQPConnectionError as e:
            self._logger.error(
                "Failed to connect to RabbitMQ",
                extra={"error": str(e), "host": self._host, "port": self._port},
            )
            raise MessageBrokerConnectionError(f"RabbitMQ connection failed: {e}")

    def disconnect(self) -> None:
        """
        Close RabbitMQ connection gracefully.
        """
        if self._connection and not self._connection.is_closed:
            self._connection.close()
            self._logger.info("RabbitMQ connection closed")

    def publish(self, routing_key: str, message: Dict[str, Any]) -> None:
        """
        Publish a message to the event exchange.
        """
        if not self._channel or self._channel.is_closed:
            raise MessageBrokerConnectionError("RabbitMQ channel is not open")

        try:
            message_body = json.dumps(message)
            self._channel.basic_publish(
                exchange=self._event_exchange,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2, content_type="application/json"
                ),
                mandatory=True,
            )

            self._logger.info(
                f"Published message to {routing_key}",
                extra={"routing_key": routing_key, "message_size": len(message_body)},
            )

        except Exception as e:
            self._logger.error(
                "Failed to publish message",
                extra={"error": str(e), "routing_key": routing_key},
            )
            raise PublishError(f"Message publication failed: {e}")

    def consume(
        self, queue_name: str, routing_keys: List[str], callback: Callable
    ) -> None:
        """
        Set up a consumer for specific routing keys.
        """
        if not self._channel or self._channel.is_closed:
            raise MessageBrokerConnectionError("RabbitMQ channel is not open")

        self._channel.queue_declare(queue=queue_name, durable=True)

        for key in routing_keys:
            self._channel.queue_bind(
                exchange=self._event_exchange, queue=queue_name, routing_key=key
            )

        # Set up consumer
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False,  # Manual acknowledgment for reliability
        )

        self._logger.info(
            f"Started consuming from {queue_name}",
            extra={"queue_name": queue_name, "routing_keys": routing_keys},
        )
        self._channel.start_consuming()

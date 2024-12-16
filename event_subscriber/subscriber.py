import json
from typing import Dict, List, Optional
from core.interfaces import CircuitBreaker, EventHandler
from message_broker.broker import RabbitMQBroker
from config import CircuitBreakerConfig
from logging_utils import CorrelatedLogger
from exceptions import EventHandlingError


class EventSubscriber:
    """
    Handles event subscription and processing with error resilience.

    Responsibilities:
    - Register event handlers
    - Consume messages from specified queues
    - Manage event processing with error handling
    """

    def __init__(
        self,
        message_broker: RabbitMQBroker,
        circuit_breaker: CircuitBreaker,
        config: CircuitBreakerConfig = CircuitBreakerConfig(),
        logger: Optional[CorrelatedLogger] = None,
    ):
        """
        Initialize event subscriber with messaging and circuit breaking capabilities.

        Args:
            message_broker (RabbitMQBroker): Message broker for consuming events
            circuit_breaker (CircuitBreaker): Mechanism to prevent system overload
            config (CircuitBreakerConfig, optional): Circuit breaker configuration
            logger (CorrelatedLogger, optional): Logger for tracking event processing
        """
        # Existing implementation remains the same
        self._message_broker = message_broker
        self._circuit_breaker = circuit_breaker
        self._config = config
        self._logger = logger or CorrelatedLogger(__name__)

        self._handlers: Dict[str, EventHandler] = {}

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """
        Register an event handler for a specific event type.
        """
        self._handlers[event_type] = handler
        self._logger.info(
            f"Registered handler for event type", extra={"event_type": event_type}
        )

    def start(self, queue_name: str, routing_keys: List[str]) -> None:
        """
        Start consuming events from specified queue and routing keys.
        """
        try:
            # Use circuit breaker to protect connection
            self._circuit_breaker.execute(self._message_broker.connect)

            # Use the broker's consume method with our custom message processing
            self._message_broker.consume(
                queue_name=queue_name,
                routing_keys=routing_keys,
                callback=self._process_message,
            )

        except Exception as e:
            self._logger.error(
                "Error starting event subscriber",
                extra={
                    "error": str(e),
                    "queue_name": queue_name,
                    "routing_keys": routing_keys,
                },
            )
            raise EventHandlingError(f"Failed to start event subscriber: {e}")

    def _process_message(self, channel, method, properties, body):
        """
        Process individual message with error handling and retry logic.
        """
        try:
            event_data = json.loads(body)
            event_type = event_data.get("type")

            # Find and execute appropriate handler
            handler = self._handlers.get(event_type)
            if handler:
                try:
                    handler.handle(event_data)
                    # Acknowledge message on successful processing
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as handler_error:
                    self._logger.error(
                        "Handler error for event type",
                        extra={"event_type": event_type, "error": str(handler_error)},
                    )
                    # Negative acknowledgement to requeue message
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                self._logger.warning(
                    "No handler for event type", extra={"event_type": event_type}
                )
                # Acknowledge unhandled messages to prevent infinite requeue
                channel.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            self._logger.error("Received malformed message")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self._logger.error(
                "Unexpected error processing message", extra={"error": str(e)}
            )
            # Do not requeue for unexpected errors
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

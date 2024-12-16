from typing import Dict, Any, Optional, List
from core.interfaces import CircuitBreaker, MessageBroker
from message_broker.broker import RabbitMQBroker
from config import CircuitBreakerConfig
from logging_utils import CorrelatedLogger
from exceptions import PublishError


class EventProducer:
    """
    Manages event publishing with error resilience and structured message generation.

    Responsibilities:
    - Register event type generators
    - Publish events to specified routing keys
    - Provide error handling and logging for event publication
    """

    def __init__(
        self,
        message_broker: RabbitMQBroker,
        circuit_breaker: CircuitBreaker,
        config: CircuitBreakerConfig = CircuitBreakerConfig(),
        logger: Optional[CorrelatedLogger] = None,
    ):
        """
        Initialize event producer with messaging and circuit breaking capabilities.

        Args:
            message_broker (MessageBroker): Message broker for publishing events
            circuit_breaker (CircuitBreaker): Mechanism to prevent system overload
            config (CircuitBreakerConfig, optional): Circuit breaker configuration
            logger (CorrelatedLogger, optional): Logger for tracking event publishing
        """
        self._message_broker = message_broker
        self._circuit_breaker = circuit_breaker
        self._config = config
        self._logger = logger or CorrelatedLogger(__name__)

        # Dictionary to store event type generators
        self._event_generators: Dict[str, callable] = {}

    def register_event_generator(self, event_type: str, generator: callable) -> None:
        """
        Register an event generator function for a specific event type.

        Args:
            event_type (str): The type of event to generate
            generator (callable): A function that creates the event message

        Logs the registration of a new event generator.
        """
        self._event_generators[event_type] = generator
        self._logger.info(
            "Registered event generator",
            extra={"event_type": event_type}
        )

    def publish(self, event_type: str, routing_key: str, additional_data: Dict[str, Any] = None) -> None:
        """
        Publish an event using the registered generator for the event type.

        Args:
            event_type (str): The type of event to publish
            routing_key (str): The routing key for message routing
            additional_data (Dict[str, Any], optional): Additional data to merge with generated event

        Raises:
            PublishError: If event generation or publishing fails
        """
        try:
            # Find the appropriate event generator
            generator = self._event_generators.get(event_type)
            if not generator:
                raise ValueError(f"No generator registered for event type: {event_type}")

            # Generate the base event message
            event_data = generator()

            # Merge with additional data if provided
            if additional_data:
                event_data.update(additional_data)

            # Ensure event type is set
            event_data['type'] = event_type

            self._circuit_breaker.execute(
                self._message_broker.publish,
                routing_key=routing_key,
                message=event_data
            )

            self._logger.info(
                "Event published successfully",
                extra={
                    "event_type": event_type,
                    "routing_key": routing_key
                }
            )

        except Exception as e:
            self._logger.error(
                "Error publishing event",
                extra={
                    "event_type": event_type,
                    "routing_key": routing_key,
                    "error": str(e)
                }
            )
            raise PublishError(
                f"Failed to publish event: {e}",
                context={
                    "event_type": event_type,
                    "routing_key": routing_key
                }
            )

    def start(self, routing_keys: List[str]) -> None:
        """
        Prepare the message broker connection.

        Args:
            routing_keys (List[str]): List of routing keys to prepare

        Raises:
            PublishError: If connection establishment fails
        """
        try:
            # Establish connection using circuit breaker
            self._circuit_breaker.execute(self._message_broker.connect)

        except Exception as e:
            self._logger.error(
                "Error starting event producer",
                extra={
                    "error": str(e),
                    "routing_keys": routing_keys,
                }
            )
            raise PublishError(
                f"Failed to start event producer: {e}",
                context={"routing_keys": routing_keys}
            )
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Type
from enum import Enum, auto


class CircuitState(Enum):
    """
    Represents the operational states of a circuit breaker.

    - CLOSED: Normal operational state
    - OPEN: Temporary service protection mode
    - HALF_OPEN: Recovery and validation state
    """

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class EventHandler(ABC):
    """Abstract base class for event handlers."""

    @abstractmethod
    def handle(self, event: Dict[str, Any]) -> None:
        """Process a specific event."""
        pass


class MessageBroker(ABC):
    """Abstract base class for message broker implementations."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the message broker."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the message broker."""
        pass

    @abstractmethod
    def publish(self, routing_key: str, message: Dict[str, Any]) -> None:
        """Publish a message to the broker."""
        pass


class CircuitBreaker(ABC):
    """Abstract base class for circuit breaker implementations."""

    @abstractmethod
    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a function with circuit breaker protection."""
        pass

    @property
    @abstractmethod
    def state(self) -> CircuitState:
        """Get the current state of the circuit breaker."""
        pass

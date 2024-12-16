class BaseApplicationError(Exception):
    """
    Base exception for application-level errors.

    Provides a common base for all custom application exceptions,
    enabling centralized error handling and logging.

    Attributes:
        message (str): Detailed error description
        context (dict, optional): Additional context about the error
    """

    def __init__(
        self,
        message: str = "An unexpected application error occurred",
        context: dict = None,
    ):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self):
        """
        Provides a detailed string representation of the error.

        Includes error message and any additional context.
        """
        context_str = ", ".join(f"{k}: {v}" for k, v in self.context.items())
        return (
            f"{self.message} (Context: {context_str})" if context_str else self.message
        )


class MessageBrokerError(BaseApplicationError):
    """Base exception for message broker related errors."""

    def __init__(
        self, message: str = "Message broker operation failed", context: dict = None
    ):
        super().__init__(message, context)


class MessageBrokerConnectionError(MessageBrokerError):
    """Raised when broker connection fails."""

    def __init__(
        self,
        message: str = "Failed to establish broker connection",
        context: dict = None,
    ):
        super().__init__(message, context)


class PublishError(MessageBrokerError):
    """Raised when message publication fails."""

    def __init__(
        self, message: str = "Failed to publish message", context: dict = None
    ):
        super().__init__(message, context)


class CircuitBreakerError(BaseApplicationError):
    """Raised when circuit breaker prevents execution."""

    def __init__(
        self, message: str = "Circuit breaker prevented execution", context: dict = None
    ):
        super().__init__(message, context)


class EventHandlingError(BaseApplicationError):
    """Raised when event handling fails."""

    def __init__(self, message: str = "Failed to handle event", context: dict = None):
        super().__init__(message, context)

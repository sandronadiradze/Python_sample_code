import time
import traceback
from typing import Any, Callable, Optional

from core.interfaces import CircuitBreaker, CircuitState
from config import CircuitBreakerConfig
from logging_utils import CorrelatedLogger
from exceptions import CircuitBreakerError


class DefaultCircuitBreaker(CircuitBreaker):
    """
    Implements a circuit breaker pattern with exponential backoff.

    Manages system resilience by tracking and responding to repeated failures,
    preventing cascading failures in distributed systems.

    Key Features:
    - Tracks consecutive failures
    - Implements exponential backoff
    - Provides automatic recovery mechanisms
    """

    def __init__(
        self,
        config: CircuitBreakerConfig = CircuitBreakerConfig(),
        logger: Optional[CorrelatedLogger] = None,
    ):
        """
        Initialize circuit breaker with custom configuration.

        Args:
            config (CircuitBreakerConfig): Configuration for failure detection and recovery
            logger (CorrelatedLogger, optional): Logger for tracking circuit breaker events
        """
        self._config = config
        self._logger = logger or CorrelatedLogger(__name__)

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._current_reset_timeout = config.reset_timeout

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute a function with circuit breaker protection.
        """
        # Check circuit state
        if self._state == CircuitState.OPEN:
            # Check if reset timeout has passed
            if (time.time() - self._last_failure_time) < self._current_reset_timeout:
                error_msg = "Circuit is currently OPEN and unavailable"
                self._logger.error(error_msg)
                raise CircuitBreakerError(error_msg)

            # Transition to half-open state for recovery attempt
            self._state = CircuitState.HALF_OPEN
            self._logger.info("Circuit transitioned to HALF_OPEN state")

        try:
            # Execute the function
            result = func(*args, **kwargs)

            # Reset tracking if previously in non-closed state
            if self._state != CircuitState.CLOSED:
                self._reset()

            return result

        except Exception as e:
            # Record and handle failures
            self._record_failure(e)
            raise

    def _record_failure(self, exception: Exception):
        """
        Record and process a failure event with exponential backoff.
        """
        self._failure_count += 1
        self._last_failure_time = time.time()

        # Log detailed failure information
        self._logger.error(
            f"Failure recorded: {exception}",
            extra={
                "failure_count": self._failure_count,
                "exception_trace": traceback.format_exc(),
            },
        )

        # Check if failure threshold is reached
        if self._failure_count >= self._config.failure_threshold:
            self._state = CircuitState.OPEN

            # Implement exponential backoff
            self._current_reset_timeout = min(
                self._current_reset_timeout * self._config.backoff_factor,
                self._config.max_reset_timeout,
            )

            self._logger.error(
                f"Circuit OPENED after {self._failure_count} consecutive failures. "
                f"Next reset attempt in {self._current_reset_timeout} seconds."
            )

    def _reset(self):
        """
        Reset circuit breaker to initial state after successful recovery.
        """
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._current_reset_timeout = self._config.reset_timeout

        self._logger.info("Circuit reset to CLOSED state")

    @property
    def state(self) -> CircuitState:
        """
        Get current circuit breaker state.
        """
        return self._state

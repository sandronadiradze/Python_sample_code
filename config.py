import os
from dataclasses import dataclass


@dataclass
class RabbitMQConfig:
    """
    Configuration settings for RabbitMQ connection.

    Loads connection parameters from environment variables
    with default values for local development.
    """

    host: str = os.getenv("RABBITMQ_HOST", "localhost")
    port: int = int(os.getenv("RABBITMQ_PORT", 5672))
    username: str = os.getenv("RABBITMQ_USERNAME", "guest")
    password: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    virtual_host: str = os.getenv("RABBITMQ_VHOST", "/")
    event_exchange: str = os.getenv("RABBITMQ_EVENT_EXCHANGE", "event_exchange")


@dataclass
class CircuitBreakerConfig:
    """
    Configuration parameters for circuit breaker behavior.

    Defines failure detection, timeout, and recovery
    characteristics for system resilience.
    """

    failure_threshold: int = int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", 5))
    reset_timeout: float = float(os.getenv("CIRCUIT_RESET_TIMEOUT", 60.0))
    max_reset_timeout: float = float(os.getenv("CIRCUIT_MAX_RESET_TIMEOUT", 300.0))
    backoff_factor: float = float(os.getenv("CIRCUIT_BACKOFF_FACTOR", 1.5))

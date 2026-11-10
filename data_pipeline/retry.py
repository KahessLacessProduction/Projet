"""Configurable retry mechanism with exponential backoff (Niveau 1.7 & 2.4).
Provides clear logging of attempts, error detection, delay management,
and clean termination when max attempts are exceeded.
"""
import functools
import logging
import time
from typing import Callable, Type, Tuple, Any

logger = logging.getLogger("data_pipeline.retry")


class MaxRetriesExceededError(Exception):
    """Raised when an operation has failed after reaching max retry attempts."""

    def __init__(self, operation_name: str, attempts: int, last_error: Exception):
        super().__init__(
            f"[FATAL] L'opération '{operation_name}' a échoué après {attempts} tentatives. "
            f"Dernière erreur : {type(last_error).__name__}: {last_error}"
        )
        self.operation_name = operation_name
        self.attempts = attempts
        self.last_error = last_error


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 0.5,
    backoff_factor: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    operation_name: str = None,
):
    """Decorator to retry a function call with exponential backoff on failure.

    Parameters:
        max_attempts: Total number of attempts allowed before giving up.
        initial_delay: Delay in seconds before the first retry.
        backoff_factor: Multiplier applied to delay after each retry.
        retryable_exceptions: Tuple of exceptions that trigger a retry.
        operation_name: Human-readable name for logging.
    """

    def decorator(func: Callable) -> Callable:
        op_name = operation_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = initial_delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as err:
                    last_exception = err
                    if attempt == max_attempts:
                        logger.error(
                            f"[RETRY TERMINÉ] Tentative {attempt}/{max_attempts} échouée pour '{op_name}'. "
                            f"Nombre maximal de tentatives dépassé. Erreur: {err}"
                        )
                        raise MaxRetriesExceededError(op_name, max_attempts, err) from err

                    logger.warning(
                        f"[RETRY DÉTECTÉ] Échec de la tentative {attempt}/{max_attempts} pour '{op_name}'. "
                        f"Cause: {type(err).__name__} ({err}). "
                        f"Nouvelle tentative dans {current_delay:.2f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

            # Fallback if loop ends without return/raise
            raise MaxRetriesExceededError(op_name, max_attempts, last_exception)

        return wrapper

    return decorator


class TransientFailureSimulator:
    """Helper to simulate transient failures for testing retry mechanisms.
    Fails `fail_count` times with `error_to_raise`, then succeeds.
    """

    def __init__(self, fail_count: int = 2, error_to_raise: Exception = None):
        self.fail_count = fail_count
        self.attempts = 0
        self.error_to_raise = error_to_raise or RuntimeError("Erreur réseau/SGBD temporaire simulée.")

    def __call__(self, real_function: Callable, *args, **kwargs) -> Any:
        self.attempts += 1
        if self.attempts <= self.fail_count:
            logger.info(
                f"[SIMULATION] Déclenchement d'une panne temporaire #{self.attempts}/{self.fail_count}..."
            )
            raise self.error_to_raise
        logger.info(f"[SIMULATION] Panne résolue à la tentative #{self.attempts}! Appel réussi.")
        return real_function(*args, **kwargs)

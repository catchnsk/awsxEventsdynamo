import math
import random


def compute_backoff(attempt: int, base_seconds: float = 1.0, jitter: float = 0.5) -> float:
    exponent = min(attempt, 10)
    backoff = base_seconds * (2 ** exponent)
    jitter_value = backoff * jitter * random.random()
    return backoff + jitter_value

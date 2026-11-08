"""API startup only: establish existing capacity without changing its limits."""
import math
from time import monotonic


def prewarm_api_databases(general, payment, *, timeout=30.0):
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("A positive finite startup deadline is required")
    deadline = monotonic() + timeout
    seen = set()
    for database in (general, payment):
        pool = database.pool
        if id(pool) in seen:
            continue
        seen.add(id(pool))
        minimum, maximum = pool.min_size, pool.max_size
        try:
            pool.resize(maximum, maximum)
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("API pool startup deadline exhausted")
            pool.wait(timeout=remaining)
        finally:
            # psycopg_pool.wait() closes its pool on a startup timeout. Do not
            # replace that original error by attempting to resize a closed pool.
            if not pool.closed:
                pool.resize(minimum, maximum)

"""PostgreSQL connection pool shared by Mastersmon synchronous FastAPI routes.

V2.19.2: thread-safe connections with bounded waiting under concurrent load.
Existing API: get_connection(), release_connection(conn), get_cursor(conn).
"""
import logging
import os
import threading
import time

from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("No se encontro DATABASE_URL en las variables de entorno.")

logger = logging.getLogger(__name__)


def _int_setting(name, default, minimum, maximum):
    raw = os.getenv(name, str(default))
    try:
        result = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser un entero") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} debe estar entre {minimum} y {maximum}")
    return result


_pool_min = _int_setting("DB_POOL_MINCONN", 1, 1, 50)
_pool_max = _int_setting("DB_POOL_MAXCONN", 10, 1, 50)
if _pool_max < _pool_min:
    raise ValueError("DB_POOL_MAXCONN debe ser mayor o igual a DB_POOL_MINCONN")

# Integer milliseconds; 0 means fail fast, never wait indefinitely.
_wait_ms = _int_setting("DB_POOL_WAIT_MS", 3000, 0, 30000)
_slow_ms = _int_setting("DB_POOL_SLOW_MS", 250, 1, 30000)

# Each Render worker/process has its own pool: size deployment capacity accordingly.
db_pool = pool.ThreadedConnectionPool(
    minconn=_pool_min,
    maxconn=_pool_max,
    dsn=DATABASE_URL,
    sslmode="require",
)

# Reserve capacity BEFORE asking psycopg2 for a connection. ThreadedConnectionPool
# itself throws PoolError immediately at capacity, rather than waiting.
_available_slots = threading.BoundedSemaphore(_pool_max)
_lease_lock = threading.Lock()
_leased_ids = set()


def get_connection():
    started = time.perf_counter()
    acquired = _available_slots.acquire(timeout=_wait_ms / 1000.0)
    if not acquired:
        logger.warning("PostgreSQL pool saturated: waiting exceeded %d ms", _wait_ms)
        raise pool.PoolError("PostgreSQL connection pool wait timeout")

    try:
        connection = db_pool.getconn()
        with _lease_lock:
            _leased_ids.add(id(connection))
        return connection
    except BaseException:
        # Failed acquisitions must not consume capacity permanently.
        _available_slots.release()
        raise
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        if elapsed_ms >= _slow_ms:
            logger.warning("PostgreSQL connection acquisition took %.1f ms", elapsed_ms)


def release_connection(conn):
    if conn is None:
        return
    with _lease_lock:
        conn_id = id(conn)
        if conn_id not in _leased_ids:
            # Avoid returning a connection twice or inflating semaphore capacity.
            logger.error("Attempted to release an unleased PostgreSQL connection")
            raise pool.PoolError("PostgreSQL connection already released or not leased")
        # Keep lease assigned until pool.putconn finishes.
        try:
            # A failed transaction is rolled back by psycopg2 when put back in pool.
            db_pool.putconn(conn)
        except BaseException:
            # Do not grant a semaphore slot if pool.putconn did not succeed.
            logger.exception("Failed returning PostgreSQL connection to pool")
            raise
        _leased_ids.remove(conn_id)
        _available_slots.release()


def get_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)

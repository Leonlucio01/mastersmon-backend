"""PostgreSQL connection pool shared by Mastersmon synchronous FastAPI routes.

V2.19.7: opt-in slow SQL and connection diagnostics without sensitive query text.
Preserves V2.19.2 pool semantics and get_connection/release_connection/get_cursor API.
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

_wait_ms = _int_setting("DB_POOL_WAIT_MS", 3000, 0, 30000)
_slow_ms = _int_setting("DB_POOL_SLOW_MS", 250, 1, 30000)

# Keep diagnostic opt-in: production logs remain unchanged unless explicitly enabled.
_perf_enabled = os.getenv("PERF_DIAGNOSTICS_ENABLED", "false").strip().lower() in ("true", "1", "yes")
_sql_slow_ms = _int_setting("PERF_SQL_SLOW_MS", 250, 1, 60000)


class DiagnosticCursor(RealDictCursor):
    """Time SQL execution only; never log SQL text, arguments or row contents."""

    def execute(self, query, vars=None):
        if not _perf_enabled:
            return super().execute(query, vars)
        started = time.perf_counter()
        try:
            return super().execute(query, vars)
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            if elapsed_ms >= _sql_slow_ms:
                logger.warning("PERF slow_sql duration_ms=%.1f", elapsed_ms)

    def executemany(self, query, vars_list):
        if not _perf_enabled:
            return super().executemany(query, vars_list)
        started = time.perf_counter()
        try:
            return super().executemany(query, vars_list)
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            if elapsed_ms >= _sql_slow_ms:
                logger.warning("PERF slow_sql_batch duration_ms=%.1f", elapsed_ms)


# Each Render worker/process has its own pool.
db_pool = pool.ThreadedConnectionPool(
    minconn=_pool_min,
    maxconn=_pool_max,
    dsn=DATABASE_URL,
    sslmode="require",
)

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
        _available_slots.release()
        raise
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        if elapsed_ms >= _slow_ms:
            logger.warning("PostgreSQL connection acquisition took %.1f ms", elapsed_ms)
        if _perf_enabled and elapsed_ms >= _slow_ms:
            logger.warning("PERF slow_db_acquire duration_ms=%.1f", elapsed_ms)


def release_connection(conn):
    if conn is None:
        return
    with _lease_lock:
        conn_id = id(conn)
        if conn_id not in _leased_ids:
            logger.error("Attempted to release an unleased PostgreSQL connection")
            raise pool.PoolError("PostgreSQL connection already released or not leased")
        try:
            db_pool.putconn(conn)
        except BaseException:
            logger.exception("Failed returning PostgreSQL connection to pool")
            raise
        _leased_ids.remove(conn_id)
        _available_slots.release()


def get_cursor(conn):
    return conn.cursor(cursor_factory=DiagnosticCursor if _perf_enabled else RealDictCursor)

"""Mastersmon V2.19.8: thread-safe PostgreSQL pool with optional, privacy-safe diagnostics.

Preserves V2.19.7 connection API, bounded waiting, and query behavior.
This module intentionally does NOT log SQL statements, parameters, tokens, or users.
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
_perf_enabled = os.getenv("PERF_DIAGNOSTICS_ENABLED", "false").strip().lower() in ("true", "1", "yes")
_sql_slow_ms = _int_setting("PERF_SQL_SLOW_MS", 250, 1, 60000)
_hold_slow_ms = _int_setting("PERF_DB_HOLD_SLOW_MS", 250, 1, 60000)


class DiagnosticCursor(RealDictCursor):
    """Record only duration of SQL execution, never statement or arguments."""
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


# One pool per Python process, matching the existing deployment semantics.
db_pool = pool.ThreadedConnectionPool(
    minconn=_pool_min,
    maxconn=_pool_max,
    dsn=DATABASE_URL,
    sslmode="require",
)
_available_slots = threading.BoundedSemaphore(_pool_max)
_lease_lock = threading.Lock()
_leased_ids = set()
_lease_start = {}  # Internal connection identity -> perf_counter timestamp, no personal data.


def get_connection():
    started = time.perf_counter()
    acquired = _available_slots.acquire(timeout=_wait_ms / 1000.0)
    wait_ms = (time.perf_counter() - started) * 1000
    if not acquired:
        logger.warning("PostgreSQL pool saturated: waiting exceeded %d ms", _wait_ms)
        if _perf_enabled:
            with _lease_lock:
                in_use = len(_leased_ids)
            logger.warning("PERF pool_timeout wait_ms=%.1f pool_in_use=%d pool_max=%d", wait_ms, in_use, _pool_max)
        raise pool.PoolError("PostgreSQL connection pool wait timeout")

    getconn_started = time.perf_counter()
    try:
        connection = db_pool.getconn()
        getconn_ms = (time.perf_counter() - getconn_started) * 1000
        with _lease_lock:
            cid = id(connection)
            _leased_ids.add(cid)
            if _perf_enabled:
                _lease_start[cid] = time.perf_counter()
            in_use = len(_leased_ids)
        return connection
    except BaseException:
        _available_slots.release()
        raise
    finally:
        total_ms = (time.perf_counter() - started) * 1000
        if total_ms >= _slow_ms:
            logger.warning("PostgreSQL connection acquisition took %.1f ms", total_ms)
        if _perf_enabled and (total_ms >= _slow_ms or wait_ms >= _slow_ms):
            logger.warning("PERF pool_wait_ms=%.1f pool_getconn_ms=%.1f pool_acquire_ms=%.1f pool_in_use=%d pool_max=%d",
                           wait_ms, (time.perf_counter() - getconn_started) * 1000,
                           total_ms, len(_leased_ids), _pool_max)


def release_connection(conn):
    if conn is None:
        return
    with _lease_lock:
        cid = id(conn)
        if cid not in _leased_ids:
            logger.error("Attempted to release an unleased PostgreSQL connection")
            raise pool.PoolError("PostgreSQL connection already released or not leased")
        started = _lease_start.get(cid)
        try:
            db_pool.putconn(conn)
        except BaseException:
            logger.exception("Failed returning PostgreSQL connection to pool")
            raise
        _leased_ids.remove(cid)
        _lease_start.pop(cid, None)
        in_use = len(_leased_ids)
        _available_slots.release()
    if _perf_enabled and started is not None:
        hold_ms = (time.perf_counter() - started) * 1000
        if hold_ms >= _hold_slow_ms:
            logger.warning("PERF connection_hold_ms=%.1f pool_in_use=%d pool_max=%d", hold_ms, in_use, _pool_max)


def get_cursor(conn):
    return conn.cursor(cursor_factory=DiagnosticCursor if _perf_enabled else RealDictCursor)

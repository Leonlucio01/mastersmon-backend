"""Shared PostgreSQL connection pool for synchronous FastAPI routes.

Mastersmon V2.19.1 - thread-safe pool, preserving the existing public helpers.
"""
import logging
import os
import time
from contextlib import contextmanager

from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("No se encontro DATABASE_URL en las variables de entorno.")

logger = logging.getLogger(__name__)


def _pool_size(env_name, default):
    raw = os.getenv(env_name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{env_name} debe ser un entero") from exc
    if value < 1 or value > 50:
        raise ValueError(f"{env_name} debe estar entre 1 y 50")
    return value


_pool_min = _pool_size("DB_POOL_MINCONN", 1)
_pool_max = _pool_size("DB_POOL_MAXCONN", 10)
if _pool_max < _pool_min:
    raise ValueError("DB_POOL_MAXCONN debe ser mayor o igual a DB_POOL_MINCONN")

# FastAPI executes synchronous endpoints in worker threads. SimpleConnectionPool
# is not thread-safe, while ThreadedConnectionPool protects its internal state.
db_pool = pool.ThreadedConnectionPool(
    minconn=_pool_min,
    maxconn=_pool_max,
    dsn=DATABASE_URL,
    sslmode="require",
)

DB_POOL_SLOW_MS = 250


def get_connection():
    started = time.perf_counter()
    try:
        return db_pool.getconn()
    except pool.PoolError:
        logger.exception("PostgreSQL pool exhausted while acquiring connection")
        raise
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        if elapsed_ms >= DB_POOL_SLOW_MS:
            logger.warning("PostgreSQL connection acquisition took %.1f ms", elapsed_ms)


def release_connection(conn):
    if conn is not None:
        db_pool.putconn(conn)


def get_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)

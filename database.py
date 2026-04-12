import os
from contextlib import contextmanager

from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_SEARCH_PATH = os.getenv("DB_SEARCH_PATH", "game, public")

if not DATABASE_URL:
    raise ValueError("No se encontró DATABASE_URL en las variables de entorno.")

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DATABASE_URL,
    sslmode="require",
)


def _configure_connection(conn, search_path: str = DEFAULT_SEARCH_PATH):
    with conn.cursor() as cursor:
        cursor.execute(f"SET search_path TO {search_path}")


def get_connection(search_path: str = DEFAULT_SEARCH_PATH):
    conn = db_pool.getconn()
    _configure_connection(conn, search_path=search_path)
    return conn


def release_connection(conn):
    if conn:
        db_pool.putconn(conn)


def get_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)


@contextmanager
def db_cursor(commit: bool = False, search_path: str = DEFAULT_SEARCH_PATH):
    conn = get_connection(search_path=search_path)
    cursor = get_cursor(conn)
    try:
        yield conn, cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)

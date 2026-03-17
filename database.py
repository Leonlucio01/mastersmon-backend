import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("No se encontró DATABASE_URL en las variables de entorno.")

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DATABASE_URL,
    sslmode="require"
)

def get_connection():
    return db_pool.getconn()

def release_connection(conn):
    if conn:
        db_pool.putconn(conn)

def get_cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes_pokemon import router
from routes_boss_idle import router_boss_idle
from routes_payments import router_payments
from database import get_connection, get_cursor, release_connection

BACKEND_MARKER = "mastersmon-backend-2026-03-23-monetization-v1"

app = FastAPI(
    title="MastersMon API",
    version="1.0.0"
)

ALLOWED_ORIGINS = [
    "https://mastersmon.com",
    "https://www.mastersmon.com",
    "https://leonlucio01.github.io",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(router_boss_idle)
app.include_router(router_payments)


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "MastersMon API running",
        "version_backend": BACKEND_MARKER,
        "router_loaded": ["routes_pokemon", "routes_boss_idle", "routes_payments"]
    }


@app.get("/health")
def health():
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = get_cursor(conn)
        cur.execute("""
            SELECT
                1 AS ok,
                current_database() AS db_name,
                current_schema() AS schema_name
        """)
        result = cur.fetchone()

        return {
            "status": "ok",
            "db": result["ok"],
            "db_name": result["db_name"],
            "schema_name": result["schema_name"],
            "version_backend": BACKEND_MARKER
        }
    except Exception as e:
        return {
            "status": "error",
            "detail": str(e),
            "version_backend": BACKEND_MARKER
        }
    finally:
        if cur:
            cur.close()
        if conn:
            release_connection(conn)

            
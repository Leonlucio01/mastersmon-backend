from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.pokemon_routes import router as pokemon_router
from routes.boss_idle_routes import router as boss_idle_router
from routes.payment_routes import router as payment_router
from routes.gym_routes import router as gym_router
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

# Transitional mounting: the wrapper modules preserve the current runtime behavior
# while the codebase migrates toward a modular structure.
app.include_router(pokemon_router)
app.include_router(boss_idle_router)
app.include_router(payment_router)
app.include_router(gym_router)


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "MastersMon API running",
        "version_backend": BACKEND_MARKER,
        "router_loaded": [
            "routes.pokemon_routes",
            "routes.boss_idle_routes",
            "routes.payment_routes",
            "routes.gym_routes",
        ],
    }


@app.get("/health")
def health():
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = get_cursor(conn)
        cur.execute(
            """
            SELECT
                1 AS ok,
                current_database() AS db_name,
                current_schema() AS schema_name
            """
        )
        result = cur.fetchone()

        return {
            "status": "ok",
            "db": result["ok"],
            "version_backend": BACKEND_MARKER,
        }
    except Exception:
        return {
            "status": "error",
            "version_backend": BACKEND_MARKER,
        }
    finally:
        if cur:
            cur.close()
        if conn:
            release_connection(conn)

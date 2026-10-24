import logging
import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from routes_pokemon import router
from routes_profile_edit import router_profile_edit
from routes_profile_badges import router_profile_badges
from routes_boss_idle import router_boss_idle
from routes_payments import router_payments
from routes_gyms import router_gyms
from routes_battle_ai import router_battle_ai
from trainer_onboarding_v14 import router_trainer_onboarding
from database import get_connection, get_cursor, release_connection
from routes_capturas_v211 import router_capturas_v211
from routes_lobby_v29 import router_lobby_v29
from routes_daily_shiny import router_daily_shiny
from routes_daily_missions import router_daily_missions

BACKEND_MARKER = "mastersmon-backend-2026-10-23-perf-v2.19.7"

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

# Only predefined, low-risk paths are logged. No URLs, tokens, user IDs,
# query strings, headers, request bodies, or response bodies are recorded.
_perf_logger = logging.getLogger("mastersmon.performance")
_perf_enabled = os.getenv("PERF_DIAGNOSTICS_ENABLED", "false").strip().lower() in ("true", "1", "yes")
try:
    _request_slow_ms = max(1, min(60000, int(os.getenv("PERF_REQUEST_SLOW_MS", "500"))))
except ValueError:
    _request_slow_ms = 500

_perf_routes = {
    "/auth/me", "/usuario/me", "/usuario/me/trainer-setup",
    "/usuario/me/pokemon", "/usuario/me/equipo",
    "/battle/gyms/progreso", "/battle/boss/estado",
    "/payments/catalogo", "/pokemon/capturas-recientes",
    "/missions/daily/me", "/payments/legendario-del-dia",
}

if _perf_enabled:
    @app.middleware("http")
    async def performance_diagnostics(request: Request, call_next):
        # Do not include OPTIONS or unknown paths in performance logs.
        path = request.scope.get("path", "")
        if request.method != "GET" or path not in _perf_routes:
            return await call_next(request)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000
            _perf_logger.warning("PERF request_failed endpoint=%s elapsed_ms=%.1f", path, elapsed_ms)
            raise
        elapsed_ms = (time.perf_counter() - started) * 1000
        if elapsed_ms >= _request_slow_ms:
            _perf_logger.warning(
                "PERF slow_request endpoint=%s status=%d elapsed_ms=%.1f",
                path, response.status_code, elapsed_ms,
            )
        return response

app.include_router(router)
app.include_router(router_profile_edit)
app.include_router(router_profile_badges)
app.include_router(router_boss_idle)
app.include_router(router_payments)
app.include_router(router_gyms)
app.include_router(router_battle_ai)
app.include_router(router_trainer_onboarding)
app.include_router(router_capturas_v211)
app.include_router(router_lobby_v29)
app.include_router(router_daily_shiny)
app.include_router(router_daily_missions)

@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "MastersMon API running",
        "version_backend": BACKEND_MARKER,
        "router_loaded": ["routes_pokemon", "routes_boss_idle", "routes_payments", "routes_gyms", "trainer_onboarding_v14"]
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
            "version_backend": BACKEND_MARKER
        }
    except Exception:
        return {
            "status": "error",
            "version_backend": BACKEND_MARKER
        }
    finally:
        if cur:
            cur.close()
        if conn:
            release_connection(conn)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette import status

from database import db_cursor
from routes.v2.adventure import router_v2_adventure
from routes.v2.auth import router_v2_auth
from routes.v2.collection import router_v2_collection
from routes.v2.gyms import router_v2_gyms
from routes.v2.house import router_v2_house
from routes.v2.home import router_v2_home
from routes.v2.onboarding import router_v2_onboarding
from routes.v2.profile import router_v2_profile
from routes.v2.team import router_v2_team
from routes.v2.trade import router_v2_trade

BACKEND_MARKER = "mastersmon-backend-v2-game-2026-04-11"

app = FastAPI(
    title="MastersMon API V2",
    version="2.0.0",
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


@app.exception_handler(Exception)
async def unhandled_exception_handler(_, exc: Exception):
    if hasattr(exc, "status_code") and hasattr(exc, "detail"):
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail and "message" in detail:
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "ok": False,
                    "error": detail,
                },
            )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "ok": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Ocurrió un error interno.",
            },
        },
    )


app.include_router(router_v2_auth)
app.include_router(router_v2_onboarding)
app.include_router(router_v2_profile)
app.include_router(router_v2_home)
app.include_router(router_v2_adventure)
app.include_router(router_v2_collection)
app.include_router(router_v2_team)
app.include_router(router_v2_gyms)
app.include_router(router_v2_house)
app.include_router(router_v2_trade)


@app.get("/")
def root():
    return {
        "ok": True,
        "data": {
            "status": "ok",
            "message": "MastersMon API V2 running",
            "version_backend": BACKEND_MARKER,
            "modules": ["v2-auth", "v2-onboarding", "v2-profile", "v2-home", "v2-adventure", "v2-collection", "v2-team", "v2-gyms", "v2-house", "v2-trade"],
        },
    }


@app.get("/health")
def health():
    try:
        with db_cursor() as (_, cursor):
            cursor.execute(
                """
                SELECT
                    current_database() AS db_name,
                    current_schema() AS schema_name
                """
            )
            result = cursor.fetchone()

        return {
            "ok": True,
            "data": {
                "status": "ok",
                "db_name": result["db_name"],
                "schema_name": result["schema_name"],
                "version_backend": BACKEND_MARKER,
            },
        }
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "ok": False,
                "error": {
                    "code": "DB_UNAVAILABLE",
                    "message": "No se pudo validar la conexión con la base de datos.",
                },
            },
        )

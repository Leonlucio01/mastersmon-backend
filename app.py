from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes_pokemon import router
from database import get_connection, get_cursor, release_connection

app = FastAPI(
    title="MastersMon API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/health")
def health():
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = get_cursor(conn)
        cur.execute("SELECT 1 AS ok;")
        result = cur.fetchone()

        return {"status": "ok", "db": result["ok"]}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    finally:
        if cur:
            cur.close()
        if conn:
            release_connection(conn)
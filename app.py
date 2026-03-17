from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes_pokemon import router
from database import get_connection, get_cursor, release_connection

app = FastAPI()

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
    try:
        conn = get_connection()
        cur = get_cursor(conn)
        cur.execute("SELECT 1 AS ok;")
        result = cur.fetchone()
        cur.close()
        release_connection(conn)
        return {"status": "ok", "db": result["ok"]}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
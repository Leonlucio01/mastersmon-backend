"""Legendario Shiny del día: consulta pública, sin compra ni entrega.
Una sola selección por día para todos los jugadores en America/Lima.
"""
import hashlib
import os
from datetime import datetime, time, timedelta

from fastapi import APIRouter, HTTPException
from database import get_connection, get_cursor, release_connection
from server_time import SERVER_TIMEZONE, server_now

router_daily_shiny = APIRouter(tags=["Legendario Shiny del día"])

# Lista inicial cerrada: especies legendarias conocidas (no añade especies a PostgreSQL).
# Puede limitarse mediante DAILY_SHINY_ALLOWED_IDS (lista CSV de IDs del catálogo existente).
DEFAULT_LEGENDARY_IDS = (144, 145, 146, 150, 243, 244, 245, 249, 250,
                         377, 378, 379, 380, 381, 382, 383, 384)
PRODUCT_CODE = "exclusive_pokemon_single"


def configured_ids():
    raw = os.getenv("DAILY_SHINY_ALLOWED_IDS", "").strip()
    if not raw:
        return DEFAULT_LEGENDARY_IDS
    try:
        ids = tuple(dict.fromkeys(int(x.strip()) for x in raw.split(",") if x.strip()))
    except ValueError:
        raise HTTPException(status_code=503, detail="Configuración de legendarios no válida")
    if not ids or any(x <= 0 for x in ids):
        raise HTTPException(status_code=503, detail="Catálogo diario no configurado")
    # Sólo un subconjunto de la lista aprobada inicialmente.
    if not set(ids).issubset(DEFAULT_LEGENDARY_IDS):
        raise HTTPException(status_code=503, detail="Lista diaria contiene especies no autorizadas")
    return ids


def stable_choice(ids, day):
    """Selección pseudoaleatoria persistente entre reinicios y workers.
    No depende del reloj del navegador. Evita repetir el día anterior cuando es posible.
    """
    seed = os.getenv("DAILY_SHINY_SECRET", "mastersmon-public-rotation-v1")
    def score(d):
        digest = hashlib.sha256(f"{seed}|{d.isoformat()}|{PRODUCT_CODE}".encode()).digest()
        return int.from_bytes(digest[:8], "big")
    pos = score(day) % len(ids)
    if len(ids) > 1:
        prev = score(day - timedelta(days=1)) % len(ids)
        if pos == prev:
            pos = (pos + 1 + score(day) % (len(ids)-1)) % len(ids)
    return ids[pos]


@router_daily_shiny.get("/payments/legendario-del-dia")
def legendary_of_day():
    today = server_now().date()
    tomorrow = datetime.combine(today + timedelta(days=1), time.min, tzinfo=SERVER_TIMEZONE)
    con = get_connection()
    cur = get_cursor(con)
    try:
        ids = configured_ids()
        cur.execute("SELECT id, nombre FROM pokemon WHERE id = ANY(%s)", (list(ids),))
        by_id = {int(row["id"]): row for row in (cur.fetchall() or [])}
        available_ids = tuple(i for i in ids if i in by_id)
        if not available_ids:
            raise HTTPException(status_code=503, detail="No hay legendarios configurados en el catálogo")
        pokemon_id = stable_choice(available_ids, today)
        p = by_id[pokemon_id]
        cur.execute("""SELECT codigo, precio_usd, moneda, activo
                       FROM monetizacion_productos WHERE codigo=%s LIMIT 1""", (PRODUCT_CODE,))
        product = cur.fetchone()
        if not product or not product["activo"]:
            raise HTTPException(status_code=503, detail="Producto exclusivo no disponible")
        return {
            "ok": True, "fecha": today.isoformat(), "zona_horaria": str(SERVER_TIMEZONE),
            "cambia_en": tomorrow.isoformat(), "pokemon": {
                "id": pokemon_id, "nombre": p["nombre"], "es_shiny": True
            },
            "producto_codigo": PRODUCT_CODE,
            "precio_usd": float(product["precio_usd"]),
            "moneda": product["moneda"],
            "compra_habilitada": False,
            "nota": "Rotación informativa. Canje automático pendiente de implementación."
        }
    finally:
        cur.close()
        release_connection(con)

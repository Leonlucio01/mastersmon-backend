"""Mastersmon V2.10: cinco capturas globales reales, sin exponer correo ni Google ID."""
from fastapi import APIRouter, Query
from database import get_connection, get_cursor, release_connection

router_capturas_v210 = APIRouter(tags=["Lobby V2.10"])

@router_capturas_v210.get("/pokemon/capturas-recientes")
def capturas_recientes(limit: int = Query(default=5, ge=1, le=5)):
    """Capturas exitosas registradas por Maps, no Pokémon regalados.

    maps_request_log: motivo='capturado', encuentro_id apunta al encuentro real.
    Usamos el id del log como orden de inserción, evitando depender de zona horaria.
    """
    conn = get_connection()
    cursor = None
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT l.id AS registro_id, e.pokemon_id, p.nombre AS pokemon_nombre,
                   p.tipo, e.es_shiny, u.nombre AS entrenador_nombre
              FROM maps_request_log l
              JOIN mapa_encuentros e ON e.id = l.encuentro_id
                                      AND e.usuario_id = l.usuario_id
              JOIN pokemon p ON p.id = e.pokemon_id
              JOIN usuarios u ON u.id = l.usuario_id
             WHERE l.endpoint = 'intentar-captura'
               AND l.motivo = 'capturado'
               AND l.status_code = 200
               AND e.consumido_en IS NOT NULL
             ORDER BY l.id DESC
             LIMIT %s
        """, (limit,))
        return {"ok": True, "criterio": "capturas_exitosas_maps_globales", "capturas": [
            {"pokemon_id": int(row["pokemon_id"]),
             "pokemon_nombre": str(row["pokemon_nombre"]),
             "tipo": str(row.get("tipo") or ""),
             "es_shiny": bool(row["es_shiny"]),
             "entrenador_nombre": str(row.get("entrenador_nombre") or "Entrenador")[:60]}
            for row in cursor.fetchall() or []
        ]}
    finally:
        if cursor is not None: cursor.close()
        release_connection(conn)

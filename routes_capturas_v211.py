"""Feed global de capturas Mastersmon. No contiene datos personales sensibles."""
import logging
from fastapi import APIRouter, Query
from database import get_connection, get_cursor, release_connection

log = logging.getLogger(__name__)
router_capturas_v211 = APIRouter(tags=["Lobby V2.11"])

@router_capturas_v211.get("/pokemon/capturas-recientes")
def capturas_recientes(limit: int = Query(default=5, ge=1, le=5)):
    """Solo capturas exitosas globales de Maps. Un mismo usuario puede aparecer varias veces.

    LEFT JOIN evita ocultar silenciosamente logs válidos por relaciones incompletas.
    Si hay datos incompletos no se fabrican nombres o tipos de Pokémon.
    """
    conn = get_connection()
    cur = None
    try:
        cur = get_cursor(conn)
        cur.execute("""
            SELECT l.id AS registro_id,
                   e.pokemon_id, p.nombre AS pokemon_nombre,
                   p.tipo AS tipo, e.es_shiny,
                   COALESCE(NULLIF(TRIM(u.nombre), ''), 'Entrenador') AS entrenador_nombre
            FROM public.maps_request_log AS l
            JOIN public.mapa_encuentros AS e
              ON e.id = l.encuentro_id AND e.usuario_id = l.usuario_id
            JOIN public.pokemon AS p ON p.id = e.pokemon_id
            LEFT JOIN public.usuarios AS u ON u.id = l.usuario_id
            WHERE l.endpoint = 'intentar-captura'
              AND l.motivo = 'capturado'
              AND l.status_code = 200
              AND e.consumido_en IS NOT NULL
            ORDER BY l.id DESC
            LIMIT %s
        """, (limit,))
        data = cur.fetchall() or []
        captures = []
        for row in data:
            # get_cursor(conn) returns RealDictCursor in the current backend
            captures.append({
                "pokemon_id": int(row["pokemon_id"]),
                "pokemon_nombre": str(row["pokemon_nombre"]),
                "tipo": str(row.get("tipo") or ""),
                "es_shiny": bool(row.get("es_shiny")),
                "entrenador_nombre": str(row.get("entrenador_nombre") or "Entrenador")[:60],
            })
        return {"ok": True, "capturas": captures, "total_devuelto": len(captures), "criterio": "maps_global_exitosas"}
    except Exception:
        log.exception("Fallo al consultar las capturas globales")
        raise
    finally:
        if cur is not None:
            cur.close()
        release_connection(conn)

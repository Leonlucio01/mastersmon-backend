"""Global recent Maps captures, including regional identity (MastersMon Alola V3D)."""
import logging
from fastapi import APIRouter, Query
from database import get_connection, get_cursor, release_connection

log = logging.getLogger(__name__)
router_capturas_v211 = APIRouter(tags=["Lobby V2.11"])

@router_capturas_v211.get("/pokemon/capturas-recientes")
def capturas_recientes(limit: int = Query(default=5, ge=1, le=5)):
    """Only successful global Maps captures; regional form comes from stored encounter.

    This endpoint does not mutate data. Regular Vulpix continues to use its
    original name, type and sprite; Alola Vulpix uses form 10205.
    """
    conn = get_connection()
    cur = None
    try:
        cur = get_cursor(conn)
        cur.execute("""
            SELECT l.id AS registro_id,
                   e.pokemon_id, e.forma_id,
                   CASE WHEN e.pokemon_id = 37 AND e.forma_id = 10205
                        THEN 'Vulpix (Alola)' ELSE p.nombre END AS pokemon_nombre,
                   CASE WHEN e.pokemon_id = 37 AND e.forma_id = 10205
                        THEN 'Ice' ELSE p.tipo END AS tipo,
                   e.es_shiny,
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
        captures = []
        for row in cur.fetchall() or []:
            species_id = int(row["pokemon_id"])
            form_id = int(row["forma_id"]) if row.get("forma_id") is not None else None
            is_alola = species_id == 37 and form_id == 10205
            captures.append({
                "pokemon_id": species_id,
                "forma_id": form_id,
                "region": "alola" if is_alola else None,
                "variant_suffix": "a" if is_alola else "",
                "pokemon_nombre": str(row["pokemon_nombre"]),
                "tipo": str(row.get("tipo") or ""),
                "es_shiny": bool(row.get("es_shiny")),
                "entrenador_nombre": str(row.get("entrenador_nombre") or "Entrenador")[:60],
            })
        return {"ok": True, "capturas": captures,
                "total_devuelto": len(captures), "criterio": "maps_global_exitosas"}
    except Exception:
        log.exception("Fallo al consultar las capturas globales")
        raise
    finally:
        if cur is not None:
            cur.close()
        release_connection(conn)

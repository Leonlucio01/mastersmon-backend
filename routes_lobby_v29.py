"""Mastersmon V2.9: ranking agregado de especies equipadas (solo lectura)."""
from fastapi import APIRouter, Query
from database import get_connection, get_cursor, release_connection

router_lobby_v29 = APIRouter(tags=["Lobby V2.9"])

@router_lobby_v29.get("/pokemon/populares")
def pokemon_populares(limit: int = Query(default=5, ge=1, le=20)):
    """Ranking basado en entrenadores distintos, no en copias de una especie.

    Se consideran solo Pokémon actualmente asignados a equipo_usuario.
    No se devuelven IDs ni nombres de usuarios.
    """
    conn = get_connection()
    cur = None
    try:
        cur = get_cursor(conn)
        cur.execute("""
            SELECT p.id AS pokemon_id, p.nombre, p.tipo,
                   COUNT(DISTINCT eu.usuario_id)::integer AS entrenadores
              FROM equipo_usuario eu
              JOIN usuario_pokemon up
                ON up.id = eu.usuario_pokemon_id
               AND up.usuario_id = eu.usuario_id
              JOIN pokemon p ON p.id = up.pokemon_id
             GROUP BY p.id, p.nombre, p.tipo
             ORDER BY entrenadores DESC, p.id ASC
             LIMIT %s
        """, (limit,))
        result = []
        for row in cur.fetchall() or []:
            result.append({
                "pokemon_id": int(row["pokemon_id"]),
                "nombre": str(row["nombre"]),
                "tipo": str(row["tipo"] or ""),
                "entrenadores": int(row["entrenadores"]),
            })
        return {"ok": True, "criterio": "entrenadores_distintos_con_especie_equipada", "populares": result}
    finally:
        if cur is not None:
            cur.close()
        release_connection(conn)

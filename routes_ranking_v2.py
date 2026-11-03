"""MastersMon - Ranking V2: backend for podium, teams and recent activity.

New read-only endpoints at /ranking/v3; legacy /ranking/v2 remains unchanged.
Presence is approximate (recent heartbeat), not a real-time connection guarantee.
"""

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Query

from auth import get_authenticated_user_id
from database import get_connection, get_cursor, release_connection

router_ranking_v2 = APIRouter(prefix="/ranking/v3", tags=["Ranking V2"])

Categoria = Literal["entrenadores", "coleccion", "shiny", "victorias"]
Equipo = Literal["red", "blue", "green"]
METRICAS = {
    "entrenadores": "experiencia_total",
    "coleccion": "total_unicos",
    "shiny": "total_shiny",
    "victorias": "victorias_total",
}
PRESENCIA_TTL_SEGUNDOS = 120

# The metric name is selected exclusively from METRICAS; never from user SQL input.
# Activity is reduced to one last heartbeat per user before joining the main ranking.
# Using the same tie-breakers as the V1 API preserves existing ranks.
RANKING_CTE = """
WITH puntuaciones AS (
    SELECT
        u.id AS usuario_id,
        COALESCE(NULLIF(BTRIM(u.nombre), ''), 'Entrenador') AS nombre,
        u.avatar_id,
        u.avatar_url,
        u.foto,
        CASE WHEN LOWER(COALESCE(u.trainer_team_color, '')) IN ('red','blue','green')
             THEN LOWER(u.trainer_team_color) ELSE NULL END AS equipo,
        COUNT(up.id)::bigint AS total_pokemon,
        COUNT(DISTINCT up.pokemon_id)::bigint AS especies_capturadas,
        COUNT(DISTINCT (up.pokemon_id, COALESCE(up.es_shiny, FALSE)))
            FILTER (WHERE up.id IS NOT NULL)::bigint AS total_unicos,
        COUNT(DISTINCT up.pokemon_id)
            FILTER (WHERE COALESCE(up.es_shiny, FALSE))::bigint AS total_shiny,
        COALESCE(SUM(up.experiencia_total),0)::bigint AS experiencia_total,
        COALESCE(SUM(up.victorias_total),0)::bigint AS victorias_total
    FROM public.usuarios u
    LEFT JOIN public.usuario_pokemon up ON up.usuario_id = u.id
    GROUP BY u.id, u.nombre, u.avatar_id, u.avatar_url, u.foto, u.trainer_team_color
), clasificados AS (
    SELECT p.*,
       ROW_NUMBER() OVER (
           ORDER BY {metrica} DESC, victorias_total DESC,
                    experiencia_total DESC, usuario_id ASC
       )::bigint AS puesto_global
    FROM puntuaciones p
), actividad_reciente AS (
    SELECT usuario_id, MAX(ultima_actividad) AS ultima_actividad
    FROM public.usuario_actividad
    WHERE online = TRUE AND ultima_actividad >= NOW() - (%s * INTERVAL '1 second')
    GROUP BY usuario_id
), enriched AS (
    SELECT c.*,
           (a.usuario_id IS NOT NULL) AS en_linea,
           a.ultima_actividad
    FROM clasificados c
    LEFT JOIN actividad_reciente a USING (usuario_id)
)
"""

# Always exclude sensitive columns such as correo, sesion_token, detalle, pagina.
def fila_publica(row, metric):
    last_active = row.get("ultima_actividad")
    return {
        "puesto": int(row["puesto"]),
        "puesto_global": int(row["puesto_global"]),
        "usuario_id": int(row["usuario_id"]),
        "nombre": row["nombre"],
        "avatar_id": row["avatar_id"],
        "avatar_url": row["avatar_url"],
        "foto": row["foto"],
        "equipo": row["equipo"],
        "puntuacion": int(row[metric] or 0),
        "total_pokemon": int(row["total_pokemon"] or 0),
        "especies_capturadas": int(row["especies_capturadas"] or 0),
        "total_unicos": int(row["total_unicos"] or 0),
        "total_shiny": int(row["total_shiny"] or 0),
        "experiencia_total": int(row["experiencia_total"] or 0),
        "victorias_total": int(row["victorias_total"] or 0),
        "en_linea": bool(row["en_linea"]),
        "ultima_actividad": last_active.isoformat() if last_active else None,
    }


def ejecutar_consulta(sql, args):
    conn = get_connection()
    cursor = None
    try:
        cursor = get_cursor(conn)
        cursor.execute(sql, args)
        return cursor.fetchall()
    finally:
        if cursor is not None:
            cursor.close()
        release_connection(conn)


@router_ranking_v2.get("/categorias")
def categorias():
    return {
        "categorias": [
            {"codigo": "entrenadores", "campo": "experiencia_total", "unidad": "EXP"},
            {"codigo": "coleccion", "campo": "total_unicos", "unidad": "variantes unicas"},
            {"codigo": "shiny", "campo": "total_shiny", "unidad": "Shiny unicos"},
            {"codigo": "victorias", "campo": "victorias_total", "unidad": "victorias"},
        ],
        "equipos": ["red", "blue", "green"],
        "presencia_ttl_segundos": PRESENCIA_TTL_SEGUNDOS,
        "semanal_disponible": False,
        "temporadas_disponibles": False,
        "puntos_rp_disponibles": False,
    }


@router_ranking_v2.get("/clasificacion")
def clasificacion(
    categoria: Categoria = "entrenadores",
    pagina: int = Query(default=1, ge=1, le=10000),
    limite: int = Query(default=10, ge=1, le=100),
    buscar: str = Query(default="", max_length=60),
    equipo: Equipo | None = None,
    _usuario_id: int = Depends(get_authenticated_user_id),
):
    metrica = METRICAS[categoria]
    offset = (pagina - 1) * limite
    # Global order is computed first; filter rank is recomputed for team-only views.
    sql = RANKING_CTE.format(metrica=metrica) + """
, en_equipo AS (
    SELECT enriched.*,
           ROW_NUMBER() OVER (ORDER BY puesto_global)::bigint AS puesto_equipo
    FROM enriched
    WHERE (%s IS NULL OR equipo = %s)
), filtrado AS (
    SELECT en_equipo.*,
           ROW_NUMBER() OVER (ORDER BY puesto_equipo)::bigint AS indice_filtro,
           COUNT(*) OVER ()::bigint AS total_filtrado
    FROM en_equipo
    WHERE (%s = '' OR POSITION(%s IN LOWER(nombre)) > 0)
)
SELECT * FROM filtrado
WHERE indice_filtro <= 3 OR (indice_filtro > %s AND indice_filtro <= %s)
ORDER BY indice_filtro
"""
    buscado = buscar.strip().casefold()
    rows = ejecutar_consulta(sql, (PRESENCIA_TTL_SEGUNDOS, equipo, equipo, buscado, buscado, offset, offset + limite))
    total = int(rows[0]["total_filtrado"]) if rows else 0
    # When filtering by team, use the team ranking; otherwise use global positions.
    def convert(row):
        result = fila_publica({**row, "puesto": row["puesto_equipo"]}, metrica)
        return result
    return {
        "categoria": categoria,
        "campo_puntuacion": metrica,
        "equipo": equipo,
        "pagina": pagina,
        "limite": limite,
        "total": total,
        "total_paginas": max(1, (total + limite - 1) // limite),
        "podio": [convert(r) for r in rows if int(r["indice_filtro"]) <= 3],
        "ranking": [convert(r) for r in rows if offset < int(r["indice_filtro"]) <= offset + limite],
        "presencia_ttl_segundos": PRESENCIA_TTL_SEGUNDOS,
        "temporada": None,
        "puntos_rp": None,
    }


@router_ranking_v2.get("/me")
def mi_posicion(
    categoria: Categoria = "entrenadores",
    usuario_id: int = Depends(get_authenticated_user_id),
):
    metrica = METRICAS[categoria]
    sql = RANKING_CTE.format(metrica=metrica) + """
SELECT enriched.*, puesto_global AS puesto FROM enriched WHERE usuario_id = %s
"""
    rows = ejecutar_consulta(sql, (PRESENCIA_TTL_SEGUNDOS, usuario_id))
    return {"categoria": categoria, "entrenador": fila_publica(rows[0], metrica) if rows else None}


@router_ranking_v2.get("/equipos")
def ranking_equipos(
    categoria: Categoria = "entrenadores",
    _usuario_id: int = Depends(get_authenticated_user_id),
):
    """Team aggregates: sum of each trainer's score, not unique union by species."""
    metrica = METRICAS[categoria]
    sql = RANKING_CTE.format(metrica=metrica) + """
SELECT equipo,
       COUNT(*)::bigint AS entrenadores,
       COUNT(*) FILTER (WHERE en_linea)::bigint AS activos,
       COALESCE(SUM({metrica}),0)::bigint AS puntuacion,
       COALESCE(SUM(total_unicos),0)::bigint AS total_unicos,
       COALESCE(SUM(total_shiny),0)::bigint AS total_shiny,
       COALESCE(SUM(victorias_total),0)::bigint AS victorias_total
FROM enriched
WHERE equipo IN ('red','blue','green')
GROUP BY equipo
ORDER BY puntuacion DESC, equipo ASC
""".format(metrica=metrica)
    rows = ejecutar_consulta(sql, (PRESENCIA_TTL_SEGUNDOS,))
    return {"categoria": categoria, "criterio": "suma_puntuaciones_jugadores", "equipos": [
        {"equipo": row["equipo"], "entrenadores": int(row["entrenadores"]),
         "activos": int(row["activos"]), "puntuacion": int(row["puntuacion"]),
         "total_unicos": int(row["total_unicos"]), "total_shiny": int(row["total_shiny"]),
         "victorias_total": int(row["victorias_total"])} for row in rows
    ]}


@router_ranking_v2.get("/actividad/resumen")
def resumen_actividad(_usuario_id: int = Depends(get_authenticated_user_id)):
    sql = """
SELECT COUNT(DISTINCT usuario_id)::bigint AS entrenadores_en_linea,
       COUNT(*)::bigint AS sesiones_recientes
FROM public.usuario_actividad
WHERE online = TRUE AND ultima_actividad >= NOW() - (%s * INTERVAL '1 second')
"""
    rows = ejecutar_consulta(sql, (PRESENCIA_TTL_SEGUNDOS,))
    row = rows[0] if rows else {}
    return {"entrenadores_en_linea": int(row.get("entrenadores_en_linea") or 0),
            "sesiones_recientes": int(row.get("sesiones_recientes") or 0),
            "ttl_segundos": PRESENCIA_TTL_SEGUNDOS,
            "significado": "Actividad registrada durante los ultimos 120 segundos"}


@router_ranking_v2.get("/actividad/entrenadores")
def entrenadores_activos(
    limite: int = Query(default=25, ge=1, le=50),
    _usuario_id: int = Depends(get_authenticated_user_id),
):
    """Only public profile fields; never expose session tokens, email or page actions."""
    sql = """
WITH actividad AS (
 SELECT usuario_id, MAX(ultima_actividad) AS ultima_actividad
 FROM public.usuario_actividad
 WHERE online = TRUE AND ultima_actividad >= NOW() - (%s * INTERVAL '1 second')
 GROUP BY usuario_id
)
SELECT u.id AS usuario_id,
       COALESCE(NULLIF(BTRIM(u.nombre), ''), 'Entrenador') AS nombre,
       u.avatar_id, u.avatar_url, u.foto,
       CASE WHEN LOWER(COALESCE(u.trainer_team_color,'')) IN ('red','blue','green')
            THEN LOWER(u.trainer_team_color) ELSE NULL END AS equipo,
       a.ultima_actividad
FROM actividad a
JOIN public.usuarios u ON u.id = a.usuario_id
ORDER BY a.ultima_actividad DESC, u.id ASC
LIMIT %s
"""
    rows = ejecutar_consulta(sql, (PRESENCIA_TTL_SEGUNDOS, limite))
    return {"ttl_segundos": PRESENCIA_TTL_SEGUNDOS, "entrenadores": [
        {"usuario_id": int(r["usuario_id"]), "nombre": r["nombre"],
         "avatar_id": r["avatar_id"], "avatar_url": r["avatar_url"],
         "foto": r["foto"], "equipo": r["equipo"],
         "ultima_actividad": r["ultima_actividad"].isoformat()} for r in rows
    ]}

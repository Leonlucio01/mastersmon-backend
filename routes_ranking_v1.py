"""MastersMon Ranking V1 - etapa 1, solo lectura y sin tablas nuevas.

Compatible con el modelo operativo public. No reemplaza rutas /ranking/* actuales.
Las puntuaciones RP, temporadas y PvP NO se calculan aqui.
"""
from fastapi import APIRouter, Depends, Query

from auth import get_authenticated_user_id
from database import get_connection, get_cursor, release_connection

router_ranking_v1 = APIRouter(prefix="/ranking/v2", tags=["Ranking V2 - etapa 1"])

# Solo nombres SQL definidos por el servidor. Nunca interpolar parametros del cliente.
METRICAS = {
    "entrenadores": "experiencia_total",
    "coleccion": "total_unicos",
    "shiny": "total_shiny",
    "victorias": "victorias_total",
}

# Se mantienen las columnas ya usadas por el ranking antiguo.
BASE_SQL = """
WITH acumulado AS (
    SELECT
        u.id AS usuario_id,
        COALESCE(NULLIF(BTRIM(u.nombre), ''), 'Entrenador') AS nombre,
        u.avatar_id,
        u.avatar_url,
        u.foto,
        COUNT(up.id)::bigint AS total_pokemon,
        COUNT(DISTINCT up.pokemon_id)::bigint AS especies_capturadas,
        COUNT(DISTINCT (up.pokemon_id, COALESCE(up.es_shiny, FALSE)))
            FILTER (WHERE up.id IS NOT NULL)::bigint AS total_unicos,
        COUNT(DISTINCT up.pokemon_id)
            FILTER (WHERE COALESCE(up.es_shiny, FALSE))::bigint AS total_shiny,
        COALESCE(SUM(up.experiencia_total), 0)::bigint AS experiencia_total,
        COALESCE(SUM(up.victorias_total), 0)::bigint AS victorias_total
    FROM public.usuarios u
    LEFT JOIN public.usuario_pokemon up ON up.usuario_id = u.id
    GROUP BY u.id, u.nombre, u.avatar_id, u.avatar_url, u.foto
), clasificado AS (
    SELECT
        acumulado.*,
        ROW_NUMBER() OVER (
            ORDER BY {metrica} DESC, victorias_total DESC,
                     experiencia_total DESC, usuario_id ASC
        )::bigint AS puesto
    FROM acumulado
)
"""


def ejecutar(cursor, sql, parametros):
    cursor.execute(sql, parametros)
    return cursor.fetchall()


def fila_publica(row, metrica):
    return {
        "puesto": int(row["puesto"]),
        "usuario_id": int(row["usuario_id"]),
        "nombre": row["nombre"],
        "avatar_id": row["avatar_id"],
        "avatar_url": row["avatar_url"],
        "foto": row["foto"],
        "puntuacion": int(row[metrica] or 0),
        "total_pokemon": int(row["total_pokemon"] or 0),
        "especies_capturadas": int(row["especies_capturadas"] or 0),
        "total_unicos": int(row["total_unicos"] or 0),
        "total_shiny": int(row["total_shiny"] or 0),
        "experiencia_total": int(row["experiencia_total"] or 0),
        "victorias_total": int(row["victorias_total"] or 0),
    }


@router_ranking_v1.get("/categorias")
def listar_categorias():
    return {
        "categorias": [
            {"codigo": "entrenadores", "campo": "experiencia_total", "unidad": "EXP"},
            {"codigo": "coleccion", "campo": "total_unicos", "unidad": "variantes unicas"},
            {"codigo": "shiny", "campo": "total_shiny", "unidad": "especies shiny"},
            {"codigo": "victorias", "campo": "victorias_total", "unidad": "victorias Pokemon"},
        ],
        "temporadas_disponibles": False,
        "puntos_competitivos_disponibles": False,
    }


@router_ranking_v1.get("/clasificacion")
def clasificacion(
    categoria: str = Query(default="entrenadores", pattern="^(entrenadores|coleccion|shiny|victorias)$"),
    pagina: int = Query(default=1, ge=1, le=10000),
    limite: int = Query(default=20, ge=1, le=100),
    buscar: str = Query(default="", max_length=60),
):
    metrica = METRICAS[categoria]
    busqueda = buscar.strip().casefold()
    comienzo = (pagina - 1) * limite
    sql = BASE_SQL.format(metrica=metrica) + """
, filtrado AS (
    SELECT clasificado.*,
           ROW_NUMBER() OVER (ORDER BY puesto)::bigint AS indice_filtro,
           COUNT(*) OVER ()::bigint AS total_filtrado
    FROM clasificado
    WHERE (%s = '' OR POSITION(%s IN LOWER(nombre)) > 0)
)
SELECT * FROM filtrado
WHERE indice_filtro <= 3
   OR (indice_filtro > %s AND indice_filtro <= %s)
ORDER BY indice_filtro
"""
    conn = get_connection()
    cursor = None
    try:
        cursor = get_cursor(conn)
        filas = ejecutar(cursor, sql, (busqueda, busqueda, comienzo, comienzo + limite))
        total = int(filas[0]["total_filtrado"]) if filas else 0
        podium = [fila_publica(f, metrica) for f in filas if int(f["indice_filtro"]) <= 3]
        resultados = [fila_publica(f, metrica) for f in filas
                      if comienzo < int(f["indice_filtro"]) <= comienzo + limite]
        return {
            "categoria": categoria,
            "campo_puntuacion": metrica,
            "temporada": None,
            "puntos_rp": None,
            "pagina": pagina,
            "limite": limite,
            "total": total,
            "total_paginas": max(1, (total + limite - 1) // limite),
            "podio": podium,
            "ranking": resultados,
        }
    finally:
        if cursor is not None:
            cursor.close()
        release_connection(conn)


@router_ranking_v1.get("/me")
def mi_posicion(
    categoria: str = Query(default="entrenadores", pattern="^(entrenadores|coleccion|shiny|victorias)$"),
    usuario_id: int = Depends(get_authenticated_user_id),
):
    """No acepta usuario_id arbitrario. Se obtiene del JWT validado."""
    metrica = METRICAS[categoria]
    sql = BASE_SQL.format(metrica=metrica) + """
SELECT * FROM clasificado WHERE usuario_id = %s
"""
    conn = get_connection()
    cursor = None
    try:
        cursor = get_cursor(conn)
        filas = ejecutar(cursor, sql, (usuario_id,))
        return {"categoria": categoria, "entrenador": fila_publica(filas[0], metrica) if filas else None}
    finally:
        if cursor is not None:
            cursor.close()
        release_connection(conn)

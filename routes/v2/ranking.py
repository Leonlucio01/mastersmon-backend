from fastapi import APIRouter, Query

from core.http import ok
from database import db_cursor

router_v2_ranking = APIRouter(prefix="/v2/ranking", tags=["v2-ranking"])


@router_v2_ranking.get("/summary")
def get_ranking_summary(limit: int = Query(default=10, ge=3, le=50)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT COUNT(*) AS total_pokedex
            FROM pokemon_species
            WHERE is_active = TRUE
            """
        )
        total_pokedex = int((cursor.fetchone() or {}).get("total_pokedex") or 0)

        cursor.execute(
            """
            WITH collector AS (
                SELECT
                    u.id AS user_id,
                    up.display_name AS nombre,
                    up.avatar_code AS avatar_url,
                    COUNT(DISTINCT p.species_id) AS total_unicos
                FROM users u
                JOIN user_profiles up ON up.user_id = u.id
                LEFT JOIN user_pokemon p ON p.user_id = u.id
                WHERE u.account_status = 'active'
                GROUP BY u.id, up.display_name, up.avatar_code
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY total_unicos DESC, nombre ASC, user_id ASC) AS puesto,
                user_id,
                nombre,
                avatar_url,
                total_unicos
            FROM collector
            ORDER BY puesto ASC
            LIMIT %s
            """,
            (limit,),
        )
        capturas_unicas = []
        for row in cursor.fetchall():
            item = dict(row)
            item["total_pokedex"] = total_pokedex
            capturas_unicas.append(item)

        cursor.execute(
            """
            WITH pokemon_rank AS (
                SELECT
                    up.id AS user_pokemon_id,
                    COALESCE(NULLIF(up.nickname, ''), ps.name) AS pokemon_nombre,
                    prof.display_name AS entrenador_nombre,
                    prof.avatar_code AS avatar_url,
                    up.level AS nivel,
                    up.exp_total AS experiencia_total,
                    up.is_shiny AS es_shiny,
                    up.species_id,
                    ps.normal_asset_path AS asset_url,
                    pt.name AS primary_type_name
                FROM user_pokemon up
                JOIN users u ON u.id = up.user_id
                JOIN user_profiles prof ON prof.user_id = u.id
                JOIN pokemon_species ps ON ps.id = up.species_id
                LEFT JOIN pokemon_species_types pst
                    ON pst.species_id = ps.id
                   AND pst.slot_order = 1
                LEFT JOIN pokemon_types pt ON pt.id = pst.type_id
                WHERE u.account_status = 'active'
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY experiencia_total DESC, nivel DESC, user_pokemon_id ASC) AS puesto,
                user_pokemon_id,
                pokemon_nombre,
                entrenador_nombre,
                avatar_url,
                nivel,
                experiencia_total,
                es_shiny,
                species_id,
                asset_url,
                primary_type_name
            FROM pokemon_rank
            ORDER BY puesto ASC
            LIMIT %s
            """,
            (limit,),
        )
        pokemon_experiencia = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            WITH trainer_rank AS (
                SELECT
                    u.id AS user_id,
                    up.display_name AS nombre,
                    up.avatar_code AS avatar_url,
                    COUNT(p.id) AS total_pokemon,
                    COALESCE(SUM(p.exp_total), 0) AS experiencia_total_sum,
                    COALESCE(MAX(p.level), 0) AS nivel_maximo
                FROM users u
                JOIN user_profiles up ON up.user_id = u.id
                LEFT JOIN user_pokemon p ON p.user_id = u.id
                WHERE u.account_status = 'active'
                GROUP BY u.id, up.display_name, up.avatar_code
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY experiencia_total_sum DESC, total_pokemon DESC, nombre ASC, user_id ASC) AS puesto,
                user_id,
                nombre,
                avatar_url,
                total_pokemon,
                experiencia_total_sum,
                nivel_maximo
            FROM trainer_rank
            ORDER BY puesto ASC
            LIMIT %s
            """,
            (limit,),
        )
        entrenadores = [dict(row) for row in cursor.fetchall()]

    return ok(
        {
            "capturas_unicas": capturas_unicas,
            "pokemon_experiencia": pokemon_experiencia,
            "entrenadores": entrenadores,
            "meta_pokedex": {
                "total_pokedex": total_pokedex,
            },
        }
    )

from fastapi import APIRouter, Depends, Query

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_collection = APIRouter(prefix="/v2/collection", tags=["v2-collection"])


def _build_collection_row(row: dict) -> dict:
    return {
        "user_pokemon_id": row["user_pokemon_id"],
        "species_id": row["species_id"],
        "species_name": row["species_name"],
        "nickname": row["nickname"],
        "display_name": row["nickname"] or row["species_name"],
        "level": row["level"],
        "exp_total": row["exp_total"],
        "variant": row["variant_code"],
        "variant_name": row["variant_name"],
        "form_code": row["form_code"],
        "form_name": row["form_name"],
        "asset_url": row["asset_url"],
        "is_shiny": row["is_shiny"],
        "is_favorite": row["is_favorite"],
        "is_locked": row["is_locked"],
        "is_trade_locked": row["is_trade_locked"],
        "is_team_locked": row["is_team_locked"],
        "captured_at": row["captured_at"],
        "trainer_team": row["trainer_team"],
    }


@router_v2_collection.get("/summary")
def get_collection_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_owned,
                COUNT(DISTINCT species_id) AS unique_owned,
                COUNT(CASE WHEN is_shiny THEN 1 END) AS shiny_owned,
                COUNT(CASE WHEN is_favorite THEN 1 END) AS favorite_owned,
                MAX(level) AS highest_level
            FROM user_pokemon
            WHERE user_id = %s
            """,
            (current_user["id"],),
        )
        totals = dict(cursor.fetchone())

        cursor.execute(
            """
            SELECT COUNT(*) AS active_species
            FROM pokemon_species
            WHERE is_active = TRUE
            """,
        )
        total_catalog = cursor.fetchone()["active_species"] or 0

        cursor.execute(
            """
            SELECT
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
                COUNT(*) AS qty
            FROM user_pokemon up
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            WHERE up.user_id = %s
            GROUP BY COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END)
            ORDER BY qty DESC, variant_code ASC
            """,
            (current_user["id"],),
        )
        variants = [dict(row) for row in cursor.fetchall()]

    unique_owned = totals["unique_owned"] or 0
    return ok(
        {
            "total_owned": totals["total_owned"] or 0,
            "unique_owned": unique_owned,
            "shiny_owned": totals["shiny_owned"] or 0,
            "favorite_owned": totals["favorite_owned"] or 0,
            "highest_level": totals["highest_level"] or 0,
            "completion_pct": int((unique_owned / total_catalog) * 100) if total_catalog else 0,
            "variants": variants,
        }
    )


@router_v2_collection.get("/pokemon")
def list_collection_pokemon(
    search: str | None = Query(default=None),
    variant: str | None = Query(default=None),
    favorites_only: bool = Query(default=False),
    limit: int = Query(default=60, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    search_value = f"%{(search or '').strip().lower()}%"

    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.id AS user_pokemon_id,
                up.species_id,
                ps.name AS species_name,
                up.nickname,
                up.level,
                up.exp_total,
                up.is_shiny,
                up.is_favorite,
                up.is_locked,
                up.is_trade_locked,
                up.is_team_locked,
                up.captured_at,
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
                COALESCE(pv.name, CASE WHEN up.is_shiny THEN 'Shiny' ELSE 'Normal' END) AS variant_name,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.name, ps.name) AS form_name,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url,
                upf.trainer_team
            FROM user_pokemon up
            JOIN pokemon_species ps ON ps.id = up.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            LEFT JOIN user_profiles upf ON upf.user_id = up.user_id
            WHERE up.user_id = %s
              AND (
                    %s = '%%'
                 OR LOWER(ps.name) LIKE %s
                 OR LOWER(COALESCE(up.nickname, '')) LIKE %s
              )
              AND (%s IS NULL OR COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) = %s)
              AND (%s = FALSE OR up.is_favorite = TRUE)
            ORDER BY up.is_favorite DESC, up.level DESC, up.id DESC
            LIMIT %s OFFSET %s
            """,
            (
                current_user["id"],
                search_value,
                search_value,
                search_value,
                variant,
                variant,
                favorites_only,
                limit,
                offset,
            ),
        )
        rows = [_build_collection_row(dict(row)) for row in cursor.fetchall()]

    return ok(
        {
            "items": rows,
            "filters": {
                "search": search,
                "variant": variant,
                "favorites_only": favorites_only,
                "limit": limit,
                "offset": offset,
            },
        }
    )


@router_v2_collection.get("/pokemon/{user_pokemon_id}")
def get_collection_pokemon_detail(user_pokemon_id: int, current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.id AS user_pokemon_id,
                up.user_id,
                up.species_id,
                ps.name AS species_name,
                ps.description AS species_description,
                ps.base_hp,
                ps.base_attack,
                ps.base_defense,
                ps.base_sp_attack,
                ps.base_sp_defense,
                ps.base_speed,
                ps.catch_rate,
                up.nickname,
                up.level,
                up.exp_total,
                up.is_shiny,
                up.origin_type,
                up.origin_ref,
                up.hp_iv,
                up.atk_iv,
                up.def_iv,
                up.sp_atk_iv,
                up.sp_def_iv,
                up.speed_iv,
                up.is_favorite,
                up.is_locked,
                up.is_trade_locked,
                up.is_team_locked,
                up.captured_at,
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
                COALESCE(pv.name, CASE WHEN up.is_shiny THEN 'Shiny' ELSE 'Normal' END) AS variant_name,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.name, ps.name) AS form_name,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
            FROM user_pokemon up
            JOIN pokemon_species ps ON ps.id = up.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            WHERE up.id = %s
              AND up.user_id = %s
            LIMIT 1
            """,
            (user_pokemon_id, current_user["id"]),
        )
        pokemon = cursor.fetchone()

        if not pokemon:
            fail("POKEMON_NOT_FOUND", "Ese Pokémon no existe en tu colección.", 404)

        cursor.execute(
            """
            SELECT
                pt.code,
                pt.name,
                pt.icon_path,
                pt.color_hex,
                pst.slot_order
            FROM pokemon_species_types pst
            JOIN pokemon_types pt ON pt.id = pst.type_id
            WHERE pst.species_id = %s
            ORDER BY pst.slot_order ASC
            """,
            (pokemon["species_id"],),
        )
        types = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                m.id,
                m.code,
                m.name,
                m.category,
                m.power,
                m.accuracy_pct,
                m.pp,
                m.priority,
                pt.code AS type_code,
                pt.name AS type_name,
                upm.unlocked_by,
                upm.unlocked_at_level,
                upms.slot_order
            FROM user_pokemon_moves upm
            JOIN moves m ON m.id = upm.move_id
            LEFT JOIN pokemon_types pt ON pt.id = m.type_id
            LEFT JOIN user_pokemon_move_set upms
                ON upms.user_pokemon_move_id = upm.id
               AND upms.user_pokemon_id = upm.user_pokemon_id
            WHERE upm.user_pokemon_id = %s
            ORDER BY COALESCE(upms.slot_order, 99) ASC, m.name ASC
            """,
            (user_pokemon_id,),
        )
        moves = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                er.to_species_id,
                ps.name AS evolves_to_name,
                er.method,
                er.required_level,
                er.required_item_code,
                er.required_time_of_day,
                er.required_known_move_code,
                er.required_min_happiness
            FROM evolution_rules er
            JOIN pokemon_species ps ON ps.id = er.to_species_id
            WHERE er.from_species_id = %s
              AND er.is_active = TRUE
            ORDER BY er.priority ASC, er.to_species_id ASC
            """,
            (pokemon["species_id"],),
        )
        evolutions = [dict(row) for row in cursor.fetchall()]

    payload = dict(pokemon)
    payload["types"] = types
    payload["moves"] = moves
    payload["evolutions"] = evolutions
    return ok(payload)

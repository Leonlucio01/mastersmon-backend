from fastapi import APIRouter, Depends

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_gyms = APIRouter(prefix="/v2/gyms", tags=["v2-gyms"])


def _current_region(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            COALESCE(up.region_start_id, fallback.id) AS region_id,
            COALESCE(r.display_order, fallback.display_order) AS display_order
        FROM users u
        LEFT JOIN user_profiles up ON up.user_id = u.id
        LEFT JOIN regions r ON r.id = up.region_start_id
        LEFT JOIN LATERAL (
            SELECT id, display_order
            FROM regions
            WHERE is_active = TRUE
            ORDER BY display_order ASC
            LIMIT 1
        ) fallback ON TRUE
        WHERE u.id = %s
        LIMIT 1
        """,
        (user_id,),
    )
    return cursor.fetchone()


def _build_gym_item(row: dict) -> dict:
    return {
        "id": row["id"],
        "code": row["code"],
        "name": row["name"],
        "city_name": row["city_name"],
        "gym_kind": row["gym_kind"],
        "primary_type_code": row["primary_type_code"],
        "badge_name": row["badge_name"],
        "badge_asset_path": row["badge_asset_path"],
        "display_order": row["display_order"],
        "recommended_level": row["recommended_level"],
        "reward_soft_currency": row["reward_soft_currency"],
        "reward_exp": row["reward_exp"],
        "description": row["description"],
        "region": {
            "id": row["region_id"],
            "code": row["region_code"],
            "name": row["region_name"],
            "generation_id": row["generation_id"],
        },
        "progress": {
            "unlocked": row["unlocked"],
            "completed": row["completed"],
            "attempts": row["attempts"],
            "victories": row["victories"],
            "best_turns": row["best_turns"],
            "last_completed_at": row["last_completed_at"],
        },
    }


@router_v2_gyms.get("/summary")
def get_gyms_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        current_region = _current_region(cursor, current_user["id"])
        current_region_id = current_region["region_id"] if current_region else None

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_gyms,
                COUNT(CASE WHEN ugp.completed THEN 1 END) AS completed_gyms,
                COUNT(CASE WHEN ugp.unlocked THEN 1 END) AS unlocked_gyms
            FROM gyms g
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.is_active = TRUE
            """,
            (current_user["id"],),
        )
        totals = dict(cursor.fetchone())

        cursor.execute(
            """
            SELECT
                g.id,
                g.code,
                g.name,
                g.badge_name,
                g.primary_type_code,
                g.recommended_level,
                r.code AS region_code,
                r.name AS region_name
            FROM gyms g
            JOIN regions r ON r.id = g.region_id
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.is_active = TRUE
              AND COALESCE(ugp.completed, FALSE) = FALSE
              AND (COALESCE(ugp.unlocked, FALSE) = TRUE OR g.region_id = %s)
            ORDER BY r.display_order ASC, g.display_order ASC, g.id ASC
            LIMIT 1
            """,
            (current_user["id"], current_region_id),
        )
        next_gym = cursor.fetchone()

    total_gyms = totals["total_gyms"] or 0
    completed_gyms = totals["completed_gyms"] or 0
    return ok(
        {
            "total_gyms": total_gyms,
            "completed_gyms": completed_gyms,
            "unlocked_gyms": totals["unlocked_gyms"] or 0,
            "completion_pct": int((completed_gyms / total_gyms) * 100) if total_gyms else 0,
            "next_gym": dict(next_gym) if next_gym else None,
        }
    )


@router_v2_gyms.get("")
def list_gyms(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                g.id,
                g.region_id,
                g.code,
                g.name,
                g.city_name,
                g.gym_kind,
                g.primary_type_code,
                g.badge_name,
                g.badge_asset_path,
                g.display_order,
                g.recommended_level,
                g.reward_soft_currency,
                g.reward_exp,
                g.description,
                r.code AS region_code,
                r.name AS region_name,
                r.generation_id,
                COALESCE(ugp.unlocked, FALSE) AS unlocked,
                COALESCE(ugp.completed, FALSE) AS completed,
                COALESCE(ugp.attempts, 0) AS attempts,
                COALESCE(ugp.victories, 0) AS victories,
                ugp.best_turns,
                ugp.last_completed_at
            FROM gyms g
            JOIN regions r ON r.id = g.region_id
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.is_active = TRUE
            ORDER BY r.display_order ASC, g.display_order ASC, g.id ASC
            """,
            (current_user["id"],),
        )
        gyms = [_build_gym_item(dict(row)) for row in cursor.fetchall()]

    return ok({"items": gyms})


@router_v2_gyms.get("/{gym_code}")
def get_gym_detail(gym_code: str, current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                g.id,
                g.region_id,
                g.code,
                g.name,
                g.city_name,
                g.gym_kind,
                g.primary_type_code,
                g.badge_name,
                g.badge_asset_path,
                g.display_order,
                g.recommended_level,
                g.reward_soft_currency,
                g.reward_exp,
                g.description,
                r.code AS region_code,
                r.name AS region_name,
                r.generation_id,
                COALESCE(ugp.unlocked, FALSE) AS unlocked,
                COALESCE(ugp.completed, FALSE) AS completed,
                COALESCE(ugp.attempts, 0) AS attempts,
                COALESCE(ugp.victories, 0) AS victories,
                ugp.best_turns,
                ugp.last_completed_at
            FROM gyms g
            JOIN regions r ON r.id = g.region_id
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.code = %s
              AND g.is_active = TRUE
            LIMIT 1
            """,
            (current_user["id"], gym_code),
        )
        gym = cursor.fetchone()

        if not gym:
            fail("GYM_NOT_FOUND", "Ese gym no existe.", 404)

        cursor.execute(
            """
            SELECT
                ger.slot_order,
                ger.level,
                ger.is_signature,
                ger.is_shiny,
                ps.id AS species_id,
                ps.name AS species_name,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.name, ps.name) AS form_name,
                COALESCE(
                    CASE WHEN ger.is_shiny THEN pf.asset_shiny_path ELSE pf.asset_normal_path END,
                    CASE WHEN ger.is_shiny THEN ps.shiny_asset_path ELSE ps.normal_asset_path END
                ) AS asset_url
            FROM gym_enemy_roster ger
            JOIN pokemon_species ps ON ps.id = ger.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = ger.form_id
            WHERE ger.gym_id = %s
            ORDER BY ger.slot_order ASC, ger.id ASC
            """,
            (gym["id"],),
        )
        roster = [dict(row) for row in cursor.fetchall()]

    return ok(
        {
            "gym": _build_gym_item(dict(gym)),
            "roster": roster,
        }
    )

from fastapi import APIRouter, Depends

from auth import get_current_user
from core.http import ok
from database import db_cursor

router_v2_home = APIRouter(prefix="/v2/home", tags=["v2-home"])


def _team_members_payload(cursor, user_id: int):
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM team_presets
        WHERE user_id = %s
          AND is_active = TRUE
        """,
        (user_id,),
    )
    has_active_team = cursor.fetchone()["total"] > 0

    if has_active_team:
        cursor.execute(
            """
            SELECT
                up.id AS user_pokemon_id,
                up.level,
                up.species_id,
                COALESCE(NULLIF(up.nickname, ''), ps.name) AS name,
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url,
                ps.base_hp,
                ps.base_attack,
                ps.base_defense,
                ps.base_sp_attack,
                ps.base_sp_defense,
                ps.base_speed,
                tpm.slot_order
            FROM team_presets tp
            JOIN team_preset_members tpm ON tpm.team_preset_id = tp.id
            JOIN user_pokemon up ON up.id = tpm.user_pokemon_id
            JOIN pokemon_species ps ON ps.id = up.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            WHERE tp.user_id = %s
              AND tp.is_active = TRUE
            ORDER BY tpm.slot_order ASC
            LIMIT 6
            """,
            (user_id,),
        )
    else:
        cursor.execute(
            """
            SELECT
                up.id AS user_pokemon_id,
                up.level,
                up.species_id,
                COALESCE(NULLIF(up.nickname, ''), ps.name) AS name,
                COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant,
                COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url,
                ps.base_hp,
                ps.base_attack,
                ps.base_defense,
                ps.base_sp_attack,
                ps.base_sp_defense,
                ps.base_speed,
                ROW_NUMBER() OVER (ORDER BY up.level DESC, up.id ASC) AS slot_order
            FROM user_pokemon up
            JOIN pokemon_species ps ON ps.id = up.species_id
            LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
            LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
            WHERE up.user_id = %s
            ORDER BY up.level DESC, up.id ASC
            LIMIT 6
            """,
            (user_id,),
        )

    members = [dict(row) for row in cursor.fetchall()]
    power_score = 0
    for member in members:
        base_total = (
            member["base_hp"]
            + member["base_attack"]
            + member["base_defense"]
            + member["base_sp_attack"]
            + member["base_sp_defense"]
            + member["base_speed"]
        )
        power_score += member["level"] * 12 + int(base_total / 8)
        member.pop("base_hp", None)
        member.pop("base_attack", None)
        member.pop("base_defense", None)
        member.pop("base_sp_attack", None)
        member.pop("base_sp_defense", None)
        member.pop("base_speed", None)
        member.pop("slot_order", None)

    return {
        "power_score": power_score,
        "member_count": len(members),
        "members": members,
    }


@router_v2_home.get("/summary")
def get_home_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                u.id,
                u.email,
                up.display_name,
                up.photo_url,
                up.avatar_code,
                up.trainer_team,
                up.region_start_id,
                r.id AS active_region_id,
                r.code AS active_region_code,
                r.name AS active_region_name
            FROM users u
            JOIN user_profiles up ON up.user_id = u.id
            LEFT JOIN regions r ON r.id = up.region_start_id
            WHERE u.id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        trainer = dict(cursor.fetchone())

        if not trainer["active_region_id"]:
            cursor.execute(
                """
                SELECT id, code, name
                FROM regions
                WHERE is_active = TRUE
                ORDER BY display_order ASC
                LIMIT 1
                """
            )
            fallback_region = cursor.fetchone()
            if fallback_region:
                trainer["active_region_id"] = fallback_region["id"]
                trainer["active_region_code"] = fallback_region["code"]
                trainer["active_region_name"] = fallback_region["name"]

        active_region_id = trainer["active_region_id"]

        cursor.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN ugp.completed THEN 1 ELSE 0 END), 0) AS completed_gyms,
                COUNT(g.id) AS total_gyms
            FROM gyms g
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.region_id = %s
              AND g.is_active = TRUE
            """,
            (current_user["id"], active_region_id),
        )
        gym_progress = dict(cursor.fetchone())
        total_gyms = gym_progress["total_gyms"] or 0
        completed_gyms = gym_progress["completed_gyms"] or 0
        completion_pct = int((completed_gyms / total_gyms) * 100) if total_gyms else 0

        cursor.execute(
            """
            SELECT COUNT(*) AS unlocked_zones
            FROM zones
            WHERE region_id = %s
              AND is_active = TRUE
            """,
            (active_region_id,),
        )
        unlocked_zones = cursor.fetchone()["unlocked_zones"]

        cursor.execute(
            """
            SELECT
                g.id,
                g.code,
                g.name,
                g.badge_name,
                g.recommended_level
            FROM gyms g
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.region_id = %s
              AND g.is_active = TRUE
              AND COALESCE(ugp.completed, FALSE) = FALSE
            ORDER BY g.display_order ASC, g.id ASC
            LIMIT 1
            """,
            (current_user["id"], active_region_id),
        )
        next_gym = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                z.id,
                z.code,
                z.name,
                z.biome_code,
                z.level_min,
                z.level_max
            FROM zones z
            WHERE z.region_id = %s
              AND z.is_active = TRUE
            ORDER BY z.display_order ASC, z.id ASC
            LIMIT 1
            """,
            (active_region_id,),
        )
        current_zone = cursor.fetchone()

        team_summary = _team_members_payload(cursor, current_user["id"])

        cursor.execute(
            """
            SELECT COUNT(DISTINCT species_id) AS owned_species
            FROM user_pokemon
            WHERE user_id = %s
            """,
            (current_user["id"],),
        )
        collection_owned = cursor.fetchone()["owned_species"] or 0

        cursor.execute(
            """
            SELECT
                ph.storage_capacity,
                huc.code,
                huc.storage_capacity AS next_storage_capacity
            FROM player_houses ph
            LEFT JOIN house_upgrade_catalog current_huc ON current_huc.id = ph.current_upgrade_id
            LEFT JOIN LATERAL (
                SELECT code, storage_capacity
                FROM house_upgrade_catalog
                WHERE is_active = TRUE
                  AND storage_capacity > ph.storage_capacity
                ORDER BY storage_capacity ASC, id ASC
                LIMIT 1
            ) huc ON TRUE
            WHERE ph.user_id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        next_house_upgrade = cursor.fetchone()

    trainer_payload = {
        "id": trainer["id"],
        "display_name": trainer["display_name"],
        "avatar_url": trainer["photo_url"],
        "avatar_code": trainer["avatar_code"],
        "team": trainer["trainer_team"],
        "trainer_level": max(1, int(completed_gyms) + max(team_summary["member_count"], 1)),
        "trainer_exp": collection_owned * 100 + completed_gyms * 750,
        "next_level_exp": (max(1, int(completed_gyms) + max(team_summary["member_count"], 1)) + 1) * 1000,
        "active_region": {
            "id": trainer["active_region_id"],
            "code": trainer["active_region_code"],
            "name": trainer["active_region_name"],
        },
    }

    return ok(
        {
            "trainer": trainer_payload,
            "progress": {
                "current_region_completion_pct": completion_pct,
                "unlocked_zones": unlocked_zones,
                "completed_gyms": completed_gyms,
                "next_gym": dict(next_gym) if next_gym else None,
            },
            "adventure": {
                "current_zone": dict(current_zone) if current_zone else None,
                "next_action": {
                    "type": "continue_zone",
                    "label": "Continuar aventura",
                },
            },
            "team_summary": team_summary,
            "alerts": {
                "idle_ready": False,
                "battle_rewards_ready": False,
                "pending_trade_count": 0,
                "unclaimed_rewards_count": 0,
            },
            "milestones": {
                "collection_owned": collection_owned,
                "collection_seen": collection_owned,
                "next_house_upgrade": {
                    "code": next_house_upgrade["code"],
                    "storage_capacity": next_house_upgrade["next_storage_capacity"],
                }
                if next_house_upgrade and next_house_upgrade["code"]
                else None,
            },
        }
    )


@router_v2_home.get("/alerts")
def get_home_alerts():
    return ok(
        {
            "idle_ready": False,
            "battle_rewards_ready": False,
            "pending_trade_count": 0,
            "unclaimed_rewards_count": 0,
            "new_unlocks_count": 0,
        }
    )

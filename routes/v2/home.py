from fastapi import APIRouter, Depends

from auth import get_current_user
from core.http import ok
from database import db_cursor

router_v2_home = APIRouter(prefix="/v2/home", tags=["v2-home"])


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_trainer_region(cursor, user_id: int) -> dict | None:
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
        LEFT JOIN user_profiles up ON up.user_id = u.id
        LEFT JOIN regions r ON r.id = up.region_start_id
        WHERE u.id = %s
        LIMIT 1
        """,
        (user_id,),
    )
    trainer = cursor.fetchone()
    if not trainer:
        return None

    trainer = dict(trainer)
    if trainer["active_region_id"]:
        return trainer

    cursor.execute(
        """
        SELECT id, code, name
        FROM regions
        WHERE is_active = TRUE
        ORDER BY display_order ASC, id ASC
        LIMIT 1
        """
    )
    fallback_region = cursor.fetchone()
    if fallback_region:
        trainer["active_region_id"] = fallback_region["id"]
        trainer["active_region_code"] = fallback_region["code"]
        trainer["active_region_name"] = fallback_region["name"]
    return trainer


def _team_members_payload(cursor, user_id: int) -> dict:
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM team_presets
        WHERE user_id = %s
          AND is_active = TRUE
        """,
        (user_id,),
    )
    has_active_team = _safe_int(cursor.fetchone()["total"]) > 0

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
        base_total = sum(
            _safe_int(member[field_name])
            for field_name in (
                "base_hp",
                "base_attack",
                "base_defense",
                "base_sp_attack",
                "base_sp_defense",
                "base_speed",
            )
        )
        power_score += _safe_int(member["level"], 1) * 12 + int(base_total / 8)
        for field_name in (
            "base_hp",
            "base_attack",
            "base_defense",
            "base_sp_attack",
            "base_sp_defense",
            "base_speed",
            "slot_order",
        ):
            member.pop(field_name, None)

    return {
        "power_score": power_score,
        "member_count": len(members),
        "members": members,
    }


def _progress_payload(cursor, user_id: int, active_region_id: int | None) -> dict:
    if not active_region_id:
        return {
            "current_region_completion_pct": 0,
            "unlocked_zones": 0,
            "completed_gyms": 0,
            "next_gym": None,
        }

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
        (user_id, active_region_id),
    )
    gym_progress = dict(cursor.fetchone())
    total_gyms = _safe_int(gym_progress["total_gyms"])
    completed_gyms = _safe_int(gym_progress["completed_gyms"])
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
    unlocked_zones = _safe_int(cursor.fetchone()["unlocked_zones"])

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
        (user_id, active_region_id),
    )
    next_gym = cursor.fetchone()

    return {
        "current_region_completion_pct": completion_pct,
        "unlocked_zones": unlocked_zones,
        "completed_gyms": completed_gyms,
        "next_gym": dict(next_gym) if next_gym else None,
    }


def _adventure_payload(cursor, active_region_id: int | None) -> dict:
    if not active_region_id:
        return {
            "current_zone": None,
            "next_action": {
                "type": "start_adventure",
                "label": "Comenzar aventura",
            },
        }

    cursor.execute(
        """
        SELECT
            z.id,
            z.code,
            z.name,
            z.biome_code AS biome,
            z.level_min AS recommended_level_min,
            z.level_max AS recommended_level_max
        FROM zones z
        WHERE z.region_id = %s
          AND z.is_active = TRUE
        ORDER BY z.display_order ASC, z.id ASC
        LIMIT 1
        """,
        (active_region_id,),
    )
    current_zone = cursor.fetchone()

    return {
        "current_zone": dict(current_zone) if current_zone else None,
        "next_action": {
            "type": "continue_zone" if current_zone else "start_adventure",
            "label": "Continuar aventura" if current_zone else "Comenzar aventura",
        },
    }


def _collection_milestones(cursor, user_id: int) -> dict:
    cursor.execute(
        """
        SELECT COUNT(DISTINCT species_id) AS owned_species
        FROM user_pokemon
        WHERE user_id = %s
        """,
        (user_id,),
    )
    collection_owned = _safe_int(cursor.fetchone()["owned_species"])

    cursor.execute(
        """
        SELECT
            huc.code,
            huc.storage_capacity AS next_storage_capacity
        FROM player_houses ph
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
        (user_id,),
    )
    next_house_upgrade = cursor.fetchone()

    return {
        "collection_owned": collection_owned,
        # The current schema does not store a separate "seen" ledger yet.
        "collection_seen": collection_owned,
        "next_house_upgrade": {
            "code": next_house_upgrade["code"],
            "storage_capacity": next_house_upgrade["next_storage_capacity"],
        }
        if next_house_upgrade and next_house_upgrade["code"]
        else None,
    }


def _alerts_payload(cursor, user_id: int) -> dict:
    cursor.execute(
        """
        SELECT COUNT(*) AS pending_trade_count
        FROM trade_offers
        WHERE created_by_user_id = %s
          AND status = 'open'
          AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """,
        (user_id,),
    )
    pending_trade_count = _safe_int(cursor.fetchone()["pending_trade_count"])

    return {
        "idle_ready": False,
        "battle_rewards_ready": False,
        "pending_trade_count": pending_trade_count,
        "unclaimed_rewards_count": 0,
    }


@router_v2_home.get("/summary")
def get_home_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        trainer = _resolve_trainer_region(cursor, current_user["id"])
        team_summary = _team_members_payload(cursor, current_user["id"])
        milestones = _collection_milestones(cursor, current_user["id"])
        progress = _progress_payload(cursor, current_user["id"], trainer["active_region_id"] if trainer else None)
        adventure = _adventure_payload(cursor, trainer["active_region_id"] if trainer else None)
        alerts = _alerts_payload(cursor, current_user["id"])

    trainer_level = max(1, progress["completed_gyms"] + max(team_summary["member_count"], 1))
    trainer_exp = milestones["collection_owned"] * 100 + progress["completed_gyms"] * 750

    trainer_payload = {
        "id": current_user["id"],
        "display_name": trainer["display_name"] if trainer and trainer["display_name"] else current_user.get("display_name") or current_user["email"].split("@")[0],
        "avatar_url": trainer["photo_url"] if trainer else current_user.get("photo_url"),
        "avatar_code": trainer["avatar_code"] if trainer and trainer["avatar_code"] else current_user.get("avatar_code"),
        "team": trainer["trainer_team"] if trainer and trainer["trainer_team"] else current_user.get("trainer_team") or "neutral",
        "trainer_level": trainer_level,
        "trainer_exp": trainer_exp,
        "next_level_exp": (trainer_level + 1) * 1000,
        "active_region": {
            "id": trainer["active_region_id"] if trainer else None,
            "code": trainer["active_region_code"] if trainer else None,
            "name": trainer["active_region_name"] if trainer else None,
        },
    }

    return ok(
        {
            "trainer": trainer_payload,
            "progress": progress,
            "adventure": adventure,
            "team_summary": team_summary,
            "alerts": alerts,
            "milestones": milestones,
        }
    )


@router_v2_home.get("/alerts")
def get_home_alerts(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        alerts = _alerts_payload(cursor, current_user["id"])

    return ok(
        {
            **alerts,
            "new_unlocks_count": 0,
        }
    )


@router_v2_home.get("/rewards")
def get_home_rewards(current_user: dict = Depends(get_current_user)):
    _ = current_user
    return ok({"items": []})

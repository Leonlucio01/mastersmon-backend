import json

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_house = APIRouter(prefix="/v2/house", tags=["v2-house"])


class StorePokemonPayload(BaseModel):
    user_pokemon_id: int
    featured: bool = False


def _get_house(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            ph.id,
            ph.user_id,
            ph.house_name,
            ph.status,
            ph.current_upgrade_id,
            ph.storage_capacity,
            ph.active_habitat_code,
            ph.theme_code,
            ph.created_at,
            ph.updated_at,
            huc.code AS current_upgrade_code,
            huc.name AS current_upgrade_name,
            huc.level_required,
            huc.habitat_slots,
            huc.encounter_bonus_json,
            c.code AS price_currency_code,
            c.name AS price_currency_name,
            huc.price_amount
        FROM player_houses ph
        LEFT JOIN house_upgrade_catalog huc ON huc.id = ph.current_upgrade_id
        LEFT JOIN currencies c ON c.id = huc.price_currency_id
        WHERE ph.user_id = %s
        LIMIT 1
        """,
        (user_id,),
    )
    return cursor.fetchone()


def _storage_rows(cursor, house_id: int, limit: int | None = None):
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    cursor.execute(
        f"""
        SELECT
            hs.id,
            hs.slot_index,
            hs.stored_at,
            hs.is_featured,
            up.id AS user_pokemon_id,
            up.species_id,
            up.level,
            up.is_shiny,
            up.is_favorite,
            COALESCE(NULLIF(up.nickname, ''), ps.name) AS display_name,
            COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant_code,
            COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
            COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
        FROM house_storage hs
        JOIN user_pokemon up ON up.id = hs.user_pokemon_id
        JOIN pokemon_species ps ON ps.id = up.species_id
        LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
        LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
        WHERE hs.house_id = %s
        ORDER BY hs.is_featured DESC, hs.slot_index ASC NULLS LAST, hs.stored_at DESC
        {limit_sql}
        """,
        (house_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


@router_v2_house.get("/summary")
def get_house_summary(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        house = _get_house(cursor, current_user["id"])
        if not house:
            fail("HOUSE_NOT_FOUND", "La casa del usuario no existe.", 404)

        cursor.execute(
            """
            SELECT COUNT(*) AS stored_count
            FROM house_storage
            WHERE house_id = %s
            """,
            (house["id"],),
        )
        stored_count = cursor.fetchone()["stored_count"] or 0

        cursor.execute(
            """
            SELECT COUNT(*) AS featured_count
            FROM house_storage
            WHERE house_id = %s
              AND is_featured = TRUE
            """,
            (house["id"],),
        )
        featured_count = cursor.fetchone()["featured_count"] or 0

        cursor.execute(
            """
            SELECT
                huc.code,
                huc.name,
                huc.storage_capacity,
                huc.level_required,
                huc.price_amount,
                c.code AS price_currency_code,
                c.name AS price_currency_name
            FROM house_upgrade_catalog huc
            LEFT JOIN currencies c ON c.id = huc.price_currency_id
            WHERE huc.is_active = TRUE
              AND huc.storage_capacity > %s
            ORDER BY huc.storage_capacity ASC, huc.id ASC
            LIMIT 1
            """,
            (house["storage_capacity"],),
        )
        next_upgrade = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                her.species_id,
                ps.name,
                her.chance_weight,
                her.min_level,
                her.max_level,
                COALESCE(pv.code, 'normal') AS variant_code,
                COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
            FROM house_encounter_rules her
            JOIN pokemon_species ps ON ps.id = her.species_id
            LEFT JOIN pokemon_variants pv ON pv.id = her.variant_id
            LEFT JOIN pokemon_forms pf
                ON pf.species_id = ps.id
               AND pf.is_default = TRUE
            WHERE her.house_id = %s
              AND her.is_active = TRUE
            ORDER BY her.chance_weight DESC, her.id ASC
            LIMIT 4
            """,
            (house["id"],),
        )
        encounter_preview = [dict(row) for row in cursor.fetchall()]

        featured_pokemon = _storage_rows(cursor, house["id"], limit=6)

    house_payload = dict(house)
    encounter_bonus_json = house_payload.get("encounter_bonus_json")
    if isinstance(encounter_bonus_json, str):
        house_payload["encounter_bonus_json"] = json.loads(encounter_bonus_json or "{}")

    return ok(
        {
            "house": house_payload,
            "storage": {
                "capacity": house["storage_capacity"],
                "stored_count": stored_count,
                "free_slots": max(0, house["storage_capacity"] - stored_count),
                "usage_pct": int((stored_count / house["storage_capacity"]) * 100) if house["storage_capacity"] else 0,
                "featured_count": featured_count,
            },
            "next_upgrade": dict(next_upgrade) if next_upgrade else None,
            "featured_pokemon": featured_pokemon,
            "encounter_preview": encounter_preview,
        }
    )


@router_v2_house.get("/storage")
def get_house_storage(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        house = _get_house(cursor, current_user["id"])
        if not house:
            fail("HOUSE_NOT_FOUND", "La casa del usuario no existe.", 404)

        items = _storage_rows(cursor, house["id"])

    return ok(
        {
            "house_id": house["id"],
            "capacity": house["storage_capacity"],
            "stored_count": len(items),
            "items": items,
        }
    )


@router_v2_house.get("/upgrades")
def get_house_upgrades(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        house = _get_house(cursor, current_user["id"])
        if not house:
            fail("HOUSE_NOT_FOUND", "La casa del usuario no existe.", 404)

        cursor.execute(
            """
            SELECT
                huc.id,
                huc.code,
                huc.name,
                huc.level_required,
                huc.storage_capacity,
                huc.habitat_slots,
                huc.price_amount,
                huc.encounter_bonus_json,
                huc.is_active,
                c.code AS price_currency_code,
                c.name AS price_currency_name,
                CASE WHEN huc.id = %s THEN TRUE ELSE FALSE END AS is_current,
                CASE WHEN huc.storage_capacity > %s THEN TRUE ELSE FALSE END AS is_next_or_higher
            FROM house_upgrade_catalog huc
            LEFT JOIN currencies c ON c.id = huc.price_currency_id
            WHERE huc.is_active = TRUE
            ORDER BY huc.storage_capacity ASC, huc.id ASC
            """,
            (house["current_upgrade_id"], house["storage_capacity"]),
        )
        upgrades = [dict(row) for row in cursor.fetchall()]

    for upgrade in upgrades:
        if isinstance(upgrade.get("encounter_bonus_json"), str):
            upgrade["encounter_bonus_json"] = json.loads(upgrade["encounter_bonus_json"] or "{}")

    return ok(
        {
            "current_upgrade_id": house["current_upgrade_id"],
            "items": upgrades,
        }
    )


@router_v2_house.post("/storage")
def store_pokemon_in_house(payload: StorePokemonPayload, current_user: dict = Depends(get_current_user)):
    with db_cursor(commit=True) as (_, cursor):
        house = _get_house(cursor, current_user["id"])
        if not house:
            fail("HOUSE_NOT_FOUND", "La casa del usuario no existe.", 404)

        cursor.execute(
            """
            SELECT COUNT(*) AS stored_count
            FROM house_storage
            WHERE house_id = %s
            """,
            (house["id"],),
        )
        stored_count = cursor.fetchone()["stored_count"] or 0
        if stored_count >= house["storage_capacity"]:
            fail("HOUSE_STORAGE_FULL", "La casa no tiene más espacio disponible.", 400)

        cursor.execute(
            """
            SELECT id, is_team_locked
            FROM user_pokemon
            WHERE id = %s
              AND user_id = %s
            LIMIT 1
            """,
            (payload.user_pokemon_id, current_user["id"]),
        )
        pokemon = cursor.fetchone()
        if not pokemon:
            fail("POKEMON_NOT_FOUND", "Ese Pokémon no pertenece al usuario.", 404)
        if pokemon["is_team_locked"]:
            fail("POKEMON_TEAM_LOCKED", "Ese Pokémon está en el equipo activo.", 400)

        cursor.execute(
            """
            SELECT 1
            FROM house_storage
            WHERE house_id = %s
              AND user_pokemon_id = %s
            LIMIT 1
            """,
            (house["id"], payload.user_pokemon_id),
        )
        if cursor.fetchone():
            fail("POKEMON_ALREADY_STORED", "Ese Pokémon ya está almacenado en la casa.", 400)

        cursor.execute(
            """
            SELECT COALESCE(MAX(slot_index), 0) + 1 AS next_slot
            FROM house_storage
            WHERE house_id = %s
            """,
            (house["id"],),
        )
        next_slot = cursor.fetchone()["next_slot"] or 1

        cursor.execute(
            """
            INSERT INTO house_storage (
                house_id,
                user_pokemon_id,
                slot_index,
                is_featured
            )
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (house["id"], payload.user_pokemon_id, next_slot, payload.featured),
        )
        storage_id = cursor.fetchone()["id"]

    return ok(
        {
            "stored": True,
            "storage_id": storage_id,
            "slot_index": next_slot,
        }
    )

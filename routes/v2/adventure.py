import random

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import get_current_user
from core.encounters import create_encounter, delete_encounter, get_encounter
from core.http import fail, ok
from database import db_cursor

router_v2_adventure = APIRouter(prefix="/v2/adventure", tags=["v2-adventure"])

BALL_BONUS = {
    "poke_ball": 1.0,
    "super_ball": 1.5,
    "ultra_ball": 2.0,
    "master_ball": 255.0,
}


class CreateEncounterPayload(BaseModel):
    mode: str = "walk"


class CaptureEncounterPayload(BaseModel):
    ball_item_code: str


def _get_current_region(cursor, user_id: int):
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


def _is_region_unlocked(region_display_order: int, current_display_order: int | None) -> bool:
    if current_display_order is None:
        return region_display_order == 1
    return region_display_order <= current_display_order


def _featured_species(cursor, zone_id: int, limit: int = 3):
    cursor.execute(
        """
        SELECT DISTINCT ON (ps.id)
            ps.id AS species_id,
            ps.name,
            ps.normal_asset_path AS asset_url,
            ze.can_be_shiny,
            ze.chance_weight
        FROM zone_encounters ze
        JOIN pokemon_species ps ON ps.id = ze.species_id
        WHERE ze.zone_id = %s
          AND ze.is_active = TRUE
        ORDER BY ps.id, ze.chance_weight DESC
        """,
        (zone_id,),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    rows.sort(key=lambda row: row["chance_weight"], reverse=True)
    return rows[:limit]


def _available_balls(cursor, user_id: int):
    cursor.execute(
        """
        SELECT
            ic.code AS item_code,
            ic.name,
            ui.quantity
        FROM user_inventory ui
        JOIN item_catalog ic ON ic.id = ui.item_id
        WHERE ui.user_id = %s
          AND ui.quantity > 0
          AND ic.code IN ('poke_ball', 'super_ball', 'ultra_ball', 'master_ball')
        ORDER BY ui.quantity DESC, ic.id ASC
        """,
        (user_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


@router_v2_adventure.get("/regions")
def list_regions(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        current_region = _get_current_region(cursor, current_user["id"])
        current_display_order = current_region["display_order"] if current_region else None
        current_region_id = current_region["region_id"] if current_region else None

        cursor.execute(
            """
            SELECT
                r.id,
                r.code,
                r.name,
                r.generation_id,
                r.card_asset_path,
                r.display_order,
                COUNT(DISTINCT z.id) AS zone_count,
                COUNT(DISTINCT g.id) AS total_gyms,
                COUNT(DISTINCT CASE WHEN ugp.completed THEN g.id END) AS completed_gyms
            FROM regions r
            LEFT JOIN zones z ON z.region_id = r.id AND z.is_active = TRUE
            LEFT JOIN gyms g ON g.region_id = r.id AND g.is_active = TRUE
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE r.is_active = TRUE
            GROUP BY r.id, r.code, r.name, r.generation_id, r.card_asset_path, r.display_order
            ORDER BY r.display_order ASC, r.id ASC
            """,
            (current_user["id"],),
        )
        rows = cursor.fetchall()

    payload = []
    for row in rows:
        item = dict(row)
        total_gyms = item["total_gyms"] or 0
        completed_gyms = item["completed_gyms"] or 0
        item["is_unlocked"] = _is_region_unlocked(item["display_order"], current_display_order)
        item["is_active_region"] = item["id"] == current_region_id
        item["completion_pct"] = int((completed_gyms / total_gyms) * 100) if total_gyms else 0
        item.pop("display_order", None)
        payload.append(item)

    return ok(payload)


@router_v2_adventure.get("/regions/{region_code}")
def get_region_detail(region_code: str, current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        current_region = _get_current_region(cursor, current_user["id"])
        current_display_order = current_region["display_order"] if current_region else None

        cursor.execute(
            """
            SELECT *
            FROM regions
            WHERE code = %s
              AND is_active = TRUE
            LIMIT 1
            """,
            (region_code,),
        )
        region = cursor.fetchone()

        if not region:
            fail("REGION_NOT_FOUND", "La región no existe.", 404)

        if not _is_region_unlocked(region["display_order"], current_display_order):
            fail("REGION_LOCKED", "Esta región aún no está desbloqueada.", 403)

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_gyms,
                COUNT(CASE WHEN ugp.completed THEN 1 END) AS completed_gyms
            FROM gyms g
            LEFT JOIN user_gym_progress ugp
                ON ugp.gym_id = g.id
               AND ugp.user_id = %s
            WHERE g.region_id = %s
              AND g.is_active = TRUE
            """,
            (current_user["id"], region["id"]),
        )
        progress = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                g.id,
                g.code,
                g.name,
                g.recommended_level,
                g.primary_type_code
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
            (current_user["id"], region["id"]),
        )
        next_gym = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                z.id,
                z.code,
                z.name,
                z.biome_code,
                z.card_asset_path,
                z.level_min,
                z.level_max,
                z.display_order
            FROM zones z
            WHERE z.region_id = %s
              AND z.is_active = TRUE
            ORDER BY z.display_order ASC, z.id ASC
            """,
            (region["id"],),
        )
        zones = [dict(row) for row in cursor.fetchall()]

        zone_payload = []
        for zone in zones:
            featured = _featured_species(cursor, zone["id"], limit=3)
            zone_payload.append(
                {
                    "id": zone["id"],
                    "code": zone["code"],
                    "name": zone["name"],
                    "biome": zone["biome_code"],
                    "theme_code": region["theme_code"],
                    "level_min": zone["level_min"],
                    "level_max": zone["level_max"],
                    "sort_order": zone["display_order"],
                    "is_unlocked": True,
                    "is_current": zone["display_order"] == min(z["display_order"] for z in zones) if zones else False,
                    "encounter_species_count": len(featured),
                    "featured_species": [
                        {
                            "species_id": item["species_id"],
                            "name": item["name"],
                            "asset_url": item["asset_url"],
                        }
                        for item in featured
                    ],
                }
            )

    total_gyms = progress["total_gyms"] or 0
    completed_gyms = progress["completed_gyms"] or 0
    return ok(
        {
            "region": {
                "id": region["id"],
                "code": region["code"],
                "name": region["name"],
                "description": region["description"],
                "completion_pct": int((completed_gyms / total_gyms) * 100) if total_gyms else 0,
                "completed_gyms": completed_gyms,
                "total_gyms": total_gyms,
            },
            "next_gym": dict(next_gym) if next_gym else None,
            "zones": zone_payload,
        }
    )


@router_v2_adventure.get("/zones/{zone_code}")
def get_zone_detail(zone_code: str, current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        current_region = _get_current_region(cursor, current_user["id"])
        current_display_order = current_region["display_order"] if current_region else None

        cursor.execute(
            """
            SELECT
                z.id,
                z.code,
                z.name,
                z.region_id,
                z.biome_code,
                z.level_min,
                z.level_max,
                z.card_asset_path,
                z.scenario_asset_path,
                z.music_asset_path,
                z.display_order,
                r.code AS region_code,
                r.display_order AS region_display_order
            FROM zones z
            JOIN regions r ON r.id = z.region_id
            WHERE z.code = %s
              AND z.is_active = TRUE
            LIMIT 1
            """,
            (zone_code,),
        )
        zone = cursor.fetchone()

        if not zone:
            fail("ZONE_NOT_FOUND", "La zona no existe.", 404)

        if not _is_region_unlocked(zone["region_display_order"], current_display_order):
            fail("ZONE_LOCKED", "Esta zona aún no está desbloqueada.", 403)

        featured = _featured_species(cursor, zone["id"], limit=5)

    return ok(
        {
            "zone": {
                "id": zone["id"],
                "code": zone["code"],
                "name": zone["name"],
                "region_code": zone["region_code"],
                "biome": zone["biome_code"],
                "theme_code": None,
                "level_min": zone["level_min"],
                "level_max": zone["level_max"],
                "card_asset_path": zone["card_asset_path"],
                "map_asset_path": zone["scenario_asset_path"],
                "music_asset_path": zone["music_asset_path"],
            },
            "encounter_preview": {
                "species_count": len(featured),
                "featured": [
                    {
                        "species_id": item["species_id"],
                        "name": item["name"],
                        "can_be_shiny": item["can_be_shiny"],
                        "asset_url": item["asset_url"],
                    }
                    for item in featured
                ],
            },
            "player_state": {
                "is_unlocked": True,
                "recommended_team_power": zone["level_max"] * 60,
                "last_encounter_at": None,
            },
        }
    )


@router_v2_adventure.post("/zones/{zone_code}/encounters")
def create_zone_encounter(
    zone_code: str,
    payload: CreateEncounterPayload,
    current_user: dict = Depends(get_current_user),
):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT z.id, z.code, z.name, z.level_min, z.level_max, z.scenario_asset_path
            FROM zones z
            WHERE z.code = %s
              AND z.is_active = TRUE
            LIMIT 1
            """,
            (zone_code,),
        )
        zone = cursor.fetchone()
        if not zone:
            fail("ZONE_NOT_FOUND", "La zona no existe.", 404)

        cursor.execute(
            """
            SELECT
                ze.id,
                ze.species_id,
                ze.level_min,
                ze.level_max,
                ze.can_be_shiny,
                ze.chance_weight,
                ps.name,
                ps.normal_asset_path,
                ps.shiny_asset_path,
                ps.base_hp,
                ps.base_attack,
                ps.base_defense,
                ps.base_sp_attack,
                ps.base_sp_defense,
                ps.base_speed,
                ps.catch_rate
            FROM zone_encounters ze
            JOIN pokemon_species ps ON ps.id = ze.species_id
            WHERE ze.zone_id = %s
              AND ze.is_active = TRUE
              AND ze.encounter_method = %s::game.encounter_method_enum
            ORDER BY ze.id ASC
            """,
            (zone["id"], payload.mode),
        )
        candidates = [dict(row) for row in cursor.fetchall()]
        if not candidates:
            fail("NO_ENCOUNTERS", "La zona no tiene encuentros configurados.", 404)

        picked = random.choices(
            candidates,
            weights=[float(item["chance_weight"]) for item in candidates],
            k=1,
        )[0]

        is_shiny = bool(picked["can_be_shiny"]) and random.random() <= 0.01
        level = random.randint(picked["level_min"], picked["level_max"])
        balls = _available_balls(cursor, current_user["id"])

    encounter = create_encounter(
        {
            "user_id": current_user["id"],
            "zone_id": zone["id"],
            "zone_code": zone["code"],
            "species_id": picked["species_id"],
            "level": level,
            "variant": "shiny" if is_shiny else "normal",
            "is_shiny": is_shiny,
            "catch_rate": picked["catch_rate"] or 45,
            "asset_url": picked["shiny_asset_path"] if is_shiny else picked["normal_asset_path"],
            "name": picked["name"],
            "stats": {
                "hp": picked["base_hp"],
                "attack": picked["base_attack"],
                "defense": picked["base_defense"],
                "sp_attack": picked["base_sp_attack"],
                "sp_defense": picked["base_sp_defense"],
                "speed": picked["base_speed"],
            },
        }
    )

    return ok(
        {
            "encounter": {
                "id": encounter["id"],
                "zone_id": encounter["zone_id"],
                "species_id": encounter["species_id"],
                "name": encounter["name"],
                "level": encounter["level"],
                "variant": encounter["variant"],
                "is_shiny": encounter["is_shiny"],
                "asset_url": encounter["asset_url"],
                "stats": encounter["stats"],
                "capture": {
                    "available_balls": [
                        {
                            "item_code": ball["item_code"],
                            "quantity": ball["quantity"],
                            "capture_rate_pct": min(
                                100,
                                int(((encounter["catch_rate"] / 255.0) * BALL_BONUS.get(ball["item_code"], 1.0)) * 100),
                            ),
                        }
                        for ball in balls
                    ],
                },
            }
        }
    )


@router_v2_adventure.post("/encounters/{encounter_id}/capture")
def capture_zone_encounter(
    encounter_id: str,
    payload: CaptureEncounterPayload,
    current_user: dict = Depends(get_current_user),
):
    encounter = get_encounter(encounter_id)
    if not encounter or encounter["user_id"] != current_user["id"]:
        fail("ENCOUNTER_NOT_FOUND", "El encuentro ya no está disponible.", 404)

    ball_item_code = payload.ball_item_code.strip().lower()
    ball_bonus = BALL_BONUS.get(ball_item_code)
    if not ball_bonus:
        fail("BALL_NOT_SUPPORTED", "La ball seleccionada no es válida.", 400)

    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT ui.id, ui.quantity, ic.id AS item_id, ic.code
            FROM user_inventory ui
            JOIN item_catalog ic ON ic.id = ui.item_id
            WHERE ui.user_id = %s
              AND ic.code = %s
            LIMIT 1
            """,
            (current_user["id"], ball_item_code),
        )
        inventory_ball = cursor.fetchone()
        if not inventory_ball or inventory_ball["quantity"] <= 0:
            fail("BALL_NOT_AVAILABLE", "No tienes esa ball disponible.", 400)

        cursor.execute(
            """
            UPDATE user_inventory
            SET quantity = quantity - 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (inventory_ball["id"],),
        )

        capture_pct = min(95, max(5, int(((encounter["catch_rate"] / 255.0) * ball_bonus) * 100)))
        if ball_item_code == "master_ball":
            captured = True
        else:
            captured = random.randint(1, 100) <= capture_pct

        if not captured:
            delete_encounter(encounter_id)
            return ok(
                {
                    "captured": False,
                    "message": "El Pokémon escapó.",
                    "reward_summary": {
                        "exp_gained": 0,
                        "collection_updated": False,
                    },
                    "captured_pokemon": None,
                }
            )

        cursor.execute(
            """
            SELECT id
            FROM pokemon_forms
            WHERE species_id = %s
              AND is_default = TRUE
            ORDER BY id ASC
            LIMIT 1
            """,
            (encounter["species_id"],),
        )
        default_form = cursor.fetchone()

        cursor.execute(
            """
            SELECT id
            FROM pokemon_variants
            WHERE code = %s
              AND is_active = TRUE
            LIMIT 1
            """,
            ("shiny" if encounter["is_shiny"] else "normal",),
        )
        variant = cursor.fetchone()

        cursor.execute(
            """
            INSERT INTO user_pokemon (
                user_id,
                species_id,
                form_id,
                variant_id,
                nickname,
                level,
                exp_total,
                is_shiny,
                origin_type,
                origin_ref
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                NULL,
                %s,
                0,
                %s,
                'capture',
                %s
            )
            RETURNING id
            """,
            (
                current_user["id"],
                encounter["species_id"],
                default_form["id"] if default_form else None,
                variant["id"] if variant else None,
                encounter["level"],
                encounter["is_shiny"],
                encounter["zone_code"],
            ),
        )
        created_user_pokemon = cursor.fetchone()
        user_pokemon_id = created_user_pokemon["id"]

        cursor.execute(
            """
            SELECT move_id, learn_method, COALESCE(required_level, 1) AS required_level
            FROM pokemon_species_moves
            WHERE species_id = %s
              AND is_active = TRUE
              AND (
                    is_starting_move = TRUE
                 OR (learn_method = 'level' AND COALESCE(required_level, 1) <= %s)
              )
            ORDER BY is_starting_move DESC, required_level ASC, learn_order ASC
            LIMIT 12
            """,
            (encounter["species_id"], encounter["level"]),
        )
        moves = [dict(row) for row in cursor.fetchall()]

        created_move_ids = []
        for move in moves:
            cursor.execute(
                """
                INSERT INTO user_pokemon_moves (
                    user_pokemon_id,
                    move_id,
                    unlocked_by,
                    unlocked_at_level
                )
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (
                    user_pokemon_id,
                    move["move_id"],
                    move["learn_method"],
                    move["required_level"],
                ),
            )
            created_move_ids.append(cursor.fetchone()["id"])

        for slot_order, user_pokemon_move_id in enumerate(created_move_ids[:4], start=1):
            cursor.execute(
                """
                INSERT INTO user_pokemon_move_set (
                    user_pokemon_id,
                    user_pokemon_move_id,
                    slot_order
                )
                VALUES (%s, %s, %s)
                """,
                (user_pokemon_id, user_pokemon_move_id, slot_order),
            )

    delete_encounter(encounter_id)
    return ok(
        {
            "captured": True,
            "message": "Pokémon capturado correctamente.",
            "reward_summary": {
                "exp_gained": encounter["level"] * 10,
                "collection_updated": True,
            },
            "captured_pokemon": {
                "user_pokemon_id": user_pokemon_id,
                "species_id": encounter["species_id"],
                "name": encounter["name"],
                "variant": encounter["variant"],
            },
        }
    )

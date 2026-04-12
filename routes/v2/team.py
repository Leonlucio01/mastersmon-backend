from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_team = APIRouter(prefix="/v2/team", tags=["v2-team"])


class SaveActiveTeamPayload(BaseModel):
    user_pokemon_ids: list[int]
    team_name: str | None = None


def _fetch_team_members(cursor, team_preset_id: int):
    cursor.execute(
        """
        SELECT
            tpm.slot_order,
            up.id AS user_pokemon_id,
            up.species_id,
            COALESCE(NULLIF(up.nickname, ''), ps.name) AS display_name,
            up.level,
            up.is_shiny,
            up.is_favorite,
            up.is_team_locked,
            COALESCE(pv.code, CASE WHEN up.is_shiny THEN 'shiny' ELSE 'normal' END) AS variant,
            COALESCE(pf.code, CONCAT('default-', ps.id)) AS form_code,
            COALESCE(pf.asset_normal_path, ps.normal_asset_path) AS asset_url
        FROM team_preset_members tpm
        JOIN user_pokemon up ON up.id = tpm.user_pokemon_id
        JOIN pokemon_species ps ON ps.id = up.species_id
        LEFT JOIN pokemon_forms pf ON pf.id = up.form_id
        LEFT JOIN pokemon_variants pv ON pv.id = up.variant_id
        WHERE tpm.team_preset_id = %s
        ORDER BY tpm.slot_order ASC
        """,
        (team_preset_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _ensure_active_team(cursor, user_id: int):
    cursor.execute(
        """
        SELECT id, code, name, is_active
        FROM team_presets
        WHERE user_id = %s
          AND is_active = TRUE
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (user_id,),
    )
    active = cursor.fetchone()
    if active:
        return dict(active)

    cursor.execute(
        """
        INSERT INTO team_presets (user_id, code, name, is_active)
        VALUES (%s, 'active', 'Active Team', TRUE)
        RETURNING id, code, name, is_active
        """,
        (user_id,),
    )
    created = dict(cursor.fetchone())

    cursor.execute(
        """
        WITH ordered_pokemon AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY level DESC, id ASC) AS slot_order
            FROM user_pokemon
            WHERE user_id = %s
              AND is_locked = FALSE
            ORDER BY level DESC, id ASC
            LIMIT 6
        )
        INSERT INTO team_preset_members (team_preset_id, user_pokemon_id, slot_order)
        SELECT %s, op.id, op.slot_order
        FROM ordered_pokemon op
        """,
        (user_id, created["id"]),
    )
    return created


@router_v2_team.get("/active")
def get_active_team(current_user: dict = Depends(get_current_user)):
    with db_cursor(commit=True) as (_, cursor):
        active = _ensure_active_team(cursor, current_user["id"])
        members = _fetch_team_members(cursor, active["id"])

    return ok(
        {
            "team": active,
            "member_count": len(members),
            "members": members,
            "warnings": ["Tu equipo tiene menos de 6 Pokémon."] if len(members) < 6 else [],
        }
    )


@router_v2_team.get("/presets")
def get_team_presets(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT id, code, name, is_active, created_at, updated_at
            FROM team_presets
            WHERE user_id = %s
            ORDER BY is_active DESC, updated_at DESC, id DESC
            """,
            (current_user["id"],),
        )
        presets = [dict(row) for row in cursor.fetchall()]

        for preset in presets:
            preset["members"] = _fetch_team_members(cursor, preset["id"])
            preset["member_count"] = len(preset["members"])

    return ok({"items": presets})


@router_v2_team.post("/active")
def save_active_team(payload: SaveActiveTeamPayload, current_user: dict = Depends(get_current_user)):
    unique_ids = []
    for pokemon_id in payload.user_pokemon_ids:
        if pokemon_id not in unique_ids:
            unique_ids.append(pokemon_id)

    if not unique_ids:
        fail("TEAM_EMPTY", "Debes enviar al menos un Pokémon para el equipo.", 400)

    if len(unique_ids) > 6:
        fail("TEAM_TOO_LARGE", "El equipo no puede tener más de 6 Pokémon.", 400)

    with db_cursor(commit=True) as (_, cursor):
        active = _ensure_active_team(cursor, current_user["id"])

        cursor.execute(
            """
            SELECT id, is_locked
            FROM user_pokemon
            WHERE user_id = %s
              AND id = ANY(%s)
            """,
            (current_user["id"], unique_ids),
        )
        owned_rows = [dict(row) for row in cursor.fetchall()]
        owned_ids = {row["id"] for row in owned_rows}

        if len(owned_ids) != len(unique_ids):
            fail("TEAM_INVALID_MEMBER", "Uno o más Pokémon no pertenecen al usuario.", 400)

        locked_ids = [row["id"] for row in owned_rows if row["is_locked"]]
        if locked_ids:
            fail("TEAM_LOCKED_MEMBER", "Uno o más Pokémon están bloqueados y no pueden equiparse.", 400)

        cursor.execute(
            """
            UPDATE team_presets
            SET is_active = CASE WHEN id = %s THEN TRUE ELSE FALSE END,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
            """,
            (active["id"], current_user["id"]),
        )

        if payload.team_name:
            cursor.execute(
                """
                UPDATE team_presets
                SET name = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (payload.team_name.strip()[:80], active["id"]),
            )

        cursor.execute(
            """
            UPDATE user_pokemon
            SET is_team_locked = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
            """,
            (current_user["id"],),
        )

        cursor.execute(
            """
            DELETE FROM team_preset_members
            WHERE team_preset_id = %s
            """,
            (active["id"],),
        )

        for slot_order, pokemon_id in enumerate(unique_ids, start=1):
            cursor.execute(
                """
                INSERT INTO team_preset_members (team_preset_id, user_pokemon_id, slot_order)
                VALUES (%s, %s, %s)
                """,
                (active["id"], pokemon_id, slot_order),
            )

        cursor.execute(
            """
            UPDATE user_pokemon
            SET is_team_locked = TRUE,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
              AND id = ANY(%s)
            """,
            (current_user["id"], unique_ids),
        )

        cursor.execute(
            """
            SELECT id, code, name, is_active
            FROM team_presets
            WHERE id = %s
            LIMIT 1
            """,
            (active["id"],),
        )
        saved_team = dict(cursor.fetchone())
        members = _fetch_team_members(cursor, saved_team["id"])

    return ok(
        {
            "team": saved_team,
            "member_count": len(members),
            "members": members,
        }
    )

import random
import string

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from auth import get_current_user
from core.http import fail, ok
from database import db_cursor

router_v2_onboarding = APIRouter(prefix="/v2/onboarding", tags=["v2-onboarding"])

STARTER_SPECIES_IDS = [
    1,
    4,
    7,
    152,
    155,
    158,
    252,
    255,
    258,
    387,
    390,
    393,
    495,
    498,
    501,
    650,
    653,
    656,
    722,
    725,
    728,
    810,
    813,
    816,
    906,
    909,
    912,
]

AVATAR_CODES = [
    "batman",
    "bryan",
    "goku",
    "hades",
    "jean",
    "jhonny",
    "leon",
    "nathaly",
    "rafael",
    "steven",
]


class CompleteOnboardingPayload(BaseModel):
    display_name: str = Field(..., min_length=2, max_length=150)
    trainer_team: str = Field(..., pattern="^(red|blue|green|neutral)$")
    starter_species_id: int
    region_code: str = Field(..., min_length=2, max_length=80)
    avatar_code: str = Field(default="steven", min_length=2, max_length=80)
    language_code: str = Field(default="es", min_length=2, max_length=8)
    timezone_code: str = Field(default="America/Lima", min_length=2, max_length=80)


def _generate_trainer_code(cursor) -> str:
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(25):
        candidate = f"MM-{''.join(random.choices(alphabet, k=8))}"
        cursor.execute(
            """
            SELECT 1
            FROM user_profiles
            WHERE trainer_code = %s
            LIMIT 1
            """,
            (candidate,),
        )
        if not cursor.fetchone():
            return candidate
    fail("TRAINER_CODE_UNAVAILABLE", "No se pudo generar el codigo de entrenador.", 500)


def _fetch_starter_options(cursor):
    cursor.execute(
        """
        SELECT
            ps.id,
            ps.pokedex_number,
            ps.name,
            ps.generation_id,
            ps.normal_asset_path AS asset_url,
            pt.code AS primary_type_code,
            pt.name AS primary_type_name
        FROM pokemon_species ps
        LEFT JOIN pokemon_species_types pst
            ON pst.species_id = ps.id
           AND pst.slot_order = 1
        LEFT JOIN pokemon_types pt ON pt.id = pst.type_id
        WHERE ps.id = ANY(%s)
          AND ps.is_active = TRUE
        ORDER BY ps.generation_id ASC, ps.pokedex_number ASC
        """,
        (STARTER_SPECIES_IDS,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _avatar_options():
    return [
        {
            "code": avatar_code,
            "name": avatar_code.capitalize(),
            "asset_url": f"/img/avatars/{avatar_code}.png",
        }
        for avatar_code in AVATAR_CODES
    ]


def _grant_starter_pokemon(cursor, *, user_id: int, species_id: int, display_name: str):
    cursor.execute(
        """
        SELECT
            pf.id AS form_id,
            pf.asset_normal_path,
            ps.name
        FROM pokemon_species ps
        LEFT JOIN pokemon_forms pf
            ON pf.species_id = ps.id
           AND pf.is_default = TRUE
        WHERE ps.id = %s
          AND ps.is_active = TRUE
        LIMIT 1
        """,
        (species_id,),
    )
    starter_species = cursor.fetchone()
    if not starter_species:
        fail("STARTER_NOT_FOUND", "El starter elegido no existe.", 404)

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
            NULL,
            NULL,
            5,
            0,
            FALSE,
            'starter',
            'onboarding'
        )
        RETURNING id
        """,
        (user_id, species_id, starter_species["form_id"]),
    )
    user_pokemon_id = cursor.fetchone()["id"]

    cursor.execute(
        """
        SELECT
            move_id,
            learn_method,
            COALESCE(required_level, 1) AS required_level
        FROM pokemon_species_moves
        WHERE species_id = %s
          AND is_active = TRUE
          AND (
                is_starting_move = TRUE
             OR (learn_method = 'level' AND COALESCE(required_level, 1) <= 5)
          )
        ORDER BY
            CASE WHEN is_starting_move THEN 0 ELSE 1 END,
            COALESCE(required_level, 1) ASC,
            learn_order ASC NULLS LAST,
            id ASC
        LIMIT 4
        """,
        (species_id,),
    )
    learned_moves = [dict(row) for row in cursor.fetchall()]

    learned_move_ids = []
    for move in learned_moves:
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
        learned_move_ids.append(cursor.fetchone()["id"])

    for slot_order, user_pokemon_move_id in enumerate(learned_move_ids[:4], start=1):
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

    cursor.execute(
        """
        SELECT id
        FROM team_presets
        WHERE user_id = %s
          AND is_active = TRUE
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (user_id,),
    )
    active_team = cursor.fetchone()
    if not active_team:
        cursor.execute(
            """
            INSERT INTO team_presets (user_id, code, name, is_active)
            VALUES (%s, 'active', 'Active Team', TRUE)
            RETURNING id
            """,
            (user_id,),
        )
        active_team = cursor.fetchone()

    cursor.execute(
        """
        DELETE FROM team_preset_members
        WHERE team_preset_id = %s
        """,
        (active_team["id"],),
    )
    cursor.execute(
        """
        INSERT INTO team_preset_members (team_preset_id, user_pokemon_id, slot_order)
        VALUES (%s, %s, 1)
        """,
        (active_team["id"], user_pokemon_id),
    )
    cursor.execute(
        """
        UPDATE user_pokemon
        SET is_team_locked = TRUE,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (user_pokemon_id,),
    )

    return {
        "user_pokemon_id": user_pokemon_id,
        "species_id": species_id,
        "species_name": starter_species["name"],
        "asset_url": starter_species["asset_normal_path"],
        "display_name": display_name,
    }


@router_v2_onboarding.get("/state")
def get_onboarding_state(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.display_name,
                up.avatar_code,
                up.trainer_code,
                up.trainer_team,
                up.starter_species_id,
                up.region_start_id,
                up.language_code,
                up.timezone_code,
                up.country_code,
                up.onboarding_completed,
                up.onboarding_step
            FROM user_profiles up
            WHERE up.user_id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        profile = dict(cursor.fetchone())

        cursor.execute(
            """
            SELECT COUNT(*) AS owned_pokemon
            FROM user_pokemon
            WHERE user_id = %s
            """,
            (current_user["id"],),
        )
        owned_pokemon = cursor.fetchone()["owned_pokemon"] or 0

    needs_onboarding = (not profile["onboarding_completed"]) or owned_pokemon == 0
    return ok(
        {
            "needs_onboarding": needs_onboarding,
            "profile": profile,
            "owned_pokemon": owned_pokemon,
        }
    )


@router_v2_onboarding.get("/options")
def get_onboarding_options(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        starter_options = _fetch_starter_options(cursor)
        cursor.execute(
            """
            SELECT
                id,
                code,
                name,
                generation_id,
                card_asset_path,
                description,
                display_order
            FROM regions
            WHERE is_active = TRUE
            ORDER BY display_order ASC, id ASC
            """
        )
        regions = [dict(row) for row in cursor.fetchall()]

    return ok(
        {
            "profile_seed": {
                "email": current_user["email"],
                "display_name": current_user.get("display_name"),
            },
            "trainer_teams": [
                {"code": "red", "name": "Team Red"},
                {"code": "blue", "name": "Team Blue"},
                {"code": "green", "name": "Team Green"},
                {"code": "neutral", "name": "Neutral"},
            ],
            "avatars": _avatar_options(),
            "starters": starter_options,
            "regions": regions,
        }
    )


@router_v2_onboarding.post("/complete")
def complete_onboarding(payload: CompleteOnboardingPayload, current_user: dict = Depends(get_current_user)):
    display_name = payload.display_name.strip()
    if len(display_name) < 2:
        fail("DISPLAY_NAME_INVALID", "El nombre visible es demasiado corto.", 400)
    avatar_code = payload.avatar_code.strip().lower()
    if avatar_code not in AVATAR_CODES:
        fail("AVATAR_INVALID", "El avatar elegido no esta disponible.", 400)

    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT
                up.user_id,
                up.onboarding_completed,
                up.trainer_code
            FROM user_profiles up
            WHERE up.user_id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        profile = cursor.fetchone()
        if not profile:
            fail("PROFILE_NOT_FOUND", "No se encontro el perfil del usuario.", 404)

        cursor.execute(
            """
            SELECT COUNT(*) AS owned_pokemon
            FROM user_pokemon
            WHERE user_id = %s
            """,
            (current_user["id"],),
        )
        owned_pokemon = cursor.fetchone()["owned_pokemon"] or 0

        if profile["onboarding_completed"] and owned_pokemon > 0:
            fail("ONBOARDING_ALREADY_DONE", "El onboarding ya fue completado.", 400)

        cursor.execute(
            """
            SELECT id, code, name
            FROM regions
            WHERE code = %s
              AND is_active = TRUE
            LIMIT 1
            """,
            (payload.region_code.strip().lower(),),
        )
        region = cursor.fetchone()
        if not region:
            fail("REGION_NOT_FOUND", "La region inicial no existe.", 404)

        starter_options = {row["id"]: row for row in _fetch_starter_options(cursor)}
        if payload.starter_species_id not in starter_options:
            fail("STARTER_INVALID", "El starter elegido no esta disponible.", 400)

        trainer_code = profile["trainer_code"] or _generate_trainer_code(cursor)

        cursor.execute(
            """
            UPDATE user_profiles
            SET display_name = %s,
                trainer_code = %s,
                avatar_code = %s,
                trainer_team = %s,
                starter_species_id = %s,
                region_start_id = %s,
                language_code = %s,
                timezone_code = %s,
                onboarding_completed = TRUE,
                onboarding_step = 'complete',
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
            """,
            (
                display_name,
                trainer_code,
                avatar_code,
                payload.trainer_team,
                payload.starter_species_id,
                region["id"],
                payload.language_code.strip()[:8],
                payload.timezone_code.strip()[:80],
                current_user["id"],
            ),
        )

        starter_granted = None
        if owned_pokemon == 0:
            starter_granted = _grant_starter_pokemon(
                cursor,
                user_id=current_user["id"],
                species_id=payload.starter_species_id,
                display_name=display_name,
            )

        cursor.execute(
            """
            SELECT
                up.display_name,
                up.avatar_code,
                up.trainer_code,
                up.trainer_team,
                up.starter_species_id,
                up.region_start_id,
                up.language_code,
                up.timezone_code,
                up.onboarding_completed,
                up.onboarding_step
            FROM user_profiles up
            WHERE up.user_id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        updated_profile = dict(cursor.fetchone())

    return ok(
        {
            "profile": updated_profile,
            "starter_granted": starter_granted,
            "region": {
                "id": region["id"],
                "code": region["code"],
                "name": region["name"],
            },
        }
    )

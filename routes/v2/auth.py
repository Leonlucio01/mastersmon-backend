from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import create_access_token, get_current_user, verify_google_token
from core.http import ok
from database import db_cursor

router_v2_auth = APIRouter(prefix="/v2/auth", tags=["v2-auth"])


class GoogleLoginPayload(BaseModel):
    credential: str


def _bootstrap_user(cursor, user_id: int, display_name: str):
    cursor.execute(
        """
        INSERT INTO user_profiles (
            user_id,
            display_name,
            photo_url,
            avatar_code,
            language_code,
            timezone_code
        )
        VALUES (%s, %s, NULL, 'steven', 'es', 'America/Lima')
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id, display_name),
    )

    cursor.execute(
        """
        INSERT INTO user_settings (user_id)
        VALUES (%s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id,),
    )

    cursor.execute(
        """
        INSERT INTO user_wallets (user_id, currency_id, balance)
        SELECT %s, c.id, 0
        FROM currencies c
        WHERE c.is_active = TRUE
        ON CONFLICT (user_id, currency_id) DO NOTHING
        """,
        (user_id,),
    )

    cursor.execute(
        """
        INSERT INTO player_houses (
            user_id,
            house_name,
            status,
            current_upgrade_id,
            storage_capacity,
            active_habitat_code,
            theme_code
        )
        SELECT
            %s,
            %s,
            'active',
            huc.id,
            COALESCE(huc.storage_capacity, 300),
            'starter',
            'default'
        FROM (
            SELECT id, storage_capacity
            FROM house_upgrade_catalog
            WHERE is_active = TRUE
            ORDER BY storage_capacity ASC, id ASC
            LIMIT 1
        ) huc
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id, f"{display_name}'s House"),
    )


@router_v2_auth.post("/google")
def login_with_google(payload: GoogleLoginPayload):
    info = verify_google_token(payload.credential)
    email = str(info.get("email") or "").strip().lower()
    display_name = str(info.get("name") or email.split("@")[0] or "Trainer").strip()
    picture = str(info.get("picture") or "").strip() or None
    google_id = str(info.get("sub") or "").strip() or None

    with db_cursor(commit=True) as (_, cursor):
        cursor.execute(
            """
            SELECT id, email, role_code
            FROM users
            WHERE email = %s
            LIMIT 1
            """,
            (email,),
        )
        user = cursor.fetchone()

        if user:
            cursor.execute(
                """
                UPDATE users
                SET google_id = COALESCE(%s, google_id),
                    last_login_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (google_id, user["id"]),
            )
            user_id = int(user["id"])
            role_code = str(user["role_code"])
        else:
            cursor.execute(
                """
                INSERT INTO users (
                    google_id,
                    email,
                    role_code,
                    account_status,
                    last_login_at
                )
                VALUES (%s, %s, 'user', 'active', CURRENT_TIMESTAMP)
                RETURNING id, role_code
                """,
                (google_id, email),
            )
            created_user = cursor.fetchone()
            user_id = int(created_user["id"])
            role_code = str(created_user["role_code"])

        _bootstrap_user(cursor, user_id=user_id, display_name=display_name)

        cursor.execute(
            """
            UPDATE user_profiles
            SET display_name = %s,
                photo_url = COALESCE(%s, photo_url),
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s
            """,
            (display_name, picture, user_id),
        )

        cursor.execute(
            """
            SELECT
                u.id,
                u.email,
                u.role_code,
                up.display_name,
                up.avatar_code,
                up.photo_url
            FROM users u
            JOIN user_profiles up ON up.user_id = u.id
            WHERE u.id = %s
            LIMIT 1
            """,
            (user_id,),
        )
        session_user = cursor.fetchone()

    token = create_access_token(user_id=user_id, email=email, role_code=role_code)
    return ok(
        {
            "token": token,
            "user": dict(session_user),
        }
    )


@router_v2_auth.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return ok({"user": current_user})

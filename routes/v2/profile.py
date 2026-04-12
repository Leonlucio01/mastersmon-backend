from fastapi import APIRouter, Depends

from auth import get_current_user
from core.http import ok
from database import db_cursor

router_v2_profile = APIRouter(prefix="/v2/profile", tags=["v2-profile"])


@router_v2_profile.get("/me")
def get_profile(current_user: dict = Depends(get_current_user)):
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                u.id,
                u.email,
                u.role_code,
                u.account_status,
                u.last_login_at,
                up.display_name,
                up.username,
                up.photo_url,
                up.avatar_code,
                up.trainer_code,
                up.trainer_team,
                up.language_code,
                up.timezone_code,
                up.country_code,
                up.bio,
                up.onboarding_completed,
                up.onboarding_step,
                us.music_enabled,
                us.sound_enabled,
                us.vibration_enabled,
                us.notifications_enabled,
                us.maps_audio_minimized,
                us.reduced_motion
            FROM users u
            LEFT JOIN user_profiles up ON up.user_id = u.id
            LEFT JOIN user_settings us ON us.user_id = u.id
            WHERE u.id = %s
            LIMIT 1
            """,
            (current_user["id"],),
        )
        profile = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                c.code,
                c.name,
                uw.balance,
                c.icon_path
            FROM user_wallets uw
            JOIN currencies c ON c.id = uw.currency_id
            WHERE uw.user_id = %s
              AND c.is_active = TRUE
            ORDER BY c.id ASC
            """,
            (current_user["id"],),
        )
        wallets = cursor.fetchall()

    payload = dict(profile)
    payload["wallets"] = [dict(row) for row in wallets]
    return ok(payload)

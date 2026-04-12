import os
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
from jose import JWTError, jwt

from database import db_cursor

JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24 * 7
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()

if not JWT_SECRET:
    raise ValueError("JWT_SECRET es obligatorio y no puede estar vacío.")

security = HTTPBearer()


def verify_google_token(token: str) -> dict:
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GOOGLE_CLIENT_ID no está configurado.",
        )

    try:
        return id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de Google inválido.",
        )


def create_access_token(user_id: int, email: str, role_code: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "email": email,
        "role": role_code,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=JWT_EXPIRE_HOURS)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado.",
        )


def get_user_by_id(user_id: int) -> dict:
    with db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT
                u.id,
                u.email,
                u.google_id,
                u.role_code,
                u.account_status,
                u.last_login_at,
                up.display_name,
                up.username,
                up.photo_url,
                up.avatar_code,
                up.trainer_code,
                up.trainer_team,
                up.starter_species_id,
                up.region_start_id,
                up.language_code,
                up.timezone_code,
                up.country_code,
                up.bio,
                up.onboarding_completed,
                up.onboarding_step
            FROM users u
            LEFT JOIN user_profiles up ON up.user_id = u.id
            WHERE u.id = %s
            LIMIT 1
            """,
            (user_id,),
        )
        user = cursor.fetchone()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado.",
        )

    if user["account_status"] != "active":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La cuenta no está activa.",
        )

    return dict(user)


def obtener_usuario_desde_payload(payload: dict) -> dict:
    try:
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido.",
        )

    return get_user_by_id(user_id)


def get_current_user_from_token(token: str) -> dict:
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token requerido.",
        )

    payload = decode_access_token(token)
    return obtener_usuario_desde_payload(payload)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return get_current_user_from_token(credentials.credentials)

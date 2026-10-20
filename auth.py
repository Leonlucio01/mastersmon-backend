import os
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

from database import get_connection, get_cursor, release_connection

JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24 * 7
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

if not JWT_SECRET:
    raise ValueError("JWT_SECRET es obligatorio y no puede estar vacío.")

security = HTTPBearer()


def verify_google_token(token: str) -> dict:
    try:
        info = id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            GOOGLE_CLIENT_ID
        )
        return info
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de Google inválido"
        )


def create_access_token(user_id: int, correo: str, rol: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "correo": correo,
        "rol": rol,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=JWT_EXPIRE_HOURS)).timestamp())
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado"
        )


def obtener_usuario_desde_payload(payload: dict) -> dict:
    try:
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )

    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT
                id,
                nombre,
                correo,
                foto,
                avatar_url,
                avatar_id,
                rol,
                pokedolares,
                fecha_registro
            FROM usuarios
            WHERE id = %s
        """, (user_id,))

        usuario = cursor.fetchone()

        if not usuario:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario no encontrado"
            )

        return dict(usuario)
    finally:
        cursor.close()
        release_connection(conn)


def get_current_user_from_token(token: str) -> dict:
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token requerido"
        )

    payload = decode_access_token(token)
    return obtener_usuario_desde_payload(payload)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    return get_current_user_from_token(token)

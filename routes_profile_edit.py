"""Mastersmon V2.20.3 - Account-owner-only trainer profile edits."""
import re
import unicodedata

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from routes_pokemon import obtener_usuario_por_id

router_profile_edit = APIRouter()

class ProfileEditPayload(BaseModel):
    nombre: str
    team_color: str


def validate_name(value: str) -> str:
    name = " ".join(value.strip().split())
    if len(name) < 3 or len(name) > 24:
        raise HTTPException(status_code=422, detail="El nombre debe tener entre 3 y 24 caracteres")
    if not all(unicodedata.category(c)[0] in ('L', 'N') or c in ' _-.' for c in name):
        raise HTTPException(status_code=422, detail="El nombre contiene caracteres no permitidos")
    if not any(c.isalpha() for c in name):
        raise HTTPException(status_code=422, detail="El nombre debe contener letras")
    return name


@router_profile_edit.put('/usuario/me/profile')
def update_own_trainer_profile(payload: ProfileEditPayload, usuario=Depends(get_current_user)):
    name = validate_name(payload.nombre)
    team = payload.team_color.strip().lower()
    if team not in {'red', 'blue', 'green'}:
        raise HTTPException(status_code=422, detail="Equipo inválido")
    conn = get_connection()
    cursor = None
    try:
        cursor = get_cursor(conn)
        cursor.execute('''
            UPDATE usuarios SET nombre = %s, trainer_team_color = %s
            WHERE id = %s RETURNING id
        ''', (name, team, int(usuario['id'])))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        # Preserve trainer_starter_code, user's actual starter, inventory and avatar.
        updated = obtener_usuario_por_id(int(usuario['id']), cursor=cursor)
        conn.commit()
        return {'ok': True, 'usuario': updated}
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        release_connection(conn)

"""Mastersmon V1.4: alta inicial atomica, sin modificar rutas existentes."""
import re
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from database import get_connection, get_cursor, release_connection
from auth import get_current_user
from routes_pokemon import (
    calcular_stats,
    calcular_experiencia_total_base,
    sincronizar_movimientos_usuario_pokemon,
    guardar_equipo_usuario,
)

router_trainer_onboarding = APIRouter(prefix='/trainer-onboarding', tags=['Trainer onboarding V1.4'])

STARTERS = {
  1: (1, 4, 7), 2: (152, 155, 158), 3: (252, 255, 258),
  4: (387, 390, 393), 5: (495, 498, 501), 6: (650, 653, 656),
  7: (722, 725, 728), 8: (810, 813, 816), 9: (906, 909, 912),
}
ALLOWED_AVATARS = {'steven', 'red', 'leaf', 'brendan', 'may', 'dawn', 'lucas', 'rosa',
                   'batman', 'bryan', 'goku', 'hades', 'jean', 'jhonny', 'leon', 'nathaly'}
ALLOWED_TEAMS = {'red', 'blue', 'green'}

class SetupInput(BaseModel):
    avatar_id: str
    team_color: str
    generation: int
    pokemon_id: int

@router_trainer_onboarding.get('/starters')
def starter_catalog():
    """Muestra SOLO los iniciales existentes realmente en la base de datos."""
    con = get_connection()
    cur = get_cursor(con)
    try:
        ids = [i for group in STARTERS.values() for i in group]
        cur.execute('SELECT id,nombre,generacion FROM pokemon WHERE id = ANY(%s)', (ids,))
        existing = {int(r['id']): dict(r) for r in cur.fetchall()}
        return {'ok': True, 'generations': {
            str(gen): [{'id': pid, 'nombre': existing[pid]['nombre'], 'available': int(existing[pid]['generacion']) == gen}
                       for pid in starters if pid in existing]
            for gen, starters in STARTERS.items()
        }}
    finally:
        cur.close()
        release_connection(con)

@router_trainer_onboarding.post('/complete')
def complete_trainer_setup(data: SetupInput, user=Depends(get_current_user)):
    avatar = data.avatar_id.strip().lower()
    team = data.team_color.strip().lower()
    if avatar not in ALLOWED_AVATARS or team not in ALLOWED_TEAMS:
        raise HTTPException(status_code=400, detail='Avatar o equipo no permitido')
    if data.generation not in STARTERS or data.pokemon_id not in STARTERS[data.generation]:
        raise HTTPException(status_code=400, detail='Pokémon inicial no permitido en esa generación')
    con = get_connection()
    cur = get_cursor(con)
    try:
        # Bloquea la fila de este jugador hasta confirmar o revertir la transaccion.
        cur.execute('''SELECT id,trainer_setup_completed,trainer_setup_completed_at,
                         trainer_starter_code FROM usuarios WHERE id=%s FOR UPDATE''',
                    (int(user['id']),))
        usuario = cur.fetchone()
        if usuario is None:
            raise HTTPException(status_code=404, detail='Jugador no encontrado')
        if usuario['trainer_setup_completed'] or usuario['trainer_setup_completed_at'] is not None or usuario['trainer_starter_code']:
            raise HTTPException(status_code=409, detail='El entrenador ya tiene una configuracion inicial registrada')
        # Protege especialmente cuentas antiguas: ningun jugador con Pokémon obtiene otro inicial.
        cur.execute('SELECT 1 FROM usuario_pokemon WHERE usuario_id=%s LIMIT 1', (int(user['id']),))
        if cur.fetchone():
            raise HTTPException(status_code=409, detail='La cuenta ya tiene Pokémon y no puede recibir un inicial adicional')
        cur.execute('''SELECT id,nombre,generacion,hp,ataque,defensa,velocidad,
                         ataque_especial,defensa_especial FROM pokemon WHERE id=%s''', (data.pokemon_id,))
        p = cur.fetchone()
        if not p or int(p['generacion']) != data.generation:
            raise HTTPException(status_code=400, detail='Este Pokémon no existe en el catálogo de la generación')
        nivel = 5
        stats = calcular_stats(int(p['hp']), int(p['ataque']), int(p['defensa']), int(p['velocidad']), nivel,
                               base_ataque_especial=int(p['ataque_especial']), base_defensa_especial=int(p['defensa_especial']))
        cur.execute('''INSERT INTO usuario_pokemon
            (usuario_id,pokemon_id,nivel,experiencia,experiencia_total,es_shiny,
             hp_actual,hp_max,ataque,defensa,velocidad,ataque_especial,defensa_especial,
             bonus_hp_renacer,bonus_ataque_renacer,bonus_defensa_renacer,bonus_velocidad_renacer,victorias_total)
            VALUES (%s,%s,%s,0,%s,FALSE,%s,%s,%s,%s,%s,%s,%s,0,0,0,0,0) RETURNING id''',
            (int(user['id']), int(p['id']), nivel, calcular_experiencia_total_base(nivel, 0),
             stats['hp_max'], stats['hp_max'], stats['ataque'], stats['defensa'],stats['velocidad'],
             stats['ataque_especial'],stats['defensa_especial']))
        obtained = cur.fetchone()
        owned_id = int(obtained['id'])
        sincronizar_movimientos_usuario_pokemon(cur, owned_id, int(p['id']), nivel, origen='captura', auto_equipar=True)

        # JL - Inicio - El inicial se asigna a la posicion 1 del equipo en la misma transaccion.
        # Es una cuenta nueva y se acaba de validar que no tiene ningun Pokemon previo.
        # Reutiliza la funcion oficial del juego que guarda usuario_pokemon.id (no la especie).
        equipo_guardado = guardar_equipo_usuario(cur, int(user['id']), [owned_id])
        # JL - Fin - Equipo inicial por defecto.

        code = re.sub(r'[^a-z0-9]+', '-', str(p['nombre']).strip().lower()).strip('-')
        cur.execute('''UPDATE usuarios SET avatar_id=%s,trainer_team_color=%s,
                         trainer_starter_code=%s,trainer_setup_completed=TRUE,
                         trainer_setup_completed_at=NOW() WHERE id=%s''',
                    (avatar, team, code, int(user['id'])))
        con.commit()
        return {'ok': True, 'usuario_pokemon_id': owned_id, 'pokemon': {'id': int(p['id']), 'nombre': p['nombre']},
                'avatar_id': avatar, 'team_color': team, 'setup_completed': True,
                'equipo_ids': equipo_guardado, 'posicion_equipo': 1}
    except HTTPException:
        con.rollback()
        raise
    except Exception:
        con.rollback()
        raise HTTPException(status_code=500, detail='No se pudo completar la configuracion del entrenador')
    finally:
        cur.close()
        release_connection(con)

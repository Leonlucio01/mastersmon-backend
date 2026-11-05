"""Authoritative Boss reward claim. Boss damage/turns come only from locked combat state.

Registered before router_boss_idle so the legacy /battle/boss/recompensa
handler cannot accept browser-supplied damage totals.
"""
import json

from fastapi import APIRouter, Depends, HTTPException, status
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from monetization_utils import aplicar_multiplicadores_recompensa_batalla
from routes_pokemon import (
    normalizar_equipo_ids_json, validar_ids_pokemon_equipo,
    registrar_actividad_usuario,
)
from routes_boss_idle import (
    BossRecompensaPayload, asegurar_tablas_boss_idle,
    obtener_participacion_boss_por_token, obtener_boss_evento_detalle,
    calcular_recompensa_boss, sumar_pokedolares_usuario, aplicar_exp_a_equipo,
)

router_boss_rewards_secure = APIRouter()


@router_boss_rewards_secure.post('/battle/boss/recompensa')
def reclamar_boss_validado(payload: BossRecompensaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        user_id = int(usuario['id'])
        participacion = obtener_participacion_boss_por_token(
            cursor, payload.boss_session_token, user_id, for_update=True
        )
        if not participacion:
            raise HTTPException(status_code=409, detail='Boss session missing or expired')
        if participacion.get('reclamado_en') is not None:
            raise HTTPException(status_code=409, detail='Boss reward already claimed')

        cursor.execute(
            'SELECT combat_json FROM public.boss_combate_estado '
            'WHERE participacion_id=%s FOR UPDATE', (int(participacion['id']),)
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=409, detail='Boss combat has not started on server')
        combat = row['combat_json']
        if isinstance(combat, str):
            try:
                combat = json.loads(combat)
            except ValueError:
                raise HTTPException(status_code=409, detail='Invalid Boss combat state')
        if not isinstance(combat, dict) or combat.get('status') not in ('victory', 'defeat'):
            raise HTTPException(status_code=409, detail='Boss combat is not finished')

        rivals = combat.get('rivals') or []
        if len(rivals) != 1 or not isinstance(rivals[0], dict):
            raise HTTPException(status_code=409, detail='Invalid Boss opponent state')
        remaining_hp = int(rivals[0].get('hp', -1))
        max_hp = int(rivals[0].get('hp_max', 0))
        if max_hp <= 0 or not 0 <= remaining_hp <= max_hp:
            raise HTTPException(status_code=409, detail='Invalid Boss HP state')
        defeated = combat['status'] == 'victory'
        if defeated != (remaining_hp == 0):
            raise HTTPException(status_code=409, detail='Inconsistent Boss result')
        confirmed_damage = max_hp - remaining_hp
        if int(combat.get('damage_total', -1)) != confirmed_damage:
            raise HTTPException(status_code=409, detail='Inconsistent Boss damage state')
        confirmed_turns = max(0, int(combat.get('turn', 1)) - 1)

        detalle = obtener_boss_evento_detalle(cursor, int(participacion['boss_evento_id']))
        if not detalle:
            raise HTTPException(status_code=409, detail='Boss event no longer exists')
        recompensa = calcular_recompensa_boss(detalle, confirmed_damage, defeated)
        # The event catalog controls rewards. Client damage/defeated/turns are ignored.
        multipliers = aplicar_multiplicadores_recompensa_batalla(
            cursor, user_id, int(recompensa['exp']), int(recompensa['pokedolares'])
        )
        recompensa['exp_base'] = int(recompensa['exp'])
        recompensa['pokedolares_base'] = int(recompensa['pokedolares'])
        recompensa['exp'] = int(multipliers['exp_final'])
        recompensa['pokedolares'] = int(multipliers['pokedolares_final'])
        recompensa['beneficios_activos'] = multipliers['beneficios_activos']

        team_ids = validar_ids_pokemon_equipo(
            cursor, user_id,
            normalizar_equipo_ids_json(participacion.get('equipo_usuario_pokemon_ids'))
        )
        balance = sumar_pokedolares_usuario(cursor, user_id, int(recompensa['pokedolares']))
        updated = aplicar_exp_a_equipo(
            cursor, user_id, team_ids, int(recompensa['exp']),
            sumar_victoria=bool(recompensa['boss_derrotado']),
        )
        cursor.execute(
            '''UPDATE public.boss_participaciones
               SET damage_total=%s,boss_hp_total=%s,boss_derrotado=%s,
                   turnos=%s,exp_ganada=%s,pokedolares_ganados=%s,
                   reward_json=%s::jsonb,activo=FALSE,resultado='reclamada',
                   finalizado_en=COALESCE(finalizado_en,NOW()),reclamado_en=NOW()
               WHERE id=%s''',
            (int(recompensa['damage_total']), int(recompensa['boss_hp_total']),
             bool(recompensa['boss_derrotado']), confirmed_turns,
             int(recompensa['exp']), int(recompensa['pokedolares']),
             json.dumps(recompensa), int(participacion['id']))
        )
        registrar_actividad_usuario(
            cursor, user_id, pagina='battle-arena', accion='battle_end',
            detalle=f"modo:boss tier:{recompensa['tier']}"
        )
        conn.commit()
        return {
            'ok': True, 'modo': 'boss',
            'boss_session_token': payload.boss_session_token,
            'reward_tier': recompensa['tier'],
            'damage_total': int(recompensa['damage_total']),
            'boss_hp_total': int(recompensa['boss_hp_total']),
            'boss_derrotado': bool(recompensa['boss_derrotado']),
            'turnos': confirmed_turns,
            'exp_ganada': int(recompensa['exp']),
            'pokedolares_ganados': int(recompensa['pokedolares']),
            'pokedolares_actuales': int(balance),
            'pokemon_actualizados': updated,
            'exp_base': int(recompensa['exp_base']),
            'pokedolares_base': int(recompensa['pokedolares_base']),
            'beneficios_activos': recompensa.get('beneficios_activos') or [],
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)

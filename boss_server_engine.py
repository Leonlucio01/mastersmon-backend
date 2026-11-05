"""MastersMon Boss combat engine (backend-only staging implementation).

Integrate in app.py before routes_boss_idle for new /battle/boss/turno endpoint.
The existing /battle/boss/recompensa MUST be migrated to server-validated state
before this is enabled in production. See README.
"""
import json
import random
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from routes_battle_ai import _damage, _type_matchup, _choose_ai_move, _active_index, _int
from routes_boss_idle import obtener_participacion_boss_por_token, obtener_boss_evento_detalle, construir_boss_desde_evento
from gym_server_engine import _equipped_moves, _rival_moves

router_boss_combat = APIRouter()

class BossTurnRequest(BaseModel):
    boss_session_token: str = Field(min_length=12, max_length=128)
    action: str
    move_slot: int | None = None
    pokemon_id: int | None = None


def _json_obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _team(snapshot, cursor):
    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)
    if not isinstance(snapshot, list) or len(snapshot) != 6:
        raise HTTPException(409, 'Boss team snapshot is incomplete')
    own = []
    for entry in snapshot:
        p = dict(entry)
        p['instance_id'] = int(p['id'])
        p['hp_max'] = max(1, _int(p.get('hp_max')))
        p['hp'] = p['hp_max']  # isolate event HP from the persistent collection
        p['moves'] = _equipped_moves(cursor, p['instance_id'])
        own.append(p)
    return own


def _new_state(cursor, participation):
    event = obtener_boss_evento_detalle(cursor, int(participation['boss_evento_id']))
    boss = construir_boss_desde_evento(event)
    own = _team(participation.get('snapshot_equipo_json'), cursor)
    foe = dict(boss)
    foe['hp_max'] = max(1, _int(boss['hp_total']))
    foe['hp'] = foe['hp_max']
    foe['moves'] = _rival_moves(cursor, int(foe['pokemon_id']), int(foe['nivel']))
    return {
        'status': 'active', 'turn': 1, 'players': own, 'rivals': [foe],
        'player_active': _active_index(own, 0), 'rival_active': 0,
        'damage_total': 0, 'events': []
    }


def _load(cursor, token, user_id):
    participation = obtener_participacion_boss_por_token(cursor, token, user_id, for_update=True)
    if not participation:
        raise HTTPException(409, 'Boss session missing, closed or expired')
    cursor.execute('SELECT combat_json FROM boss_combate_estado WHERE participacion_id=%s FOR UPDATE',
                   (int(participation['id']),))
    row = cursor.fetchone()
    if row:
        state = _json_obj(row['combat_json'])
    else:
        state = _new_state(cursor, participation)
        cursor.execute('INSERT INTO boss_combate_estado (participacion_id,combat_json) VALUES (%s,%s::jsonb)',
                       (int(participation['id']),json.dumps(state)))
    if state.get('status') not in ('active','victory','defeat'):
        raise HTTPException(409, 'Corrupt Boss combat state')
    return participation, state


def _save(cursor, participation, state):
    cursor.execute('UPDATE boss_combate_estado SET combat_json=%s::jsonb,actualizado_en=NOW() WHERE participacion_id=%s',
                   (json.dumps(state),int(participation['id'])))


def _public(state):
    return {k: state[k] for k in ('status','turn','players','rivals','player_active','rival_active','damage_total','events')}


def _estimate(move, attacker, defender):
    level=max(1,_int(attacker.get('nivel'),1))
    atk=max(1,_int(attacker.get('ataque')))
    defense=max(1,_int(defender.get('defensa')))
    base=((2*level/5+2)*min(250,_int(move.get('potencia')))*atk/defense)/50+2
    return base * _type_matchup(move['tipo'], defender)


@router_boss_combat.get('/battle/boss/estado-combate')
def boss_combat_state(boss_session_token: str, user=Depends(get_current_user)):
    conn=get_connection();cursor=get_cursor(conn)
    try:
        participation,state=_load(cursor,boss_session_token,int(user['id']))
        conn.commit()
        return {'ok': True, **_public(state)}
    except Exception:
        conn.rollback();raise
    finally:
        cursor.close();release_connection(conn)


@router_boss_combat.post('/battle/boss/turno')
def boss_combat_turn(data: BossTurnRequest, user=Depends(get_current_user)):
    if data.action not in ('attack','auto','switch'):
        raise HTTPException(400,'Invalid Boss action')
    conn=get_connection();cursor=get_cursor(conn)
    try:
        participation,state=_load(cursor,data.boss_session_token,int(user['id']))
        if state['status']!='active':
            raise HTTPException(409,'Boss battle already ended')
        pi=state['player_active']
        if pi is None:
            raise HTTPException(409,'No active Pokemon')
        own=state['players'];player=own[pi];enemy=state['rivals'][0]
        turn=int(state['turn']);events=[];rng=random.SystemRandom()
        if data.action=='switch':
            target=next((i for i,p in enumerate(own) if int(p['instance_id'])==data.pokemon_id and p['hp']>0),None)
            if target is None or target==pi:
                raise HTTPException(400,'Choose another available Pokemon')
            state['player_active']=target
            player=own[target]
            events.append({'turn':turn,'event':'switch','pokemon_id':player['instance_id']})
        else:
            if data.action=='auto':
                options=[m for m in player['moves'] if _int(m.get('potencia'))>0]
                move=max(options,key=lambda m:_estimate(m,player,enemy)) if options else None
            else:
                move=next((m for m in player['moves'] if m['slot']==data.move_slot),None)
            if not move or _int(move.get('potencia'))<=0:
                raise HTTPException(400,'Invalid equipped move')
            effect=_type_matchup(move['tipo'],enemy)
            hit=min(enemy['hp'],_damage(player,enemy,min(250,_int(move['potencia'])),0,rng,effect))
            enemy['hp']-=hit
            state['damage_total']+=hit
            events.append({'turn':turn,'event':'attack','move_name':move['nombre'],
                           'move_type':move['tipo'],'effectiveness':effect,'damage':hit,
                           'target_hp':enemy['hp'],'auto':data.action=='auto'})
        if enemy['hp']<=0:
            state['status']='victory'
            events.append({'turn':turn,'event':'victory'})
        else:
            move=_choose_ai_move(enemy,player,4,rng)
            effect=_type_matchup(move['tipo'],player)
            hit=min(player['hp'],_damage(enemy,player,_int(move['potencia']),0,rng,effect))
            player['hp']-=hit
            events.append({'turn':turn,'event':'rival_attack','move_name':move['nombre'],
                           'move_type':move['tipo'],'effectiveness':effect,'damage':hit,'target_hp':player['hp']})
            if player['hp']<=0:
                next_pi=_active_index(own,state['player_active'])
                if next_pi is None:
                    state['status']='defeat'
                    events.append({'turn':turn,'event':'defeat'})
                else:
                    state['player_active']=next_pi
                    events.append({'turn':turn,'event':'player_switch','pokemon_id':own[next_pi]['instance_id']})
        state['turn']=turn+1
        state['events']=(state['events']+events)[-40:]
        _save(cursor,participation,state)
        conn.commit()
        return {'ok':True,'turn_events':events,**_public(state)}
    except Exception:
        conn.rollback();raise
    finally:
        cursor.close();release_connection(conn)

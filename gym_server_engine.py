"""Server-authoritative turn engine for MastersMon Gym battles.

No client-supplied HP, damage, winner, team or rival is accepted.
The canonical session and its JSONB state are locked on every turn.
"""
import json
import random
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from routes_battle_ai import _damage, _type_matchup, _choose_ai_move, _active_index, _int

router_gym_combat = APIRouter()

class GymTurnRequest(BaseModel):
    gym_session_token: str = Field(min_length=12, max_length=128)
    action: str
    move_slot: int | None = None
    pokemon_id: int | None = None


def _as_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            data = json.loads(value)
            return data if isinstance(data, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _equipped_moves(cursor, pokemon_id):
    cursor.execute("""
        SELECT upme.slot, m.id AS move_id, m.nombre, m.tipo, m.potencia
        FROM usuario_pokemon_movimientos_equipo upme
        JOIN usuario_pokemon_movimientos upm
          ON upm.id=upme.usuario_pokemon_movimiento_id
         AND upm.usuario_pokemon_id=upme.usuario_pokemon_id AND upm.activo=TRUE
        JOIN movimientos m ON m.id=upm.movimiento_id AND m.activo=TRUE
        WHERE upme.usuario_pokemon_id=%s ORDER BY upme.slot
    """, (int(pokemon_id),))
    return [{'slot': _int(x['slot']), 'id': _int(x['move_id']),
             'nombre': x['nombre'], 'tipo': x['tipo'],
             'potencia': _int(x['potencia'])} for x in cursor.fetchall()]


def _rival_moves(cursor, pokemon_id, level):
    cursor.execute("""
        SELECT m.id,m.nombre,m.tipo,m.potencia
        FROM pokemon_movimientos pm JOIN movimientos m ON m.id=pm.movimiento_id
        WHERE pm.pokemon_id=%s AND pm.activo=TRUE AND m.activo=TRUE
          AND pm.nivel_requerido<=%s AND COALESCE(m.potencia,0)>0
        ORDER BY m.potencia DESC, pm.nivel_requerido DESC, m.id LIMIT 12
    """, (int(pokemon_id), int(level)))
    pool = [{'id':_int(m['id']), 'nombre':m['nombre'], 'tipo':m['tipo'],
             'potencia':_int(m['potencia'])} for m in cursor.fetchall()]
    picked = []
    used = set()
    for move in pool:
        kind = str(move['tipo']).lower()
        if kind not in used:
            picked.append(move)
            used.add(kind)
        if len(picked) == 4:
            break
    for move in pool:
        if len(picked) >= 4:
            break
        if move not in picked:
            picked.append(move)
    return picked or [{'id':None,'nombre':'Struggle','tipo':'normal','potencia':40}]


def _construct(cursor, session):
    players = session.get('snapshot_equipo_usuario_json') or []
    rivals = session.get('snapshot_equipo_rival_json') or []
    if isinstance(players, str): players = json.loads(players)
    if isinstance(rivals, str): rivals = json.loads(rivals)
    if not isinstance(players, list) or not isinstance(rivals, list) or len(players) != 6 or len(rivals) != 6:
        raise HTTPException(409, 'Gym team snapshots are incomplete')
    own = []
    for p in players:
        q = dict(p)
        q['instance_id'] = int(p['usuario_pokemon_id'])
        q['hp_max'] = max(1, _int(p['hp_max']))
        q['hp'] = max(0, min(q['hp_max'], _int(p.get('hp', p['hp_max']))))
        q['moves'] = _equipped_moves(cursor, q['instance_id'])
        own.append(q)
    foe = []
    for p in rivals:
        q = dict(p)
        q['hp_max'] = max(1, _int(p['hp_max']))
        q['hp'] = max(0, min(q['hp_max'], _int(p.get('hp', p['hp_max']))))
        q['moves'] = _rival_moves(cursor, q['pokemon_id'], q['nivel'])
        foe.append(q)
    return {'status':'active', 'turn':1, 'players':own, 'rivals':foe,
            'player_active':_active_index(own, 0), 'rival_active':_active_index(foe, 0),
            'events':[]}


def _load_locked(cursor, token, user_id):
    from routes_gyms import obtener_sesion_gym_por_token
    session = obtener_sesion_gym_por_token(cursor, token, user_id, for_update=True)
    if not session:
        raise HTTPException(404, 'Gym session was not found')
    if not session.get('activo') or session.get('cancelado_en') or session.get('reclamado_en'):
        raise HTTPException(409, 'Gym session has already closed')
    expiration = session.get('expira_en')
    if expiration and expiration < datetime.now():
        raise HTTPException(410, 'Gym session has expired')
    result = _as_dict(session.get('resultado_json'))
    state = result.get('combat')
    if not isinstance(state, dict):
        state = _construct(cursor, session)
        result['combat'] = state
    return session, result, state


def _save(cursor, session, result, state):
    result['combat'] = state
    cursor.execute('UPDATE gym_sesiones SET resultado_json=%s::jsonb WHERE id=%s',
                   (json.dumps(result), int(session['id'])))


def _safe(state):
    return {'status':state['status'],'turn':state['turn'],
            'players':state['players'],'rivals':state['rivals'],
            'player_active':state['player_active'],'rival_active':state['rival_active'],
            'events':state.get('events',[])[-15:]}


@router_gym_combat.get('/battle/gyms/estado')
def gym_battle_state(gym_session_token: str, user=Depends(get_current_user)):
    conn=get_connection(); cursor=get_cursor(conn)
    try:
        session, result, state = _load_locked(cursor, gym_session_token, int(user['id']))
        _save(cursor, session, result, state)
        conn.commit()
        return {'ok': True, **_safe(state)}
    except Exception:
        conn.rollback(); raise
    finally:
        cursor.close(); release_connection(conn)


@router_gym_combat.post('/battle/gyms/turno')
def gym_battle_turn(data: GymTurnRequest, user=Depends(get_current_user)):
    if data.action not in ('attack','switch','auto'):
        raise HTTPException(400, 'Only attack, auto or switch is supported')
    conn=get_connection(); cursor=get_cursor(conn)
    try:
        session, result, state = _load_locked(cursor, data.gym_session_token, int(user['id']))
        if state['status'] != 'active':
            raise HTTPException(409, 'Gym battle has already ended')
        own=state['players']; rivals=state['rivals']
        pi=state['player_active']; ri=state['rival_active']
        if pi is None or ri is None:
            raise HTTPException(409, 'Invalid active Pokemon')
        player=own[pi]; enemy=rivals[ri]
        turn=int(state['turn']); events=[]; rng=random.SystemRandom()
        if data.action=='switch':
            target=next((i for i,p in enumerate(own)
                         if int(p['instance_id'])==data.pokemon_id and p['hp']>0), None)
            if target is None or target==pi:
                raise HTTPException(400, 'Choose another available Pokemon')
            state['player_active']=target
            player=own[target]
            events.append({'turn':turn,'event':'switch','pokemon_id':player['instance_id']})
        else:
            if data.action=='auto':
                # Server selects the highest expected damage; clients cannot inject a move.
                options=[m for m in player['moves'] if _int(m.get('potencia'))>0]
                def estimate(m):
                    level=max(1,_int(player['nivel'],1))
                    attack=max(1,_int(player.get('ataque')))
                    defense=max(1,_int(enemy.get('defensa')))
                    base=(((2*level/5+2)*min(250,_int(m['potencia']))*attack/defense)/50)+2
                    return base*_type_matchup(m['tipo'],enemy)
                move=max(options,key=estimate) if options else None
            else:
                move=next((m for m in player['moves'] if m['slot']==data.move_slot), None)
            if not move or _int(move.get('potencia'))<=0:
                raise HTTPException(400, 'Invalid or non-offensive equipped move')
            effect=_type_matchup(move['tipo'],enemy)
            hit=min(enemy['hp'],_damage(player,enemy,min(250,_int(move['potencia'])),0,rng,effect))
            enemy['hp']-=hit
            events.append({'turn':turn,'event':'attack','move_name':move['nombre'],
                           'move_type':move['tipo'],'effectiveness':effect,
                           'damage':hit,'target_hp':enemy['hp'],'auto':data.action=='auto'})
        if enemy['hp']<=0:
            next_foe=_active_index(rivals,ri)
            if next_foe is None:
                state['status']='victory'
                events.append({'turn':turn,'event':'victory'})
            else:
                state['rival_active']=next_foe
                events.append({'turn':turn,'event':'rival_switch','pokemon_id':rivals[next_foe]['pokemon_id']})
        else:
            move=_choose_ai_move(enemy,player,4,rng)
            effect=_type_matchup(move['tipo'],player)
            hit=min(player['hp'],_damage(enemy,player,_int(move['potencia']),0,rng,effect))
            player['hp']-=hit
            events.append({'turn':turn,'event':'rival_attack','move_name':move['nombre'],
                           'move_type':move['tipo'],'effectiveness':effect,
                           'damage':hit,'target_hp':player['hp']})
            if player['hp']<=0:
                next_own=_active_index(own,state['player_active'])
                if next_own is None:
                    state['status']='defeat'
                    events.append({'turn':turn,'event':'defeat'})
                else:
                    state['player_active']=next_own
                    events.append({'turn':turn,'event':'player_switch',
                                   'pokemon_id':own[next_own]['instance_id']})
        state['events']=(state.get('events') or [])[-40:]+events
        state['turn']=turn+1
        _save(cursor,session,result,state)
        conn.commit()
        return {'ok':True,'turn_events':events,**_safe(state)}
    except Exception:
        conn.rollback();raise
    finally:
        cursor.close();release_connection(conn)

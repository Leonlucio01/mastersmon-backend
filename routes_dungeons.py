"""MastersMon Dungeons Alpha V1: server-authoritative, non-economic prototype.

No premium, no granted Pokemon, no currency/items mutations.
Requires sql_dungeons_alpha_v1.sql to be applied once before use.
"""
import hashlib
import json
import random
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from auth import get_current_user
from database import get_connection, get_cursor, release_connection

router_dungeons = APIRouter(prefix='/dungeons', tags=['Dungeons Alpha'])

# World maps are authoritative. '#' wall; '.' floor; 'S' start; 'E' enemy; 'B' boss; 'C' chest; '>' stairs.
FLOORS = [
    ["#############", "#S....#.....#", "#.###.#.###.#", "#...#...#...#", "###.#####.#.#", "#...#.....#.#", "#.#######.#.#", "#.....E...>.#", "#############"],
    ["#############", "#S..#.......#", "#.#.#.#####.#", "#.#...#...#.#", "#.#####.#.#.#", "#.......#...#", "#.#######.###", "#....E....>.#", "#############"],
    ["#############", "#S..........#", "#.#########.#", "#.#.......#.#", "#.#.#####.#.#", "#...#...#...#", "###.#.B.#.###", "#.....C.....#", "#############"],
]
FLOOR_INFO = [{'enemy': 'Wild Pidgeotto', 'hp': 30}, {'enemy': 'Wild Gloom', 'hp': 42}, {'enemy': 'Alpha Venusaur', 'hp': 95}]
DUNGEON = {'code':'mystic_forest', 'name':'Mystic Forest', 'floors':3, 'boss':'Alpha Venusaur', 'free':True, 'status':'alpha'}

class StartPayload(BaseModel):
    dungeon_code: str = 'mystic_forest'
    pokemon_ids: list[int] = Field(min_length=1, max_length=3)

class MovePayload(BaseModel):
    direction: str
    expected_turn: int = Field(ge=0)

class AttackPayload(BaseModel):
    expected_turn: int = Field(ge=0)

class NextFloorPayload(BaseModel):
    expected_turn: int = Field(ge=0)

class ChestPayload(BaseModel):
    expected_turn: int = Field(ge=0)


def tiles(floor):
    grid = FLOORS[floor - 1]
    return {'width':len(grid[0]), 'height':len(grid), 'rows':grid}


def pos(grid, ch):
    for y,row in enumerate(grid):
        x=row.find(ch)
        if x >= 0: return [x,y]
    return None


def public_state(session):
    state=session['state']
    return {'id':session['id'], 'status':session['status'], 'dungeon':DUNGEON,
            'floor':session['floor'], 'map':tiles(session['floor']),
            'turn':session['turn'], 'player':state['player'],
            'team':state['team'], 'enemy':state['enemy'],
            'chest_opened':state['chest_opened'], 'reward':state.get('reward'),
            'message':state.get('message','')}


def read_locked(cur, session_id, uid):
    cur.execute('SELECT id, usuario_id, floor, turn, status, state FROM public.dungeon_runs WHERE id=%s AND usuario_id=%s FOR UPDATE', (session_id, uid))
    row=cur.fetchone()
    if not row: raise HTTPException(404, 'Dungeon run not found')
    row=dict(row)
    if isinstance(row['state'],str): row['state']=json.loads(row['state'])
    return row


def save(cur, run):
    cur.execute('UPDATE public.dungeon_runs SET floor=%s, turn=%s, status=%s, state=%s::jsonb, updated_at=now() WHERE id=%s AND usuario_id=%s',
                (run['floor'],run['turn'],run['status'],json.dumps(run['state']),run['id'],run['usuario_id']))


def transaction(action):
    conn=get_connection()
    cur=None
    try:
        cur=get_cursor(conn)
        result=action(cur)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur:cur.close()
        release_connection(conn)


def check_turn(run, expected):
    if run['status']!='active': raise HTTPException(409, 'Dungeon run is not active')
    if run['turn']!=expected: raise HTTPException(409, 'Stale action; refresh dungeon state')


def adjacent(a,b):
    return abs(a[0]-b[0])+abs(a[1]-b[1])<=1


def enemy_turn(run, damage=5):
    state=run['state']
    enemy=state['enemy']
    if enemy['hp']>0 and adjacent(state['player'], enemy['pos']):
        state['team'][0]['hp'] = max(0,state['team'][0]['hp']-damage)
        if state['team'][0]['hp']==0:
            run['status']='failed'
            state['message']='Your leader fainted. Expedition failed.'


@router_dungeons.get('/catalog')
def catalog():
    return {'dungeons':[DUNGEON], 'rewards_mode':'preview_only'}


@router_dungeons.post('/start')
def start(payload:StartPayload, user=Depends(get_current_user)):
    if payload.dungeon_code!=DUNGEON['code']: raise HTTPException(404,'Unknown dungeon')
    if len(set(payload.pokemon_ids))!=len(payload.pokemon_ids): raise HTTPException(400,'Duplicate Pokemon IDs')
    uid=int(user['id'])
    def op(cur):
        cur.execute('SELECT id FROM public.dungeon_runs WHERE usuario_id=%s AND status=%s FOR UPDATE', (uid,'active'))
        if cur.fetchone(): raise HTTPException(409,'You already have an active expedition')
        cur.execute('SELECT up.id,up.pokemon_id,up.nivel,p.nombre FROM public.usuario_pokemon up JOIN public.pokemon p ON p.id=up.pokemon_id WHERE up.usuario_id=%s AND up.id = ANY(%s)', (uid,payload.pokemon_ids))
        found={int(r['id']):dict(r) for r in cur.fetchall()}
        if len(found)!=len(payload.pokemon_ids): raise HTTPException(400,'Pokemon must belong to your account')
        team=[{'capture_id':i,'pokemon_id':int(found[i]['pokemon_id']),'name':found[i]['nombre'],'level':int(found[i]['nivel'] or 1),'hp':100,'max_hp':100} for i in payload.pokemon_ids]
        grid=FLOORS[0]
        state={'player':pos(grid,'S'),'team':team,'enemy':{'name':FLOOR_INFO[0]['enemy'],'hp':FLOOR_INFO[0]['hp'],'max_hp':FLOOR_INFO[0]['hp'],'pos':pos(grid,'E')},'chest_opened':False,'reward':None,'message':'Find the enemy, defeat it, and reach the stairs.'}
        cur.execute('INSERT INTO public.dungeon_runs (usuario_id,dungeon_code,floor,turn,status,state) VALUES (%s,%s,1,0,%s,%s::jsonb) RETURNING id', (uid,payload.dungeon_code,'active',json.dumps(state)))
        rid=cur.fetchone()['id']
        return {'id':rid,'status':'active','floor':1,'turn':0,'state':state}
    return transaction(lambda cur: public_state(op(cur)))


@router_dungeons.get('/active')
def active(user=Depends(get_current_user)):
    uid=int(user['id'])
    def op(cur):
        cur.execute("SELECT id, usuario_id, floor, turn, status, state FROM public.dungeon_runs WHERE usuario_id=%s AND status='active'", (uid,))
        r=cur.fetchone()
        return {'run':public_state(dict(r)) if r else None}
    return transaction(op)


@router_dungeons.get('/runs/{run_id}')
def get_run(run_id:int,user=Depends(get_current_user)):
    uid=int(user['id'])
    return transaction(lambda cur: public_state(read_locked(cur,run_id,uid)))


@router_dungeons.post('/runs/{run_id}/move')
def move(run_id:int,payload:MovePayload,user=Depends(get_current_user)):
    uid=int(user['id'])
    vectors={'up':(0,-1),'down':(0,1),'left':(-1,0),'right':(1,0)}
    if payload.direction not in vectors: raise HTTPException(400,'Invalid direction')
    def op(cur):
        run=read_locked(cur,run_id,uid)
        check_turn(run,payload.expected_turn)
        dx,dy=vectors[payload.direction]
        x,y=run['state']['player']
        nx,ny=x+dx,y+dy
        grid=FLOORS[run['floor']-1]
        if ny<0 or ny>=len(grid) or nx<0 or nx>=len(grid[0]) or grid[ny][nx]=='#': raise HTTPException(400,'Blocked tile')
        enemy=run['state']['enemy']
        if [nx,ny]==enemy['pos'] and enemy['hp']>0: raise HTTPException(400,'Enemy blocks this tile; attack first')
        run['state']['player']=[nx,ny]
        run['turn']+=1
        run['state']['message']='Explore the floor.'
        enemy_turn(run)
        save(cur,run)
        return public_state(run)
    return transaction(op)


@router_dungeons.post('/runs/{run_id}/attack')
def attack(run_id:int,payload:AttackPayload,user=Depends(get_current_user)):
    uid=int(user['id'])
    def op(cur):
        run=read_locked(cur,run_id,uid)
        check_turn(run,payload.expected_turn)
        enemy=run['state']['enemy']
        if enemy['hp']<=0: raise HTTPException(409,'Enemy already defeated')
        if not adjacent(run['state']['player'],enemy['pos']): raise HTTPException(400,'Enemy out of range')
        leader=run['state']['team'][0]
        power=min(36,12+leader['level']//6)
        enemy['hp']=max(0,enemy['hp']-power)
        run['turn']+=1
        run['state']['message']=f"{leader['name']} dealt {power} damage!"
        enemy_turn(run, 8 if run['floor']==3 else 5)
        if enemy['hp']==0:
            run['state']['message']=f"{enemy['name']} defeated! " + ('Open the treasure chest.' if run['floor']==3 else 'Find the stairs.')
        save(cur,run)
        return public_state(run)
    return transaction(op)


@router_dungeons.post('/runs/{run_id}/next-floor')
def next_floor(run_id:int,payload:NextFloorPayload,user=Depends(get_current_user)):
    uid=int(user['id'])
    def op(cur):
        run=read_locked(cur,run_id,uid)
        check_turn(run,payload.expected_turn)
        if run['floor']>=3: raise HTTPException(409,'This is the final floor')
        grid=FLOORS[run['floor']-1]
        if run['state']['enemy']['hp']>0 or run['state']['player']!=pos(grid,'>'): raise HTTPException(400,'Defeat the enemy and reach the stairs')
        run['floor']+=1
        run['turn']+=1
        grid=FLOORS[run['floor']-1]
        config=FLOOR_INFO[run['floor']-1]
        run['state']['player']=pos(grid,'S')
        run['state']['enemy']={'name':config['enemy'],'hp':config['hp'],'max_hp':config['hp'],'pos':pos(grid,'B' if run['floor']==3 else 'E')}
        run['state']['message']='New floor unlocked.'
        save(cur,run)
        return public_state(run)
    return transaction(op)


@router_dungeons.post('/runs/{run_id}/chest')
def chest(run_id:int,payload:ChestPayload,user=Depends(get_current_user)):
    uid=int(user['id'])
    def op(cur):
        run=read_locked(cur,run_id,uid)
        check_turn(run,payload.expected_turn)
        grid=FLOORS[2]
        state=run['state']
        if run['floor']!=3 or state['enemy']['hp']>0 or not adjacent(state['player'],pos(grid,'C')): raise HTTPException(400,'Treasure is not accessible')
        if state['chest_opened']: raise HTTPException(409,'Chest already opened')
        # Cosmetic, preview-only reward. It does not grant items, currency or Pokemon.
        labels=['Forest Relic (preview)','Alpha Crest (preview)','Mystic Gem (preview)']
        reward=labels[random.SystemRandom().randrange(len(labels))]
        state['reward']={'name':reward,'granted':False}
        state['chest_opened']=True
        state['message']='Dungeon cleared! Reward preview only; nothing added to inventory.'
        run['status']='completed'
        run['turn']+=1
        save(cur,run)
        return public_state(run)
    return transaction(op)


@router_dungeons.post('/runs/{run_id}/abandon')
def abandon(run_id:int,user=Depends(get_current_user)):
    uid=int(user['id'])
    def op(cur):
        run=read_locked(cur,run_id,uid)
        if run['status']!='active': raise HTTPException(409,'Run already finished')
        run['status']='abandoned'
        run['state']['message']='Expedition abandoned.'
        save(cur,run)
        return public_state(run)
    return transaction(op)

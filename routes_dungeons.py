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
# Per-floor packs. All positions are validated against the floor layout below.
ENEMY_PACKS = [
    [('Wild Pidgeotto',17,30,(6,7)),('Wild Caterpie',10,24,(4,1)),('Wild Oddish',43,28,(9,3))],
    [('Wild Gloom',44,42,(5,7)),('Wild Zubat',41,26,(3,3)),('Wild Paras',46,30,(9,3))],
    [('Alpha Venusaur',3,95,(6,6)),('Wild Bellsprout',69,32,(3,5)),('Wild Tangela',114,40,(9,5))],
]

def new_pack(floor):
    return [{'id':f'{floor}-{idx}', 'name':name, 'pokemon_id':species, 'hp':hp,
             'max_hp':hp, 'pos':list(pos), 'boss':floor==3 and idx==1}
            for idx,(name,species,hp,pos) in enumerate(ENEMY_PACKS[floor-1],1)]

def normalize_enemies(state, floor):
    if 'enemies' not in state:
        legacy=state.get('enemy') or {}
        species={1:17,2:44,3:3}[floor]
        state['enemies']=[{**legacy, 'id':f'{floor}-1', 'pokemon_id':species, 'boss':floor==3}]
    state['enemy']=next((e for e in state['enemies'] if e['hp']>0),state['enemies'][0])
    return state

def alive(state):
    return [e for e in state.get('enemies',[]) if e['hp']>0]

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



# V5.1: server-verified prototype skills. These are generic dungeon skills, NOT
# claims that a species knows a move from the canonical learnset.
SKILLS = {
    'fire': {'name':'Flame Burst','range':3,'power':15,'cooldown':3},
    'water': {'name':'Water Pulse','range':3,'power':14,'cooldown':3},
    'electric': {'name':'Spark','range':2,'power':17,'cooldown':3},
    'slash': {'name':'Quick Slash','range':1,'power':20,'cooldown':2},
}

class SkillPayload(BaseModel):
    expected_turn: int = Field(ge=0)
    skill: str
    enemy_id: str | None = None

@router_dungeons.post('/runs/{run_id}/skill')
def use_skill(run_id:int,payload:SkillPayload,user=Depends(get_current_user)):
    skill=SKILLS.get(payload.skill)
    if not skill: raise HTTPException(400,'Unknown skill')
    uid=int(user['id'])
    def op(cur):
        run=read_locked(cur,run_id,uid)
        check_turn(run,payload.expected_turn)
        state=run['state']
        if not state.get('team') or state['team'][0]['hp']<=0: raise HTTPException(409,'Leader cannot attack')
        cooldowns=state.setdefault('skill_last_turn',{})
        last=cooldowns.get(payload.skill)
        if last is not None and run['turn']-int(last)<skill['cooldown']:
            raise HTTPException(409,'Skill is recharging')
        px,py=state['player']
        enemies=[e for e in alive(state) if (payload.enemy_id is None or str(e['id'])==payload.enemy_id)]
        enemies=[e for e in enemies if abs(e['pos'][0]-px)+abs(e['pos'][1]-py)<=skill['range']]
        if not enemies: raise HTTPException(400,'No enemy in skill range')
        enemy=min(enemies,key=lambda e:(abs(e['pos'][0]-px)+abs(e['pos'][1]-py),str(e['id'])))
        old_hp=enemy['hp']
        level=max(1,int(state['team'][0]['level']))
        damage=min(38,skill['power']+level//10)
        enemy['hp']=max(0,old_hp-damage)
        actual=old_hp-enemy['hp']
        cooldowns[payload.skill]=run['turn']
        run['turn']+=1
        state['message']=f"{state['team'][0]['name']} used {skill['name']}! {actual} damage to {enemy['name']}."
        state['last_combat_event']={'type':'skill','skill':payload.skill,'enemy_id':enemy['id'],'damage':actual,
                                    'enemy_pos':list(enemy['pos']),'player_pos':list(state['player'])}
        if alive(state): enemy_turn(run,8 if run['floor']==3 else 5)
        else: state['message']+=' Floor cleared! Find the stairs or chest.'
        normalize_enemies(state,run['floor'])
        save(cur,run)
        return public_state(run)
    return transaction(op)


def tiles(floor):
    grid = FLOORS[floor - 1]
    return {'width':len(grid[0]), 'height':len(grid), 'rows':grid}


def pos(grid, ch):
    for y,row in enumerate(grid):
        x=row.find(ch)
        if x >= 0: return [x,y]
    return None


def public_state(session):
    state=normalize_enemies(session['state'],session['floor'])
    return {'id':session['id'], 'status':session['status'], 'dungeon':DUNGEON,
            'floor':session['floor'], 'map':tiles(session['floor']),
            'turn':session['turn'], 'player':state['player'],
            'team':state['team'], 'enemy':state['enemy'], 'enemies':state['enemies'],
            'chest_opened':state['chest_opened'], 'reward':state.get('reward'), 'last_combat_event':state.get('last_combat_event'), 'skill_last_turn':state.get('skill_last_turn',{}),
            'message':state.get('message','')}


def read_locked(cur, session_id, uid):
    cur.execute('SELECT id, usuario_id, floor, turn, status, state FROM public.dungeon_runs WHERE id=%s AND usuario_id=%s FOR UPDATE', (session_id, uid))
    row=cur.fetchone()
    if not row: raise HTTPException(404, 'Dungeon run not found')
    row=dict(row)
    if isinstance(row['state'],str): row['state']=json.loads(row['state'])
    normalize_enemies(row['state'],row['floor'])
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
    enemies=alive(state)
    player=state['player']
    occupied={tuple(e['pos']) for e in enemies}
    grid=FLOORS[run['floor']-1]
    # Advance a single nearby enemy on alternate turns. The server chooses movements.
    if run['turn'] % 2 == 0:
        movers=sorted((e for e in enemies if not adjacent(player,e['pos'])),
                      key=lambda e:abs(e['pos'][0]-player[0])+abs(e['pos'][1]-player[1]))
        for e in movers:
            x,y=e['pos']
            options=[(x+1,y),(x-1,y),(x,y+1),(x,y-1)]
            options.sort(key=lambda p:abs(p[0]-player[0])+abs(p[1]-player[1]))
            for nx,ny in options:
                if (nx,ny) in occupied or [nx,ny]==player or not (0<=ny<len(grid) and 0<=nx<len(grid[ny])) or grid[ny][nx]=='#':continue
                occupied.remove((x,y));occupied.add((nx,ny));e['pos']=[nx,ny]
                break
            break
    attackers=[e for e in alive(state) if adjacent(player,e['pos'])]
    if attackers:
        # Limit total damage; avoid making multi-enemy floors instantly lethal.
        total=min(12,damage+max(0,len(attackers)-1)*2)
        state['team'][0]['hp']=max(0,state['team'][0]['hp']-total)
        if state['team'][0]['hp']==0:
            run['status']='failed'
            state['message']='Your leader fainted. Expedition failed.'
    normalize_enemies(state,run['floor'])


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
        state={'player':pos(grid,'S'),'team':team,'enemies':new_pack(1),'chest_opened':False,'reward':None,'message':'Find the enemy, defeat it, and reach the stairs.'}
        normalize_enemies(state,1)
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
        if any([nx,ny]==e['pos'] for e in alive(run['state'])): raise HTTPException(400,'Enemy blocks this tile; attack first')
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
        targets=[e for e in alive(run['state']) if adjacent(run['state']['player'],e['pos'])]
        if not targets: raise HTTPException(400,'Enemy out of range')
        enemy=min(targets,key=lambda e:(e['hp'],e['id']))
        leader=run['state']['team'][0]
        power=min(36,12+leader['level']//6)
        enemy['hp']=max(0,enemy['hp']-power)
        run['turn']+=1
        run['state']['message']=f"{leader['name']} dealt {power} damage to {enemy['name']}!"
        if not alive(run['state']):
            run['state']['message']='All enemies defeated! ' + ('Open the treasure chest.' if run['floor']==3 else 'Find the stairs.')
        else:
            enemy_turn(run,8 if run['floor']==3 else 5)
        normalize_enemies(run['state'],run['floor'])
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
        if alive(run['state']) or run['state']['player']!=pos(grid,'>'): raise HTTPException(400,'Defeat the enemy and reach the stairs')
        run['floor']+=1
        run['turn']+=1
        grid=FLOORS[run['floor']-1]
        config=FLOOR_INFO[run['floor']-1]
        run['state']['player']=pos(grid,'S')
        run['state']['enemies']=new_pack(run['floor'])
        normalize_enemies(run['state'],run['floor'])
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
        if run['floor']!=3 or alive(state) or not adjacent(state['player'],pos(grid,'C')): raise HTTPException(400,'Treasure is not accessible')
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

"""Read-only authoritative AI roster preview. No turns or prizes are awarded here."""
from fastapi import APIRouter, Depends, HTTPException
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from routes_pokemon import calcular_stats, obtener_equipo_guardado_ids, obtener_select_stats_especiales

router_battle_ai = APIRouter()
# Existing preview rival lineups. Preview only: server turn resolution is the next stage.
AI_ROSTERS = {
    0: [3, 25, 9, 1, 6, 18],
    2: [443, 448, 468, 445, 131, 282],
    4: [94, 376, 149, 248, 65, 130],
    6: [150, 384, 248, 282, 131, 149],
}

@router_battle_ai.get("/battle/ai/preview")
def battle_ai_preview(difficulty: int = 0, user=Depends(get_current_user)):
    if difficulty not in AI_ROSTERS:
        raise HTTPException(status_code=400, detail="Invalid difficulty")
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        ids = obtener_equipo_guardado_ids(cursor, int(user["id"]))
        if not ids:
            raise HTTPException(status_code=400, detail="Save your team first")
        cursor.execute(
            "SELECT nivel FROM usuario_pokemon WHERE usuario_id=%s AND id=ANY(%s)",
            (int(user["id"]), ids),
        )
        levels = [max(1, int(row["nivel"] or 1)) for row in cursor.fetchall()]
        if not levels:
            raise HTTPException(status_code=400, detail="No valid team")
        average = round(sum(levels) / len(levels))
        # Server-defined level cap, shared with the current battle UI.
        enemy_level = max(1, min(200, average + difficulty))
        p_atk, p_def = obtener_select_stats_especiales(cursor, "pokemon", "p")
        cursor.execute(
            f"""SELECT p.id, p.nombre, p.tipo, p.hp AS hp_base,
                   p.ataque AS ataque_base, p.defensa AS defensa_base,
                   p.velocidad AS velocidad_base, {p_atk}, {p_def}
                FROM pokemon p WHERE p.id = ANY(%s)""",
            (AI_ROSTERS[difficulty],),
        )
        found = {int(row["id"]): row for row in cursor.fetchall()}
        if any(pid not in found for pid in AI_ROSTERS[difficulty]):
            raise HTTPException(status_code=500, detail="AI roster is incomplete")
        rivals = []
        for slot, pid in enumerate(AI_ROSTERS[difficulty], 1):
            p = found[pid]
            stats = calcular_stats(
                p["hp_base"], p["ataque_base"], p["defensa_base"],
                p["velocidad_base"], enemy_level, False,
                base_ataque_especial=p["ataque_especial"],
                base_defensa_especial=p["defensa_especial"],
            )
            rivals.append({
                "slot": slot, "pokemon_id": pid, "nombre": p["nombre"],
                "tipo": p["tipo"], "nivel": enemy_level, "es_shiny": False,
                **stats,
            })
        return {
            "ok": True, "mode": "preview_only", "difficulty": difficulty,
            "team_average_level": average, "rival_level": enemy_level,
            "rivals": rivals, "rewards_enabled": False,
        }
    finally:
        cursor.close()
        release_connection(conn)


# ================================================================
# V2.38 - Server-owned battle sessions (no rewards yet).
# The battle state is stored atomically in PostgreSQL. A single
# request changes a single turn; the client never submits damage/HP.
# ================================================================
import json
import random
import secrets
from datetime import datetime, timezone
from pydantic import BaseModel, Field

BATTLE_TTL_MINUTES = 45

class AIBattleStart(BaseModel):
    difficulty: int = 0

class AIBattleAction(BaseModel):
    session_token: str = Field(min_length=20, max_length=128)
    action: str
    move_slot: int | None = None
    pokemon_id: int | None = None


def _ensure_battle_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS battle_ai_sessions (
            id BIGSERIAL PRIMARY KEY,
            usuario_id BIGINT NOT NULL,
            token VARCHAR(128) UNIQUE NOT NULL,
            state JSONB NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '45 minutes')
        )
    """)
    cur.execute("""CREATE INDEX IF NOT EXISTS ix_battle_ai_user_status
                   ON battle_ai_sessions(usuario_id, status)""")


def _int(value, default=0):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _fetch_player_team(cur, user_id):
    ids = obtener_equipo_guardado_ids(cur, user_id)[:6]
    if not ids:
        raise HTTPException(400, "Save your team before battling")
    cur.execute("""
        SELECT up.id AS instance_id, up.pokemon_id, up.nivel, up.es_shiny,
               p.nombre, p.tipo, p.hp AS hp_base, p.ataque AS ataque_base,
               p.defensa AS defensa_base, p.velocidad AS velocidad_base
        FROM usuario_pokemon up JOIN pokemon p ON p.id=up.pokemon_id
        WHERE up.usuario_id=%s AND up.id=ANY(%s)
    """, (user_id, ids))
    found = {int(x["instance_id"]): x for x in cur.fetchall()}
    if len(found) != len(ids):
        raise HTTPException(400, "Saved team includes unavailable Pokémon")
    result = []
    for instance in ids:
        p = found[instance]
        stats = calcular_stats(
            p["hp_base"], p["ataque_base"], p["defensa_base"],
            p["velocidad_base"], _int(p["nivel"], 1), bool(p["es_shiny"])
        )
        # Always resolve equipped moves on the server. Client controls only move_slot.
        cur.execute("""
            SELECT upme.slot, m.id AS move_id, m.nombre, m.tipo, m.potencia
            FROM usuario_pokemon_movimientos_equipo upme
            JOIN usuario_pokemon_movimientos upm
              ON upm.id=upme.usuario_pokemon_movimiento_id
             AND upm.usuario_pokemon_id=upme.usuario_pokemon_id
             AND upm.activo=TRUE
            JOIN movimientos m ON m.id=upm.movimiento_id AND m.activo=TRUE
            WHERE upme.usuario_pokemon_id=%s ORDER BY upme.slot
        """, (instance,))
        moves = [{"slot": _int(m["slot"]), "id": _int(m["move_id"]),
                  "nombre": m["nombre"], "tipo": m["tipo"],
                  "potencia": _int(m["potencia"])} for m in cur.fetchall()]
        result.append({
            "instance_id": instance, "pokemon_id": _int(p["pokemon_id"]),
            "nombre": p["nombre"], "tipo": p["tipo"],
            "nivel": _int(p["nivel"], 1), "es_shiny": bool(p["es_shiny"]),
            **stats, "hp": stats["hp_max"], "moves": moves
        })
    return result


def _fetch_rival_team(cur, difficulty, player_team):
    ids = AI_ROSTERS[difficulty]
    level = max(1, min(200, round(sum(x["nivel"] for x in player_team) / len(player_team)) + difficulty))
    special_atk, special_def = obtener_select_stats_especiales(cur, "pokemon", "p")
    cur.execute(f"""
        SELECT p.id, p.nombre, p.tipo, p.hp AS hp_base,
               p.ataque AS ataque_base, p.defensa AS defensa_base,
               p.velocidad AS velocidad_base, {special_atk}, {special_def}
        FROM pokemon p WHERE p.id=ANY(%s)
    """, (ids,))
    found = {int(x["id"]): x for x in cur.fetchall()}
    if len(found) != len(set(ids)):
        raise HTTPException(500, "AI roster contains missing species")
    rivals = []
    for pid in ids:
        p = found[pid]
        stats = calcular_stats(
            p["hp_base"], p["ataque_base"], p["defensa_base"],
            p["velocidad_base"], level, False,
            base_ataque_especial=p["ataque_especial"],
            base_defensa_especial=p["defensa_especial"]
        )
        rivals.append({"pokemon_id": pid, "nombre": p["nombre"], "tipo": p["tipo"],
                       "nivel": level, "es_shiny": False, **stats, "hp": stats["hp_max"]})
    return rivals


def _active_index(team, idx):
    idx = _int(idx)
    if 0 <= idx < len(team) and team[idx]["hp"] > 0:
        return idx
    return next((i for i,p in enumerate(team) if p["hp"] > 0), None)


# V2.41 authoritative matchup table. Types are normalized before lookup.
TYPE_CHART = {
 "normal":{"roca":.5,"acero":.5,"fantasma":0},
 "fuego":{"planta":2,"hielo":2,"bicho":2,"acero":2,"fuego":.5,"agua":.5,"roca":.5,"dragon":.5},
 "agua":{"fuego":2,"tierra":2,"roca":2,"agua":.5,"planta":.5,"dragon":.5},
 "planta":{"agua":2,"tierra":2,"roca":2,"fuego":.5,"planta":.5,"veneno":.5,"volador":.5,"bicho":.5,"dragon":.5,"acero":.5},
 "electrico":{"agua":2,"volador":2,"planta":.5,"electrico":.5,"dragon":.5,"tierra":0},
 "hielo":{"planta":2,"tierra":2,"volador":2,"dragon":2,"fuego":.5,"agua":.5,"hielo":.5,"acero":.5},
 "lucha":{"normal":2,"hielo":2,"roca":2,"siniestro":2,"acero":2,"veneno":.5,"volador":.5,"psiquico":.5,"bicho":.5,"hada":.5,"fantasma":0},
 "veneno":{"planta":2,"hada":2,"veneno":.5,"tierra":.5,"roca":.5,"fantasma":.5,"acero":0},
 "tierra":{"fuego":2,"electrico":2,"veneno":2,"roca":2,"acero":2,"planta":.5,"bicho":.5,"volador":0},
 "volador":{"planta":2,"lucha":2,"bicho":2,"electrico":.5,"roca":.5,"acero":.5},
 "psiquico":{"lucha":2,"veneno":2,"psiquico":.5,"acero":.5,"siniestro":0},
 "bicho":{"planta":2,"psiquico":2,"siniestro":2,"fuego":.5,"lucha":.5,"veneno":.5,"volador":.5,"fantasma":.5,"acero":.5,"hada":.5},
 "roca":{"fuego":2,"hielo":2,"volador":2,"bicho":2,"lucha":.5,"tierra":.5,"acero":.5},
 "fantasma":{"psiquico":2,"fantasma":2,"siniestro":.5,"normal":0},
 "dragon":{"dragon":2,"acero":.5,"hada":0},
 "siniestro":{"psiquico":2,"fantasma":2,"lucha":.5,"siniestro":.5,"hada":.5},
 "acero":{"hielo":2,"roca":2,"hada":2,"fuego":.5,"agua":.5,"electrico":.5,"acero":.5},
 "hada":{"lucha":2,"dragon":2,"siniestro":2,"fuego":.5,"veneno":.5,"acero":.5},
}
TYPE_ALIASES={"fire":"fuego","water":"agua","grass":"planta","electric":"electrico","ice":"hielo","fighting":"lucha","poison":"veneno","ground":"tierra","flying":"volador","psychic":"psiquico","bug":"bicho","rock":"roca","ghost":"fantasma","dark":"siniestro","steel":"acero","fairy":"hada","normal":"normal","dragon":"dragon"}
def _type_name(value):
    import unicodedata
    v=unicodedata.normalize("NFKD",str(value or "normal").lower().strip())
    v="".join(c for c in v if not unicodedata.combining(c))
    return TYPE_ALIASES.get(v,v)
def _type_matchup(attack_type, defender):
    kind=_type_name(attack_type)
    row=TYPE_CHART.get(kind,{})
    import re
    defensive=[_type_name(t) for t in re.split(r"[,/;]+",str(defender.get("tipo") or "normal")) if t.strip()]
    value=1.0
    for item in defensive[:2]:
        value*=row.get(item,1.0)
    return value

def _damage(attacker, defender, power, difficulty, rng, effectiveness=1.0):
    # V2.38 baseline. Server-owned, bounded and stat-based; matchup/status
    # tuning follows before activating rewards.
    if effectiveness == 0:\n        return 0\n    level = max(1, _int(attacker["nivel"], 1))
    attack = max(1, _int(attacker.get("ataque")))
    defense = max(1, _int(defender.get("defensa")))
    raw = (((2 * level / 5 + 2) * max(1, power) * attack / defense) / 50) + 2
    factor = rng.uniform(.85, 1.0)
    factor *= 1 + .04 * difficulty if difficulty else 1.0
    return max(1, min(_int(defender["hp_max"]), round(raw * factor * effectiveness)))


def _safe_state(state):
    # The token is never included in the state body, only at API root.
    return {"difficulty": state["difficulty"], "turn": state["turn"],
            "status": state["status"], "player": state["player"],
            "rivals": state["rivals"], "player_active": state["player_active"],
            "rival_active": state["rival_active"], "log": state["log"][-20:],
            "rewards_enabled": False}


def _get_session(cur, user_id, token, lock=False):
    cur.execute("""
       SELECT id, state, status, expires_at
       FROM battle_ai_sessions
       WHERE usuario_id=%s AND token=%s
       FOR UPDATE
    """ if lock else """
       SELECT id, state, status, expires_at
       FROM battle_ai_sessions WHERE usuario_id=%s AND token=%s
    """, (user_id, token))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Battle session not found")
    state = row["state"]
    if isinstance(state, str):
        state = json.loads(state)
    if row["expires_at"] < datetime.now(timezone.utc) and state["status"] == "active":
        raise HTTPException(410, "Battle session expired")
    return row, state


@router_battle_ai.post("/battle/ai/start")
def battle_ai_start(data: AIBattleStart, user=Depends(get_current_user)):
    if data.difficulty not in AI_ROSTERS:
        raise HTTPException(400, "Invalid difficulty; choose 0, 2, 4, or 6")
    conn = get_connection()
    cur = get_cursor(conn)
    try:
        _ensure_battle_table(cur)
        user_id = int(user["id"])
        player = _fetch_player_team(cur, user_id)
        rivals = _fetch_rival_team(cur, data.difficulty, player)
        # A user can have only one active IA battle at a time.
        cur.execute("UPDATE battle_ai_sessions SET status='replaced' WHERE usuario_id=%s AND status='active'", (user_id,))
        token = secrets.token_urlsafe(40)
        state = {"difficulty": data.difficulty, "turn": 1, "status": "active",
                 "player": player, "rivals": rivals, "player_active": 0, "rival_active": 0,
                 "log": [{"turn": 1, "event": "start"}]}
        cur.execute("""INSERT INTO battle_ai_sessions(usuario_id, token, state)
                       VALUES (%s, %s, %s::jsonb)""", (user_id, token, json.dumps(state)))
        conn.commit()
        return {"ok": True, "session_token": token, **_safe_state(state)}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        release_connection(conn)


@router_battle_ai.get("/battle/ai/state")
def battle_ai_state(session_token: str, user=Depends(get_current_user)):
    conn = get_connection()
    cur = get_cursor(conn)
    try:
        _, state = _get_session(cur, int(user["id"]), session_token)
        return {"ok": True, **_safe_state(state)}
    finally:
        conn.rollback()
        cur.close()
        release_connection(conn)


@router_battle_ai.post("/battle/ai/action")
def battle_ai_action(data: AIBattleAction, user=Depends(get_current_user)):
    if data.action not in ("attack", "switch"):
        raise HTTPException(400, "Action must be attack or switch")
    conn = get_connection()
    cur = get_cursor(conn)
    try:
        row, state = _get_session(cur, int(user["id"]), data.session_token, lock=True)
        if state["status"] != "active":
            raise HTTPException(409, "Battle already ended")
        players, rivals = state["player"], state["rivals"]
        pi, ri = state["player_active"], state["rival_active"]
        p = players[pi]
        enemy = rivals[ri]
        turn = state["turn"]
        events = []
        if data.action == "switch":
            target = next((i for i,x in enumerate(players)
                           if x["instance_id"] == data.pokemon_id and x["hp"] > 0), None)
            if target is None or target == pi:
                raise HTTPException(400, "Choose a different available team Pokémon")
            state["player_active"] = target
            p = players[target]
            events.append({"turn": turn, "event": "switch", "pokemon_id": p["instance_id"]})
        else:
            move = next((m for m in p["moves"] if m["slot"] == data.move_slot), None)
            if move is None:
                raise HTTPException(400, "This Pokémon has no equipped move in that slot")
            rng = random.SystemRandom()
            power = max(0, min(250, move["potencia"]))
            if power == 0:
                raise HTTPException(400, "Status moves are not supported in V2.38")
            effectiveness = _type_matchup(move["tipo"], enemy)
            dealt = min(enemy["hp"], _damage(p, enemy, power, 0, rng, effectiveness))
            enemy["hp"] -= dealt
            events.append({"turn": turn, "event": "attack", "pokemon_id": p["instance_id"],
                           "move_id": move["id"], "move_name": move["nombre"], "move_type": move["tipo"],
                           "effectiveness": effectiveness, "damage": dealt, "target_hp": enemy["hp"]})
        # Fainted foes do not counterattack.
        if enemy["hp"] == 0:
            next_rival = _active_index(rivals, ri)
            if next_rival is None:
                state["status"] = "victory"
                events.append({"turn": turn, "event": "victory", "rewards_granted": False})
            else:
                state["rival_active"] = next_rival
                events.append({"turn": turn, "event": "rival_switch",
                               "pokemon_id": rivals[next_rival]["pokemon_id"]})
        else:
            # Server-controlled AI response; difficulty influences move choice
            # and damage. Future iteration adds full move-learning and matchup AI.
            rng = random.SystemRandom()
            enemy_power = rng.choice([35, 45, 55, 65]) if state["difficulty"] < 4 else rng.choice([50, 60, 75, 90])
            enemy_type = str(enemy.get("tipo") or "normal").split("/")[0].split(",")[0].strip()
            enemy_effectiveness = _type_matchup(enemy_type, p)
            hit = min(p["hp"], _damage(enemy, p, enemy_power, state["difficulty"], rng, enemy_effectiveness))
            p["hp"] -= hit
            events.append({"turn": turn, "event": "rival_attack",
                           "pokemon_id": enemy["pokemon_id"], "move_name": "AI "+enemy_type,
                           "move_type": enemy_type, "effectiveness": enemy_effectiveness,
                           "damage": hit, "target_hp": p["hp"]})
            if p["hp"] == 0:
                next_player = _active_index(players, state["player_active"])
                if next_player is None:
                    state["status"] = "defeat"
                    events.append({"turn": turn, "event": "defeat", "rewards_granted": False})
                else:
                    state["player_active"] = next_player
                    events.append({"turn": turn, "event": "player_switch",
                                   "pokemon_id": players[next_player]["instance_id"]})
        state["log"].extend(events)
        state["log"] = state["log"][-60:]
        state["turn"] += 1
        cur.execute("""UPDATE battle_ai_sessions
                       SET state=%s::jsonb, status=%s, updated_at=NOW()
                       WHERE id=%s""", (json.dumps(state), state["status"], row["id"]))
        conn.commit()
        return {"ok": True, "events": events, **_safe_state(state)}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        release_connection(conn)

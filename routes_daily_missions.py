"""Mastersmon daily missions, separate from one-time onboarding.
Requires the SQL migration shipped in this package. Server time: America/Lima.
"""
from datetime import datetime, time, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from server_time import server_now, SERVER_TIMEZONE

router_daily_missions = APIRouter(tags=["Misiones diarias"])
MISSIONS = (("capturar_1", 1, 500), ("capturar_3", 3, 1200), ("batalla_1", 1, 800))

class ClaimDaily(BaseModel):
    codigo: str

def _day_bounds():
    today = server_now().date()
    start = datetime.combine(today, time.min, tzinfo=SERVER_TIMEZONE)
    end = start + timedelta(days=1)
    # Existing timestamp without time zone columns are stored by Render/PostgreSQL in UTC.
    return today, start.astimezone(timezone.utc).replace(tzinfo=None), end.astimezone(timezone.utc).replace(tzinfo=None), end

def _progress(cursor, user_id, start_utc, end_utc):
    # Captures tracked in user collection. Dates are UTC timestamps without time zone.
    cursor.execute("""SELECT COUNT(*) AS n FROM usuario_pokemon
        WHERE usuario_id=%s AND fecha_captura >= %s AND fecha_captura < %s""",
        (user_id, start_utc, end_utc))
    captured = int((cursor.fetchone() or {}).get("n") or 0)
    cursor.execute("""SELECT COUNT(*) AS n FROM battle_sesiones
        WHERE usuario_id=%s AND resultado='reclamada' AND finalizado_en >= %s AND finalizado_en < %s""",
        (user_id, start_utc, end_utc))
    legacy_battles = int((cursor.fetchone() or {}).get("n") or 0)
    # Current Battle IA records the completed server-validated victory in battle_ai_sessions.
    # updated_at is TIMESTAMPTZ; convert UTC-naive daily bounds to aware UTC datetimes.
    begin_aware = start_utc.replace(tzinfo=timezone.utc)
    end_aware = end_utc.replace(tzinfo=timezone.utc)
    cursor.execute("""SELECT COUNT(*) AS n FROM battle_ai_sessions
        WHERE usuario_id=%s AND status='victory'
          AND state->>'reward_claimed'='true'
          AND updated_at >= %s AND updated_at < %s""",
        (user_id, begin_aware, end_aware))
    ai_battles = int((cursor.fetchone() or {}).get("n") or 0)
    battles = legacy_battles + ai_battles
    return {"capturar_1": captured, "capturar_3": captured, "batalla_1": battles}

def _response(cursor, user_id):
    day,start_utc,end_utc,next_reset = _day_bounds()
    progress = _progress(cursor,user_id,start_utc,end_utc)
    cursor.execute("SELECT codigo FROM usuario_misiones_diarias WHERE usuario_id=%s AND fecha=%s",(user_id,day))
    claimed = {str(r["codigo"]) for r in cursor.fetchall()}
    return {"ok":True,"fecha":day.isoformat(),"zona_horaria":str(SERVER_TIMEZONE),"cambia_en":next_reset.isoformat(),
        "misiones":[{"codigo":code,"objetivo":goal,"actual":min(progress[code],goal),
             "completada":progress[code]>=goal,"reclamada":code in claimed,
             "recompensa_pokedolares":reward} for code,goal,reward in MISSIONS]}

@router_daily_missions.get('/missions/daily/me')
def daily_me(usuario=Depends(get_current_user)):
    con=get_connection();cur=get_cursor(con)
    try:return _response(cur,int(usuario["id"]))
    finally:cur.close();release_connection(con)

@router_daily_missions.post('/missions/daily/claim')
def claim_daily(payload:ClaimDaily,usuario=Depends(get_current_user)):
    selected=next(((code,goal,reward) for code,goal,reward in MISSIONS if code==payload.codigo),None)
    if selected is None:raise HTTPException(status_code=404,detail='Misión diaria desconocida')
    code,goal,reward=selected
    con=get_connection();cur=get_cursor(con)
    try:
        user_id=int(usuario["id"])
        # Serializes rewards for this account, preventing concurrent duplicate grants.
        cur.execute('SELECT id FROM usuarios WHERE id=%s FOR UPDATE',(user_id,))
        if not cur.fetchone():raise HTTPException(status_code=404,detail='Usuario no encontrado')
        day,start,end,_=_day_bounds()
        if _progress(cur,user_id,start,end)[code]<goal:
            raise HTTPException(status_code=409,detail='Misión no completada')
        cur.execute("""INSERT INTO usuario_misiones_diarias (usuario_id,fecha,codigo,recompensa_pokedolares)
            VALUES (%s,%s,%s,%s) ON CONFLICT (usuario_id,fecha,codigo) DO NOTHING RETURNING codigo""",
            (user_id,day,code,reward))
        granted=bool(cur.fetchone())
        if granted:cur.execute('UPDATE usuarios SET pokedolares = pokedolares + %s WHERE id=%s',(reward,user_id))
        con.commit()
        return {'ok':True,'recompensa_aplicada':granted,'recompensa_pokedolares':reward if granted else 0,
                'misiones':_response(cur,user_id)['misiones']}
    except HTTPException:
        con.rollback();raise
    except Exception:
        con.rollback();raise
    finally:cur.close();release_connection(con)

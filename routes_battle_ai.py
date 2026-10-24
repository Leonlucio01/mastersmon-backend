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

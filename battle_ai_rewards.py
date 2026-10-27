"""Atomic, server-calculated rewards for completed AI battles."""
from fastapi import HTTPException
from routes_pokemon import (
    MAX_NIVEL_POKEMON, obtener_select_stats_especiales,
    obtener_select_bonus_renacer_especiales, stats_especiales_habilitados,
    calcular_stats, sincronizar_movimientos_usuario_pokemon,
    validar_ids_pokemon_equipo,
)
from monetization_utils import aplicar_multiplicadores_recompensa_batalla

BASE_AI_REWARDS = {0: (4000, 3500), 2: (5000, 4500), 4: (6000, 5500), 6: (7000, 6500)}

def grant_ai_victory(cursor, user_id, state):
    """Called ONCE while the winning battle session row is FOR UPDATE locked.
    Caller owns transaction and commits state + currency + EXP atomically.
    """
    if state.get("status") != "victory" or state.get("reward_claimed"):
        raise HTTPException(409, "AI battle reward not available")
    difficulty = int(state["difficulty"])
    if difficulty not in BASE_AI_REWARDS:
        raise HTTPException(400, "Invalid reward difficulty")
    pd_base, exp_base = BASE_AI_REWARDS[difficulty]
    ids_limpios = validar_ids_pokemon_equipo(cursor, user_id,
        [int(p["instance_id"]) for p in state["player"]])
    multiplicadores = aplicar_multiplicadores_recompensa_batalla(cursor, user_id, exp_base, pd_base)
    exp_ganada = int(multiplicadores["exp_final"])
    pokedolares_ganados = int(multiplicadores["pokedolares_final"])
    cursor.execute("UPDATE usuarios SET pokedolares = COALESCE(pokedolares,0) + %s WHERE id=%s RETURNING pokedolares",
        (pokedolares_ganados, user_id))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(404, "Trainer not found")
    nuevos_pokedolares = int(row["pokedolares"])
    usuario = {"id": user_id}
    pokemon_actualizados = []
    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
    bonus_atk_sp_sql, bonus_def_sp_sql = obtener_select_bonus_renacer_especiales(cursor, "up")
    up_has_special = stats_especiales_habilitados(cursor, "usuario_pokemon")
    pokemon_actualizados = []

    for usuario_pokemon_id in ids_limpios:
        cursor.execute(f"""
            SELECT
                up.id,
                up.usuario_id,
                up.pokemon_id,
                up.nivel,
                up.experiencia,
                COALESCE(up.experiencia_total, 0) AS experiencia_total,
                COALESCE(up.victorias_total, 0) AS victorias_total,
                up.es_shiny,
                up.bonus_hp_renacer,
                up.bonus_ataque_renacer,
                up.bonus_defensa_renacer,
                up.bonus_velocidad_renacer,
                {bonus_atk_sp_sql},
                {bonus_def_sp_sql},
                {('up.ataque_especial AS ataque_especial, up.defensa_especial AS defensa_especial,' if up_has_special else 'up.ataque AS ataque_especial, up.defensa AS defensa_especial,')}
                p.hp,
                p.ataque,
                p.defensa,
                p.velocidad,
                {p_atk_sp_sql},
                {p_def_sp_sql}
            FROM usuario_pokemon up
            JOIN pokemon p ON p.id = up.pokemon_id
            WHERE up.id = %s AND up.usuario_id = %s
            FOR UPDATE
        """, (usuario_pokemon_id, usuario["id"]))
        poke = cursor.fetchone()
        if not poke:
            continue

        nivel_actual = int(poke["nivel"] or 1)
        exp_actual = int(poke["experiencia"] or 0)
        exp_total_actual = int(poke["experiencia_total"] or 0)
        victorias_actuales = int(poke["victorias_total"] or 0)

        nuevo_exp_total = exp_total_actual + exp_ganada
        nuevas_victorias = victorias_actuales + 1

        if nivel_actual >= MAX_NIVEL_POKEMON:
            nuevo_nivel = MAX_NIVEL_POKEMON
            nueva_exp = 0
        else:
            nueva_exp = exp_actual + exp_ganada
            nuevo_nivel = nivel_actual
            while nuevo_nivel < MAX_NIVEL_POKEMON and nueva_exp >= (nuevo_nivel * 50):
                nueva_exp -= (nuevo_nivel * 50)
                nuevo_nivel += 1
            if nuevo_nivel >= MAX_NIVEL_POKEMON:
                nuevo_nivel = MAX_NIVEL_POKEMON
                nueva_exp = 0

        stats = calcular_stats(
            poke["hp"],
            poke["ataque"],
            poke["defensa"],
            poke["velocidad"],
            nuevo_nivel,
            bool(poke["es_shiny"]),
            poke["bonus_hp_renacer"],
            poke["bonus_ataque_renacer"],
            poke["bonus_defensa_renacer"],
            poke["bonus_velocidad_renacer"],
            poke.get("ataque_especial"),
            poke.get("defensa_especial"),
            poke.get("bonus_ataque_especial_renacer"),
            poke.get("bonus_defensa_especial_renacer"),
        )

        if up_has_special:
            cursor.execute("""
                UPDATE usuario_pokemon
                SET nivel = %s,
                    experiencia = %s,
                    experiencia_total = %s,
                    victorias_total = %s,
                    hp_max = %s,
                    hp_actual = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s,
                    ataque_especial = %s,
                    defensa_especial = %s
                WHERE id = %s
            """, (
                nuevo_nivel,
                nueva_exp,
                nuevo_exp_total,
                nuevas_victorias,
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                stats["ataque_especial"],
                stats["defensa_especial"],
                usuario_pokemon_id,
            ))
        else:
            cursor.execute("""
                UPDATE usuario_pokemon
                SET nivel = %s,
                    experiencia = %s,
                    experiencia_total = %s,
                    victorias_total = %s,
                    hp_max = %s,
                    hp_actual = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s
                WHERE id = %s
            """, (
                nuevo_nivel,
                nueva_exp,
                nuevo_exp_total,
                nuevas_victorias,
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                usuario_pokemon_id,
            ))

        sincronizar_movimientos_usuario_pokemon(
            cursor,
            usuario_pokemon_id,
            int(poke["pokemon_id"]),
            nuevo_nivel,
            origen="nivel",
            auto_equipar=True,
        )

        pokemon_actualizados.append({
            "usuario_pokemon_id": usuario_pokemon_id,
            "nivel": nuevo_nivel,
            "experiencia": nueva_exp,
            "experiencia_total": nuevo_exp_total,
            "victorias_total": nuevas_victorias,
            "hp_max": stats["hp_max"],
            "ataque": stats["ataque"],
            "defensa": stats["defensa"],
            "velocidad": stats["velocidad"],
            "ataque_especial": stats["ataque_especial"],
            "defensa_especial": stats["defensa_especial"],
        })

    state["reward_claimed"] = True
    state["reward"] = {
        "pokedolares_ganados": pokedolares_ganados,
        "exp_ganada": exp_ganada,
        "pokedolares_actuales": nuevos_pokedolares,
        "pokemon_actualizados": pokemon_actualizados,
        "exp_x2_aplicado": bool(multiplicadores.get("exp_multiplier", 1) > 1),
        "gold_x2_aplicado": bool(multiplicadores.get("gold_multiplier", 1) > 1),
    }
    return state["reward"]

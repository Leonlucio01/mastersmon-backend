import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from monetization_utils import aplicar_multiplicadores_recompensa_batalla
from routes_pokemon import (
    MAX_NIVEL_POKEMON,
    calcular_stats,
    crear_token_simple,
    existe_tabla,
    guardar_equipo_usuario,
    normalizar_equipo_ids_json,
    obtener_equipo_guardado_ids,
    obtener_select_bonus_renacer_especiales,
    obtener_select_stats_especiales,
    registrar_actividad_usuario,
    stats_especiales_habilitados,
    validar_ids_pokemon_equipo,
)

router_gyms = APIRouter()

GYM_SESSION_TTL_SEGUNDOS = 60 * 45
GYM_SESSION_MINIMA_SEGUNDOS = 8


class GymIniciarPayload(BaseModel):
    gym_codigo: str
    usuario_pokemon_ids: list[int] | None = None
    guardar_equipo: bool = True


class GymRecompensaPayload(BaseModel):
    gym_session_token: str
    victoria: bool = False
    turnos: int = 0


class GymCancelarPayload(BaseModel):
    gym_session_token: str | None = None


# =========================================================
# HELPERS
# =========================================================

def ahora_server_naive() -> datetime:
    return datetime.now()


def asegurar_tablas_gyms(cursor):
    requeridas = (
        "gym_regiones",
        "gym_catalogo",
        "gym_equipo_rival",
        "gym_usuario_progreso",
        "gym_sesiones",
    )
    faltantes = [tabla for tabla in requeridas if not existe_tabla(cursor, tabla)]
    if faltantes:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Faltan tablas de Gyms: {', '.join(faltantes)}",
        )


def invalidar_sesiones_gym_activas(cursor, usuario_id: int):
    cursor.execute(
        """
        UPDATE gym_sesiones
        SET activo = FALSE,
            resultado = CASE
                WHEN resultado = 'pendiente' THEN 'cancelada'
                ELSE resultado
            END,
            cancelado_en = COALESCE(cancelado_en, NOW()),
            finalizado_en = COALESCE(finalizado_en, NOW())
        WHERE usuario_id = %s
          AND activo = TRUE
        """,
        (usuario_id,),
    )


def obtener_sesion_gym_por_token(cursor, token: str, usuario_id: int, for_update: bool = False):
    sql_for_update = " FOR UPDATE" if for_update else ""
    cursor.execute(
        f"""
        SELECT gs.*, gc.codigo AS gym_codigo, gc.lider_nombre, gc.medalla_nombre, gc.reward_exp AS gym_reward_exp,
               gc.reward_pokedolares AS gym_reward_pokedolares
        FROM gym_sesiones gs
        JOIN gym_catalogo gc ON gc.id = gs.gym_id
        WHERE gs.token = %s
          AND gs.usuario_id = %s
        LIMIT 1
        {sql_for_update}
        """,
        (str(token or "").strip(), int(usuario_id)),
    )
    return cursor.fetchone()



def asegurar_progreso_usuario_gyms(cursor, usuario_id: int):
    cursor.execute(
        """
        INSERT INTO gym_usuario_progreso (usuario_id, gym_id, desbloqueado, completado, intentos, victorias)
        SELECT %s, gc.id, gc.desbloqueado_por_defecto, FALSE, 0, 0
        FROM gym_catalogo gc
        LEFT JOIN gym_usuario_progreso gup
            ON gup.usuario_id = %s
           AND gup.gym_id = gc.id
        WHERE gc.activo = TRUE
          AND gup.id IS NULL
        """,
        (usuario_id, usuario_id),
    )

    cursor.execute(
        """
        UPDATE gym_usuario_progreso gup
        SET desbloqueado = CASE
                WHEN gup.completado = TRUE THEN TRUE
                WHEN gc.desbloqueado_por_defecto = TRUE THEN TRUE
                WHEN gc.gym_requerido_id IS NOT NULL AND COALESCE(prev.completado, FALSE) = TRUE THEN TRUE
                ELSE FALSE
            END,
            actualizado_en = CURRENT_TIMESTAMP
        FROM gym_catalogo gc
        LEFT JOIN gym_usuario_progreso prev
            ON prev.usuario_id = %s
           AND prev.gym_id = gc.gym_requerido_id
        WHERE gup.usuario_id = %s
          AND gup.gym_id = gc.id
        """,
        (usuario_id, usuario_id),
    )


def obtener_gym_por_codigo(cursor, gym_codigo: str):
    cursor.execute(
        """
        SELECT
            gc.*,
            gr.codigo AS region_codigo,
            gr.nombre AS region_nombre,
            gr.orden AS region_orden
        FROM gym_catalogo gc
        JOIN gym_regiones gr ON gr.id = gc.region_id
        WHERE gc.codigo = %s
          AND gc.activo = TRUE
        LIMIT 1
        """,
        (str(gym_codigo or "").strip().lower(),),
    )
    return cursor.fetchone()


def obtener_catalogo_gyms_usuario(cursor, usuario_id: int):
    asegurar_progreso_usuario_gyms(cursor, usuario_id)
    cursor.execute(
        """
        SELECT
            gc.id,
            gc.codigo,
            gc.lider_nombre,
            gc.lider_slug,
            gc.ciudad,
            gc.medalla_nombre,
            gc.medalla_slug,
            gc.tipo_principal,
            gc.orden_region,
            gc.nivel_recomendado,
            gc.reward_exp,
            gc.reward_pokedolares,
            gc.trainer_asset_path,
            gc.badge_asset_path,
            gc.descripcion,
            gc.pista,
            gc.fase,
            gc.enemy_style,
            gc.desbloqueado_por_defecto,
            gc.gym_requerido_id,
            gr.codigo AS region_codigo,
            gr.nombre AS region_nombre,
            gr.orden AS region_orden,
            COALESCE(gup.desbloqueado, FALSE) AS desbloqueado,
            COALESCE(gup.completado, FALSE) AS completado,
            COALESCE(gup.intentos, 0) AS intentos,
            COALESCE(gup.victorias, 0) AS victorias,
            gup.mejor_turnos,
            gup.completado_en,
            req.codigo AS gym_requerido_codigo,
            req.lider_nombre AS gym_requerido_lider
        FROM gym_catalogo gc
        JOIN gym_regiones gr ON gr.id = gc.region_id
        LEFT JOIN gym_usuario_progreso gup
            ON gup.gym_id = gc.id
           AND gup.usuario_id = %s
        LEFT JOIN gym_catalogo req
            ON req.id = gc.gym_requerido_id
        WHERE gc.activo = TRUE
          AND gr.activo = TRUE
        ORDER BY gr.orden ASC, gc.orden_region ASC
        """,
        (usuario_id,),
    )
    return cursor.fetchall()


def obtener_resumen_regiones(catalogo_rows: list[dict]):
    regiones: dict[str, dict] = {}
    for row in catalogo_rows:
        codigo = row["region_codigo"]
        region = regiones.setdefault(
            codigo,
            {
                "codigo": codigo,
                "nombre": row["region_nombre"],
                "orden": int(row["region_orden"] or 0),
                "total_gyms": 0,
                "completados": 0,
                "desbloqueados": 0,
                "porcentaje": 0,
            },
        )
        region["total_gyms"] += 1
        if bool(row["desbloqueado"]):
            region["desbloqueados"] += 1
        if bool(row["completado"]):
            region["completados"] += 1

    regiones_list = sorted(regiones.values(), key=lambda item: item["orden"])
    for region in regiones_list:
        total = max(1, int(region["total_gyms"] or 0))
        region["porcentaje"] = int(round((int(region["completados"] or 0) / total) * 100))
    return regiones_list


def obtener_equipo_rival_gym(cursor, gym_id: int) -> list[dict]:
    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")

    cursor.execute(
        f"""
        SELECT
            ger.slot,
            ger.nivel,
            ger.es_lider,
            ger.es_shiny,
            p.id AS pokemon_id,
            p.nombre,
            p.tipo,
            p.imagen,
            COALESCE(ger.hp_override, p.hp) AS hp_base,
            COALESCE(ger.ataque_override, p.ataque) AS ataque_base,
            COALESCE(ger.defensa_override, p.defensa) AS defensa_base,
            COALESCE(ger.velocidad_override, p.velocidad) AS velocidad_base,
            COALESCE(ger.ataque_especial_override, {p_atk_sp_sql.split(' AS ')[0]}) AS ataque_especial_base,
            COALESCE(ger.defensa_especial_override, {p_def_sp_sql.split(' AS ')[0]}) AS defensa_especial_base
        FROM gym_equipo_rival ger
        JOIN pokemon p ON p.id = ger.pokemon_id
        WHERE ger.gym_id = %s
          AND ger.activo = TRUE
        ORDER BY ger.slot ASC, ger.id ASC
        """,
        (gym_id,),
    )
    rows = cursor.fetchall()

    equipo = []
    for row in rows:
        stats = calcular_stats(
            row["hp_base"],
            row["ataque_base"],
            row["defensa_base"],
            row["velocidad_base"],
            int(row["nivel"] or 1),
            bool(row["es_shiny"]),
            0,
            0,
            0,
            0,
            row.get("ataque_especial_base"),
            row.get("defensa_especial_base"),
            0,
            0,
        )
        equipo.append(
            {
                "slot": int(row["slot"] or 0),
                "pokemon_id": int(row["pokemon_id"] or 0),
                "nombre": row["nombre"],
                "tipo": row.get("tipo") or "",
                "imagen": row.get("imagen"),
                "nivel": int(row["nivel"] or 1),
                "es_lider": bool(row.get("es_lider")),
                "es_shiny": bool(row.get("es_shiny")),
                "hp": int(stats["hp_max"]),
                "hp_max": int(stats["hp_max"]),
                "ataque": int(stats["ataque"]),
                "defensa": int(stats["defensa"]),
                "velocidad": int(stats["velocidad"]),
                "ataque_especial": int(stats.get("ataque_especial") or stats["ataque"]),
                "defensa_especial": int(stats.get("defensa_especial") or stats["defensa"]),
            }
        )
    return equipo


def obtener_equipo_usuario_snapshot(cursor, usuario_id: int, usuario_pokemon_ids: list[int]) -> list[dict]:
    ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)
    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
    bonus_atk_sp_sql, bonus_def_sp_sql = obtener_select_bonus_renacer_especiales(cursor, "up")
    up_has_special = stats_especiales_habilitados(cursor, "usuario_pokemon")

    cursor.execute(
        f"""
        SELECT
            up.id,
            up.pokemon_id,
            up.nivel,
            up.experiencia,
            COALESCE(up.experiencia_total, 0) AS experiencia_total,
            up.es_shiny,
            up.hp_actual,
            up.hp_max,
            up.ataque,
            up.defensa,
            up.velocidad,
            {('up.ataque_especial AS ataque_especial, up.defensa_especial AS defensa_especial,' if up_has_special else 'up.ataque AS ataque_especial, up.defensa AS defensa_especial,')}
            p.nombre,
            p.tipo,
            p.imagen,
            p.hp AS base_hp,
            p.ataque AS base_ataque,
            p.defensa AS base_defensa,
            p.velocidad AS base_velocidad,
            {p_atk_sp_sql},
            {p_def_sp_sql},
            COALESCE(up.bonus_hp_renacer, 0) AS bonus_hp_renacer,
            COALESCE(up.bonus_ataque_renacer, 0) AS bonus_ataque_renacer,
            COALESCE(up.bonus_defensa_renacer, 0) AS bonus_defensa_renacer,
            COALESCE(up.bonus_velocidad_renacer, 0) AS bonus_velocidad_renacer,
            {bonus_atk_sp_sql},
            {bonus_def_sp_sql}
        FROM usuario_pokemon up
        JOIN pokemon p ON p.id = up.pokemon_id
        WHERE up.usuario_id = %s
          AND up.id = ANY(%s)
        ORDER BY array_position(%s::int[], up.id)
        """,
        (usuario_id, ids_limpios, ids_limpios),
    )
    rows = cursor.fetchall()

    snapshot = []
    for row in rows:
        snapshot.append(
            {
                "usuario_pokemon_id": int(row["id"] or 0),
                "pokemon_id": int(row["pokemon_id"] or 0),
                "nombre": row["nombre"],
                "tipo": row.get("tipo") or "",
                "imagen": row.get("imagen"),
                "nivel": int(row["nivel"] or 1),
                "experiencia": int(row["experiencia"] or 0),
                "experiencia_total": int(row["experiencia_total"] or 0),
                "es_shiny": bool(row["es_shiny"]),
                "hp": int(row.get("hp_actual") or row.get("hp_max") or 0),
                "hp_max": int(row.get("hp_max") or 0),
                "ataque": int(row.get("ataque") or 0),
                "defensa": int(row.get("defensa") or 0),
                "velocidad": int(row.get("velocidad") or 0),
                "ataque_especial": int(row.get("ataque_especial") or row.get("ataque") or 0),
                "defensa_especial": int(row.get("defensa_especial") or row.get("defensa") or 0),
            }
        )
    return snapshot


def actualizar_desbloqueos_post_victoria(cursor, usuario_id: int, gym_id: int):
    cursor.execute(
        """
        UPDATE gym_usuario_progreso
        SET desbloqueado = TRUE,
            completado = TRUE,
            victorias = victorias + 1,
            completado_en = COALESCE(completado_en, NOW()),
            actualizado_en = CURRENT_TIMESTAMP
        WHERE usuario_id = %s
          AND gym_id = %s
        """,
        (usuario_id, gym_id),
    )
    asegurar_progreso_usuario_gyms(cursor, usuario_id)


def aplicar_recompensas_gym(cursor, usuario_id: int, usuario_pokemon_ids: list[int], exp_ganada: int, pokedolares_ganados: int):
    ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)

    cursor.execute(
        """
        SELECT id, pokedolares
        FROM usuarios
        WHERE id = %s
        FOR UPDATE
        """,
        (usuario_id,),
    )
    usuario_db = cursor.fetchone()
    if not usuario_db:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    nuevos_pokedolares = int(usuario_db["pokedolares"] or 0) + int(pokedolares_ganados or 0)
    cursor.execute(
        "UPDATE usuarios SET pokedolares = %s WHERE id = %s",
        (nuevos_pokedolares, usuario_id),
    )

    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
    bonus_atk_sp_sql, bonus_def_sp_sql = obtener_select_bonus_renacer_especiales(cursor, "up")
    up_has_special = stats_especiales_habilitados(cursor, "usuario_pokemon")

    pokemon_actualizados = []
    for usuario_pokemon_id in ids_limpios:
        cursor.execute(
            f"""
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
            """,
            (usuario_pokemon_id, usuario_id),
        )
        poke = cursor.fetchone()
        if not poke:
            continue

        nivel_actual = int(poke["nivel"] or 1)
        exp_actual = int(poke["experiencia"] or 0)
        exp_total_actual = int(poke["experiencia_total"] or 0)
        victorias_actuales = int(poke["victorias_total"] or 0)

        nuevo_exp_total = exp_total_actual + int(exp_ganada or 0)
        nuevas_victorias = victorias_actuales + 1

        if nivel_actual >= MAX_NIVEL_POKEMON:
            nuevo_nivel = MAX_NIVEL_POKEMON
            nueva_exp = 0
        else:
            nueva_exp = exp_actual + int(exp_ganada or 0)
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
            cursor.execute(
                """
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
                """,
                (
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
                ),
            )
        else:
            cursor.execute(
                """
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
                """,
                (
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
                ),
            )

        pokemon_actualizados.append(
            {
                "usuario_pokemon_id": int(usuario_pokemon_id),
                "nivel": int(nuevo_nivel),
                "experiencia": int(nueva_exp),
                "experiencia_total": int(nuevo_exp_total),
                "victorias_total": int(nuevas_victorias),
                "hp_max": int(stats["hp_max"]),
                "ataque": int(stats["ataque"]),
                "defensa": int(stats["defensa"]),
                "velocidad": int(stats["velocidad"]),
                "ataque_especial": int(stats.get("ataque_especial") or stats["ataque"]),
                "defensa_especial": int(stats.get("defensa_especial") or stats["defensa"]),
            }
        )

    return {
        "pokedolares_actuales": int(nuevos_pokedolares),
        "pokemon_actualizados": pokemon_actualizados,
    }


# =========================================================
# ROUTES
# =========================================================

@router_gyms.get("/battle/gyms/catalogo")
def obtener_catalogo_gyms(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_gyms(cursor)
        catalogo_rows = obtener_catalogo_gyms_usuario(cursor, int(usuario["id"]))
        regiones = obtener_resumen_regiones(catalogo_rows)

        gyms = []
        for row in catalogo_rows:
            roster = obtener_equipo_rival_gym(cursor, int(row["id"]))
            nivel_recomendado_catalogo = int(row["nivel_recomendado"] or 1)
            nivel_recomendado_real = max(
                [int(pokemon.get("nivel") or 1) for pokemon in roster],
                default=nivel_recomendado_catalogo,
            )

            gyms.append(
                {
                    "id": int(row["id"]),
                    "codigo": row["codigo"],
                    "region_codigo": row["region_codigo"],
                    "region_nombre": row["region_nombre"],
                    "lider_nombre": row["lider_nombre"],
                    "lider_slug": row["lider_slug"],
                    "ciudad": row["ciudad"],
                    "medalla_nombre": row["medalla_nombre"],
                    "medalla_slug": row["medalla_slug"],
                    "tipo_principal": row["tipo_principal"],
                    "orden_region": int(row["orden_region"] or 0),
                    "nivel_recomendado": nivel_recomendado_real,
                    "nivel_recomendado_catalogo": nivel_recomendado_catalogo,
                    "reward_exp": int(row["reward_exp"] or 0),
                    "reward_pokedolares": int(row["reward_pokedolares"] or 0),
                    "trainer_asset_path": row.get("trainer_asset_path"),
                    "badge_asset_path": row.get("badge_asset_path"),
                    "descripcion": row.get("descripcion"),
                    "pista": row.get("pista"),
                    "fase": row.get("fase"),
                    "enemy_style": row.get("enemy_style"),
                    "desbloqueado": bool(row["desbloqueado"]),
                    "completado": bool(row["completado"]),
                    "intentos": int(row["intentos"] or 0),
                    "victorias": int(row["victorias"] or 0),
                    "mejor_turnos": int(row["mejor_turnos"] or 0) if row.get("mejor_turnos") is not None else None,
                    "gym_requerido_codigo": row.get("gym_requerido_codigo"),
                    "gym_requerido_lider": row.get("gym_requerido_lider"),
                    "roster": roster,
                    "cantidad_pokemon_rival": len(roster),
                }
            )

        conn.commit()
        return {"ok": True, "regiones": regiones, "gyms": gyms}
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo cargar el catálogo de Gyms")
    finally:
        cursor.close()
        release_connection(conn)


@router_gyms.get("/battle/gyms/progreso")
def obtener_progreso_gyms(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_gyms(cursor)
        catalogo_rows = obtener_catalogo_gyms_usuario(cursor, int(usuario["id"]))
        regiones = obtener_resumen_regiones(catalogo_rows)

        total = len(catalogo_rows)
        completados = sum(1 for row in catalogo_rows if bool(row["completado"]))
        desbloqueados = sum(1 for row in catalogo_rows if bool(row["desbloqueado"]))
        siguiente = next((row for row in catalogo_rows if bool(row["desbloqueado"]) and not bool(row["completado"])), None)

        conn.commit()
        return {
            "ok": True,
            "total_gyms": total,
            "completados": completados,
            "desbloqueados": desbloqueados,
            "porcentaje": int(round((completados / max(1, total)) * 100)),
            "regiones": regiones,
            "siguiente_gym": {
                "codigo": siguiente["codigo"],
                "lider_nombre": siguiente["lider_nombre"],
                "region_codigo": siguiente["region_codigo"],
                "medalla_nombre": siguiente["medalla_nombre"],
            } if siguiente else None,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo cargar el progreso de Gyms")
    finally:
        cursor.close()
        release_connection(conn)


@router_gyms.post("/battle/gyms/iniciar")
def iniciar_gym(payload: GymIniciarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_gyms(cursor)
        usuario_id = int(usuario["id"])
        asegurar_progreso_usuario_gyms(cursor, usuario_id)

        gym = obtener_gym_por_codigo(cursor, payload.gym_codigo)
        if not gym:
            raise HTTPException(status_code=404, detail="Gym no encontrado")

        cursor.execute(
            """
            SELECT *
            FROM gym_usuario_progreso
            WHERE usuario_id = %s AND gym_id = %s
            LIMIT 1
            FOR UPDATE
            """,
            (usuario_id, int(gym["id"])),
        )
        progreso = cursor.fetchone()
        if not progreso or not bool(progreso["desbloqueado"]):
            raise HTTPException(status_code=403, detail="Ese Gym todavía no está desbloqueado")

        if bool(progreso["completado"]):
            raise HTTPException(status_code=409, detail="Ese Gym ya fue completado")

        usuario_pokemon_ids = payload.usuario_pokemon_ids or []
        if not usuario_pokemon_ids:
            usuario_pokemon_ids = obtener_equipo_guardado_ids(cursor, usuario_id)

        if not usuario_pokemon_ids:
            raise HTTPException(status_code=400, detail="No tienes un equipo guardado para iniciar el Gym")

        ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)
        if payload.guardar_equipo:
            guardar_equipo_usuario(cursor, usuario_id, ids_limpios)

        snapshot_equipo_usuario = obtener_equipo_usuario_snapshot(cursor, usuario_id, ids_limpios)
        if not snapshot_equipo_usuario:
            raise HTTPException(status_code=400, detail="No se pudo construir el equipo del usuario para el Gym")

        snapshot_equipo_rival = obtener_equipo_rival_gym(cursor, int(gym["id"]))
        if not snapshot_equipo_rival:
            raise HTTPException(status_code=409, detail="El equipo rival de este Gym todavía no está configurado en base de datos")

        invalidar_sesiones_gym_activas(cursor, usuario_id)

        token = crear_token_simple(24)
        creado_en = ahora_server_naive()
        expira_en = creado_en + timedelta(seconds=GYM_SESSION_TTL_SEGUNDOS)

        cursor.execute(
            """
            INSERT INTO gym_sesiones (
                token,
                usuario_id,
                gym_id,
                equipo_usuario_pokemon_ids,
                snapshot_equipo_usuario_json,
                snapshot_equipo_rival_json,
                resultado_json,
                exp_ganada,
                pokedolares_ganados,
                badge_otorgada,
                activo,
                resultado,
                creado_en,
                expira_en
            )
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, '{}'::jsonb, %s, %s, FALSE, TRUE, 'pendiente', %s, %s)
            """,
            (
                token,
                usuario_id,
                int(gym["id"]),
                json.dumps(ids_limpios),
                json.dumps(snapshot_equipo_usuario),
                json.dumps(snapshot_equipo_rival),
                int(gym["reward_exp"] or 0),
                int(gym["reward_pokedolares"] or 0),
                creado_en,
                expira_en,
            ),
        )

        cursor.execute(
            """
            UPDATE gym_usuario_progreso
            SET intentos = intentos + 1,
                ultima_sesion_token = %s,
                actualizado_en = CURRENT_TIMESTAMP
            WHERE usuario_id = %s
              AND gym_id = %s
            """,
            (token, usuario_id, int(gym["id"])),
        )

        registrar_actividad_usuario(
            cursor,
            usuario_id,
            pagina="battle",
            accion="battle_start",
            detalle=f"gym:{gym['codigo']}",
            sesion_token=token,
            online=True,
        )

        conn.commit()
        return {
            "ok": True,
            "gym_session_token": token,
            "expira_en": expira_en.isoformat(),
            "ttl_segundos": GYM_SESSION_TTL_SEGUNDOS,
            "gym": {
                "id": int(gym["id"]),
                "codigo": gym["codigo"],
                "lider_nombre": gym["lider_nombre"],
                "medalla_nombre": gym["medalla_nombre"],
                "tipo_principal": gym["tipo_principal"],
                "region_codigo": gym["region_codigo"],
                "region_nombre": gym["region_nombre"],
                "trainer_asset_path": gym.get("trainer_asset_path"),
                "badge_asset_path": gym.get("badge_asset_path"),
                "reward_exp": int(gym["reward_exp"] or 0),
                "reward_pokedolares": int(gym["reward_pokedolares"] or 0),
            },
            "equipo_usuario": snapshot_equipo_usuario,
            "equipo_rival": snapshot_equipo_rival,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo iniciar el Gym")
    finally:
        cursor.close()
        release_connection(conn)


@router_gyms.post("/battle/gyms/recompensa")
def reclamar_recompensa_gym(payload: GymRecompensaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_gyms(cursor)
        usuario_id = int(usuario["id"])
        sesion = obtener_sesion_gym_por_token(cursor, payload.gym_session_token, usuario_id, for_update=True)
        if not sesion:
            raise HTTPException(status_code=404, detail="Sesión de Gym no encontrada")

        if sesion.get("reclamado_en") is not None:
            raise HTTPException(status_code=409, detail="La recompensa de este Gym ya fue reclamada")

        if sesion.get("cancelado_en") is not None:
            raise HTTPException(status_code=409, detail="La sesión del Gym ya fue cancelada")

        creado_en = sesion.get("creado_en")
        if creado_en and (datetime.now() - creado_en).total_seconds() < GYM_SESSION_MINIMA_SEGUNDOS:
            raise HTTPException(status_code=429, detail="La sesión del Gym todavía es demasiado reciente para cerrar")

        victoria = bool(payload.victoria)
        turnos = max(0, int(payload.turnos or 0))
        ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, normalizar_equipo_ids_json(sesion.get("equipo_usuario_pokemon_ids")))

        exp_ganada = 0
        pokedolares_ganados = 0
        badge_otorgada = False
        pokedolares_actuales = None
        pokemon_actualizados = []

        if victoria:
            multiplicadores = aplicar_multiplicadores_recompensa_batalla(
                cursor,
                usuario_id,
                int(sesion.get("gym_reward_exp") or sesion.get("exp_ganada") or 0),
                int(sesion.get("gym_reward_pokedolares") or sesion.get("pokedolares_ganados") or 0),
            )
            exp_ganada = int(multiplicadores["exp_final"])
            pokedolares_ganados = int(multiplicadores["pokedolares_final"])
            recompensa = aplicar_recompensas_gym(cursor, usuario_id, ids_limpios, exp_ganada, pokedolares_ganados)
            pokedolares_actuales = recompensa["pokedolares_actuales"]
            pokemon_actualizados = recompensa["pokemon_actualizados"]
            badge_otorgada = True

            cursor.execute(
                """
                UPDATE gym_usuario_progreso
                SET completado = TRUE,
                    desbloqueado = TRUE,
                    victorias = victorias + 1,
                    mejor_turnos = CASE
                        WHEN mejor_turnos IS NULL THEN %s
                        WHEN %s > 0 AND %s < mejor_turnos THEN %s
                        ELSE mejor_turnos
                    END,
                    completado_en = COALESCE(completado_en, NOW()),
                    actualizado_en = CURRENT_TIMESTAMP
                WHERE usuario_id = %s
                  AND gym_id = %s
                """,
                (turnos, turnos, turnos, turnos, usuario_id, int(sesion["gym_id"])),
            )
            asegurar_progreso_usuario_gyms(cursor, usuario_id)
        else:
            cursor.execute(
                """
                UPDATE gym_usuario_progreso
                SET actualizado_en = CURRENT_TIMESTAMP
                WHERE usuario_id = %s
                  AND gym_id = %s
                """,
                (usuario_id, int(sesion["gym_id"])),
            )

        resultado_codigo = "victoria" if victoria else "derrota"
        resultado_json = {
            "victoria": victoria,
            "turnos": turnos,
            "exp_ganada": exp_ganada,
            "pokedolares_ganados": pokedolares_ganados,
            "badge_otorgada": badge_otorgada,
        }

        cursor.execute(
            """
            UPDATE gym_sesiones
            SET activo = FALSE,
                resultado = %s,
                resultado_json = %s::jsonb,
                exp_ganada = %s,
                pokedolares_ganados = %s,
                badge_otorgada = %s,
                finalizado_en = NOW(),
                reclamado_en = NOW()
            WHERE id = %s
            """,
            (
                resultado_codigo,
                json.dumps(resultado_json),
                exp_ganada,
                pokedolares_ganados,
                badge_otorgada,
                int(sesion["id"]),
            ),
        )

        registrar_actividad_usuario(
            cursor,
            usuario_id,
            pagina="battle",
            accion="battle_end",
            detalle=f"gym:{sesion['gym_codigo']}:{resultado_codigo}",
            sesion_token=str(sesion["token"]),
            online=True,
        )

        conn.commit()
        return {
            "ok": True,
            "resultado": resultado_codigo,
            "victoria": victoria,
            "badge_otorgada": badge_otorgada,
            "exp_ganada": exp_ganada,
            "pokedolares_ganados": pokedolares_ganados,
            "pokedolares_actuales": pokedolares_actuales,
            "turnos": turnos,
            "pokemon_actualizados": pokemon_actualizados,
            "gym": {
                "codigo": sesion["gym_codigo"],
                "lider_nombre": sesion["lider_nombre"],
                "medalla_nombre": sesion["medalla_nombre"],
            },
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo cerrar el Gym")
    finally:
        cursor.close()
        release_connection(conn)


@router_gyms.post("/battle/gyms/cancelar")
def cancelar_sesion_gym(payload: GymCancelarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_gyms(cursor)
        usuario_id = int(usuario["id"])
        token = str(payload.gym_session_token or "").strip()

        if token:
            sesion = obtener_sesion_gym_por_token(cursor, token, usuario_id, for_update=True)
            if not sesion:
                raise HTTPException(status_code=404, detail="Sesión de Gym no encontrada")
            cursor.execute(
                """
                UPDATE gym_sesiones
                SET activo = FALSE,
                    resultado = CASE WHEN resultado = 'pendiente' THEN 'cancelada' ELSE resultado END,
                    cancelado_en = COALESCE(cancelado_en, NOW()),
                    finalizado_en = COALESCE(finalizado_en, NOW())
                WHERE id = %s
                """,
                (int(sesion["id"]),),
            )
        else:
            invalidar_sesiones_gym_activas(cursor, usuario_id)

        conn.commit()
        return {"ok": True, "mensaje": "Sesión de Gym cancelada"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo cancelar la sesión de Gym")
    finally:
        cursor.close()
        release_connection(conn)

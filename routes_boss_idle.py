import json
import random
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from monetization_utils import (
    aplicar_multiplicadores_recompensa_batalla,
    resolver_item_por_codigo,
    usuario_tiene_beneficio_activo,
)
from routes_pokemon import (
    MAX_NIVEL_POKEMON,
    calcular_stats,
    crear_token_simple,
    existe_tabla,
    normalizar_avatar_id,
    normalizar_equipo_ids_json,
    obtener_equipo_guardado_ids,
    obtener_select_bonus_renacer_especiales,
    obtener_select_stats_especiales,
    registrar_actividad_usuario,
    sincronizar_movimientos_usuario_pokemon,
    stats_especiales_habilitados,
    validar_ids_pokemon_equipo,
    guardar_equipo_usuario,
)

router_boss_idle = APIRouter()

SERVER_TIMEZONE = ZoneInfo("America/Lima")
BOSS_EVENT_HORA_INICIO = 20
BOSS_EVENT_DURACION_HORAS = 4
BOSS_SESSION_TTL_SEGUNDOS = 60 * 45

IDLE_DURACIONES_PERMITIDAS = {3600, 7200, 14400, 28800}

IDLE_TIER_CONFIG = {
    "ruta": {
        "tick_segundos": 45,
        "base_exp": 18,
        "base_pokedolares": 16,
        "enemy_power": 1.00,
        "drop_pool": [
            {"item_code": "potion", "chance": 0.12},
            {"item_code": "poke_ball", "chance": 0.08},
        ],
    },
    "elite": {
        "tick_segundos": 55,
        "base_exp": 48,
        "base_pokedolares": 40,
        "enemy_power": 1.25,
        "drop_pool": [
            {"item_code": "potion", "chance": 0.18},
            {"item_code": "super_ball", "chance": 0.10},
            {"item_code": "ultra_ball", "chance": 0.035},
        ],
    },
    "legend": {
        "tick_segundos": 65,
        "base_exp": 120,
        "base_pokedolares": 102,
        "enemy_power": 1.55,
        "drop_pool": [
            {"item_code": "potion", "chance": 0.22},
            {"item_code": "super_ball", "chance": 0.16},
            {"item_code": "ultra_ball", "chance": 0.08},
        ],
    },
    "masters": {
        "tick_segundos": 65,
        "base_exp": 240,
        "base_pokedolares": 203,
        "enemy_power": 1.55,
        "drop_pool": [
            {"item_code": "potion", "chance": 0.24},
            {"item_code": "super_ball", "chance": 0.18},
            {"item_code": "ultra_ball", "chance": 0.12},
            {"item_code": "master_ball", "chance": 0.0045},
        ],
    },
}


class BossIniciarPayload(BaseModel):
    usuario_pokemon_ids: list[int] | None = None
    guardar_equipo: bool = True


class BossRecompensaPayload(BaseModel):
    boss_session_token: str
    damage_total: int = 0
    boss_derrotado: bool = False
    turnos: int = 0


class IdleIniciarPayload(BaseModel):
    tier_codigo: str = "ruta"
    duracion_segundos: int = 3600
    usuario_pokemon_ids: list[int] | None = None
    guardar_equipo: bool = True


class IdleCancelarPayload(BaseModel):
    idle_session_token: str | None = None


def ahora_server():
    return datetime.now(SERVER_TIMEZONE)


def ahora_server_naive():
    return ahora_server().replace(tzinfo=None)


def serializar_datetime_lima(dt_value: datetime | None) -> str | None:
    if not dt_value:
        return None
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=SERVER_TIMEZONE).isoformat()
    return dt_value.astimezone(SERVER_TIMEZONE).isoformat()


def normalizar_datetime_idle_db(dt_value: datetime | None, referencia: datetime | None = None) -> datetime | None:
    if not dt_value:
        return None

    ahora_ref = referencia or ahora_server_naive()

    # Si la fecha viene "en el futuro" respecto a la hora Lima del servidor,
    # asumimos que fue guardada como UTC naive por PostgreSQL y la convertimos.
    if dt_value > (ahora_ref + timedelta(minutes=2)):
        return dt_value.replace(tzinfo=ZoneInfo("UTC")).astimezone(SERVER_TIMEZONE).replace(tzinfo=None)

    return dt_value


def normalizar_fechas_sesion_idle(cursor, sesion: dict) -> tuple[datetime | None, datetime | None, bool]:
    if not sesion:
        return None, None, False

    referencia = ahora_server_naive()
    iniciado_raw = sesion.get("iniciado_en")
    termina_raw = sesion.get("termina_en")
    duracion_segundos = int(sesion.get("duracion_segundos") or 0)

    iniciado = normalizar_datetime_idle_db(iniciado_raw, referencia)
    termina = normalizar_datetime_idle_db(termina_raw, referencia)

    if iniciado and duracion_segundos > 0:
        termina_esperada = iniciado + timedelta(seconds=duracion_segundos)
        if not termina or abs(int((termina - iniciado).total_seconds()) - duracion_segundos) > 1:
            termina = termina_esperada

    corregido = (iniciado != iniciado_raw) or (termina != termina_raw)

    if corregido and sesion.get("id"):
        cursor.execute(
            """
            UPDATE idle_sesiones
            SET iniciado_en = %s,
                termina_en = %s
            WHERE id = %s
            """,
            (iniciado, termina, int(sesion["id"]))
        )
        sesion["iniciado_en"] = iniciado
        sesion["termina_en"] = termina

    return iniciado, termina, corregido


def obtener_ventana_boss_para_fecha(fecha_obj: date):
    inicio = datetime(fecha_obj.year, fecha_obj.month, fecha_obj.day, BOSS_EVENT_HORA_INICIO, 0, 0, tzinfo=SERVER_TIMEZONE)
    fin = inicio + timedelta(hours=BOSS_EVENT_DURACION_HORAS)
    return inicio, fin


def estado_boss_para_fecha(fecha_obj: date):
    ahora = ahora_server()
    inicio, fin = obtener_ventana_boss_para_fecha(fecha_obj)
    if ahora < inicio:
        return "programado"
    if inicio <= ahora < fin:
        return "activo"
    return "cerrado"


def normalizar_tier_idle(valor: str | None) -> str:
    codigo = str(valor or "ruta").strip().lower()
    if codigo not in IDLE_TIER_CONFIG:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="tier_codigo no es válido")
    return codigo


def normalizar_duracion_idle(duracion_segundos: int | None) -> int:
    try:
        valor = int(duracion_segundos or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="duracion_segundos no es válida")

    if valor not in IDLE_DURACIONES_PERMITIDAS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="duracion_segundos no permitida")
    return valor


def asegurar_tablas_boss_idle(cursor):
    tablas = ("boss_catalogo", "boss_eventos", "boss_participaciones", "idle_sesiones")
    faltantes = [tabla for tabla in tablas if not existe_tabla(cursor, tabla)]
    if faltantes:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Faltan tablas para Boss/Idle: {', '.join(faltantes)}. Ejecuta la migración primero."
        )


def obtener_desbloqueos_idle_por_gyms(cursor, usuario_id: int) -> dict:
    gym_tables = ("gym_regiones", "gym_catalogo", "gym_usuario_progreso")
    if any(not existe_tabla(cursor, tabla) for tabla in gym_tables):
        return {
            "kanto_completo": False,
            "johto_completo": False,
            "hoenn_completo": False,
            "tres_regiones_completas": False,
        }

    cursor.execute(
        """
        SELECT
            LOWER(gr.codigo) AS region_codigo,
            COUNT(gc.id) AS total_gyms,
            SUM(CASE WHEN COALESCE(gup.completado, FALSE) = TRUE THEN 1 ELSE 0 END) AS completados
        FROM gym_regiones gr
        JOIN gym_catalogo gc
          ON gc.region_id = gr.id
         AND gc.activo = TRUE
        LEFT JOIN gym_usuario_progreso gup
          ON gup.gym_id = gc.id
         AND gup.usuario_id = %s
        WHERE gr.activo = TRUE
        GROUP BY LOWER(gr.codigo)
        """,
        (int(usuario_id),)
    )
    rows = cursor.fetchall() or []
    resumen = {
        str(row["region_codigo"]): {
            "total": int(row["total_gyms"] or 0),
            "completados": int(row["completados"] or 0),
        }
        for row in rows
    }

    def region_completa(codigo: str) -> bool:
        region = resumen.get(codigo, {})
        return bool(region.get("total", 0) > 0 and region.get("completados", 0) >= region.get("total", 0))

    kanto = region_completa("kanto")
    johto = region_completa("johto")
    hoenn = region_completa("hoenn")
    return {
        "kanto_completo": kanto,
        "johto_completo": johto,
        "hoenn_completo": hoenn,
        "tres_regiones_completas": kanto and johto and hoenn,
    }


def validar_requisitos_tier_idle(cursor, usuario_id: int, tier_codigo: str):
    tier = normalizar_tier_idle(tier_codigo)
    if tier == "ruta":
        return
    if tier == "masters":
        if not usuario_tiene_beneficio_activo(cursor, int(usuario_id), "idle_masters"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="El tier Masters requiere el beneficio Idle Masters activo"
            )
        return

    desbloqueos = obtener_desbloqueos_idle_por_gyms(cursor, int(usuario_id))
    if tier == "elite" and not desbloqueos["kanto_completo"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="El tier Elite requiere completar la ruta de Gyms de Kanto"
        )
    if tier == "legend" and not desbloqueos["tres_regiones_completas"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="El tier Legend requiere completar Kanto, Johto y Hoenn"
        )


def obtener_snapshot_equipo_para_modo(cursor, usuario_id: int, usuario_pokemon_ids: list[int]) -> list[dict]:
    ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)

    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
    up_has_special = stats_especiales_habilitados(cursor, "usuario_pokemon")

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
            up.hp_actual,
            up.hp_max,
            up.ataque,
            up.defensa,
            up.velocidad,
            {"up.ataque_especial AS ataque_especial, up.defensa_especial AS defensa_especial," if up_has_special else "up.ataque AS ataque_especial, up.defensa AS defensa_especial,"}
            up.es_shiny,
            up.renacimientos,
            up.puntos_renacer_disponibles,
            up.bonus_hp_renacer,
            up.bonus_ataque_renacer,
            up.bonus_defensa_renacer,
            up.bonus_velocidad_renacer,
            p.nombre,
            p.tipo,
            p.imagen,
            p.rareza,
            p.generacion,
            p.tiene_mega,
            p.hp AS base_hp,
            p.ataque AS base_ataque,
            p.defensa AS base_defensa,
            p.velocidad AS base_velocidad,
            {p_atk_sp_sql},
            {p_def_sp_sql}
        FROM usuario_pokemon up
        JOIN pokemon p ON p.id = up.pokemon_id
        WHERE up.usuario_id = %s
          AND up.id = ANY(%s)
        ORDER BY array_position(%s::int[], up.id)
        """,
        (usuario_id, ids_limpios, ids_limpios)
    )
    rows = cursor.fetchall()

    snapshot = []
    for row in rows:
        snapshot.append({
            "id": int(row["id"]),
            "usuario_id": int(row["usuario_id"]),
            "pokemon_id": int(row["pokemon_id"]),
            "nombre": row["nombre"],
            "tipo": row["tipo"],
            "imagen": row["imagen"],
            "rareza": row["rareza"],
            "generacion": row["generacion"],
            "tiene_mega": bool(row["tiene_mega"]),
            "nivel": int(row["nivel"] or 1),
            "experiencia": int(row["experiencia"] or 0),
            "experiencia_total": int(row["experiencia_total"] or 0),
            "victorias_total": int(row["victorias_total"] or 0),
            "hp_actual": int(row["hp_actual"] or 0),
            "hp_max": int(row["hp_max"] or 0),
            "ataque": int(row["ataque"] or 0),
            "defensa": int(row["defensa"] or 0),
            "velocidad": int(row["velocidad"] or 0),
            "ataque_especial": int(row.get("ataque_especial") or row["ataque"] or 0),
            "defensa_especial": int(row.get("defensa_especial") or row["defensa"] or 0),
            "es_shiny": bool(row["es_shiny"]),
            "renacimientos": int(row["renacimientos"] or 0),
            "puntos_renacer_disponibles": int(row["puntos_renacer_disponibles"] or 0),
            "bonus_hp_renacer": int(row["bonus_hp_renacer"] or 0),
            "bonus_ataque_renacer": int(row["bonus_ataque_renacer"] or 0),
            "bonus_defensa_renacer": int(row["bonus_defensa_renacer"] or 0),
            "bonus_velocidad_renacer": int(row["bonus_velocidad_renacer"] or 0),
            "base_hp": int(row["base_hp"] or 0),
            "base_ataque": int(row["base_ataque"] or 0),
            "base_defensa": int(row["base_defensa"] or 0),
            "base_velocidad": int(row["base_velocidad"] or 0),
            "base_ataque_especial": int(row.get("base_ataque_especial") or row["base_ataque"] or 0),
            "base_defensa_especial": int(row.get("base_defensa_especial") or row["base_defensa"] or 0),
        })

    if len(snapshot) != len(ids_limpios):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudo construir el snapshot del equipo")

    return snapshot


def obtener_boss_catalogo_activo(cursor) -> list[dict]:
    cursor.execute(
        """
        SELECT
            bc.id,
            bc.codigo,
            bc.pokemon_id,
            bc.nombre_visual,
            bc.descripcion,
            bc.nivel_base,
            bc.hp_multiplier,
            bc.atk_multiplier,
            bc.def_multiplier,
            bc.sp_atk_multiplier,
            bc.sp_def_multiplier,
            bc.speed_multiplier,
            bc.prioridad,
            bc.reward_json,
            p.nombre AS pokemon_nombre,
            p.tipo,
            p.imagen,
            p.hp,
            p.ataque,
            p.defensa,
            p.velocidad,
            COALESCE(p.ataque_especial, p.ataque) AS ataque_especial,
            COALESCE(p.defensa_especial, p.defensa) AS defensa_especial
        FROM boss_catalogo bc
        JOIN pokemon p ON p.id = bc.pokemon_id
        WHERE bc.activo = TRUE
        ORDER BY bc.prioridad ASC, bc.id ASC
        """
    )
    return cursor.fetchall()


def obtener_o_crear_evento_boss_hoy(cursor) -> dict:
    fecha_hoy = ahora_server().date()

    cursor.execute(
        """
        SELECT *
        FROM boss_eventos
        WHERE fecha_evento = %s
        LIMIT 1
        """,
        (fecha_hoy,)
    )
    existente = cursor.fetchone()
    if existente:
        return existente

    catalogo = obtener_boss_catalogo_activo(cursor)
    if not catalogo:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No existe un boss activo en boss_catalogo"
        )

    selector = fecha_hoy.toordinal() % len(catalogo)
    boss = catalogo[selector]
    inicio, fin = obtener_ventana_boss_para_fecha(fecha_hoy)
    estado = estado_boss_para_fecha(fecha_hoy)

    cursor.execute(
        """
        INSERT INTO boss_eventos
            (boss_catalogo_id, fecha_evento, estado, hora_inicio, hora_fin, seed, reward_json, creado_en, actualizado_en)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
        ON CONFLICT (fecha_evento) DO UPDATE SET
            actualizado_en = NOW()
        RETURNING *
        """,
        (
            int(boss["id"]),
            fecha_hoy,
            estado,
            inicio.replace(tzinfo=None),
            fin.replace(tzinfo=None),
            f"boss:{fecha_hoy.isoformat()}:{boss['codigo']}",
            json.dumps(boss.get("reward_json") or {}),
        )
    )
    return cursor.fetchone()


def obtener_boss_evento_detalle(cursor, boss_evento_id: int) -> dict | None:
    cursor.execute(
        """
        SELECT
            be.*,
            bc.codigo AS boss_codigo,
            bc.pokemon_id,
            bc.nombre_visual,
            bc.descripcion,
            bc.nivel_base,
            bc.hp_multiplier,
            bc.atk_multiplier,
            bc.def_multiplier,
            bc.sp_atk_multiplier,
            bc.sp_def_multiplier,
            bc.speed_multiplier,
            bc.reward_json AS reward_json_catalogo,
            p.nombre AS pokemon_nombre,
            p.tipo,
            p.imagen,
            p.hp,
            p.ataque,
            p.defensa,
            p.velocidad,
            COALESCE(p.ataque_especial, p.ataque) AS ataque_especial,
            COALESCE(p.defensa_especial, p.defensa) AS defensa_especial
        FROM boss_eventos be
        JOIN boss_catalogo bc ON bc.id = be.boss_catalogo_id
        JOIN pokemon p ON p.id = bc.pokemon_id
        WHERE be.id = %s
        LIMIT 1
        """,
        (boss_evento_id,)
    )
    return cursor.fetchone()


def construir_boss_desde_evento(evento_detalle: dict) -> dict:
    nivel = int(evento_detalle["nivel_base"] or 1)
    hp_total = max(1, int(round(int(evento_detalle["hp"] or 1) * float(evento_detalle["hp_multiplier"] or 10))))
    ataque = max(1, int(round(int(evento_detalle["ataque"] or 1) * float(evento_detalle["atk_multiplier"] or 1))))
    defensa = max(1, int(round(int(evento_detalle["defensa"] or 1) * float(evento_detalle["def_multiplier"] or 1))))
    velocidad = max(1, int(round(int(evento_detalle["velocidad"] or 1) * float(evento_detalle["speed_multiplier"] or 1))))
    ataque_especial = max(1, int(round(int(evento_detalle["ataque_especial"] or evento_detalle["ataque"] or 1) * float(evento_detalle["sp_atk_multiplier"] or 1))))
    defensa_especial = max(1, int(round(int(evento_detalle["defensa_especial"] or evento_detalle["defensa"] or 1) * float(evento_detalle["sp_def_multiplier"] or 1))))

    return {
        "codigo": evento_detalle["boss_codigo"],
        "pokemon_id": int(evento_detalle["pokemon_id"]),
        "nombre": evento_detalle["nombre_visual"] or evento_detalle["pokemon_nombre"],
        "nombre_base": evento_detalle["pokemon_nombre"],
        "descripcion": evento_detalle.get("descripcion"),
        "tipo": evento_detalle["tipo"],
        "imagen": evento_detalle["imagen"],
        "nivel": nivel,
        "hp_total": hp_total,
        "hp_actual": hp_total,
        "ataque": ataque,
        "defensa": defensa,
        "velocidad": velocidad,
        "ataque_especial": ataque_especial,
        "defensa_especial": defensa_especial,
    }


def calcular_recompensa_boss(evento_detalle: dict, damage_total: int, boss_derrotado: bool) -> dict:
    boss_data = construir_boss_desde_evento(evento_detalle)
    hp_total = int(boss_data["hp_total"])
    damage = max(0, min(int(damage_total or 0), hp_total))

    reward_catalogo = evento_detalle.get("reward_json_catalogo") or {}
    if isinstance(reward_catalogo, str):
        try:
            reward_catalogo = json.loads(reward_catalogo)
        except Exception:
            reward_catalogo = {}

    victory_exp = int(reward_catalogo.get("victory_exp") or 4500)
    victory_pokedolares = int(reward_catalogo.get("victory_pokedolares") or 9000)
    participation_exp = int(reward_catalogo.get("participation_exp") or 1200)
    participation_pokedolares = int(reward_catalogo.get("participation_pokedolares") or 1800)

    porcentaje = (damage / hp_total) if hp_total > 0 else 0.0
    tier = "bronze"
    factor = 1.0

    if porcentaje >= 1 or boss_derrotado:
        tier = "alpha"
        factor = 1.0
    elif porcentaje >= 0.75:
        tier = "diamond"
        factor = 0.85
    elif porcentaje >= 0.50:
        tier = "gold"
        factor = 0.65
    elif porcentaje >= 0.25:
        tier = "silver"
        factor = 0.45
    else:
        tier = "bronze"
        factor = 0.25

    if boss_derrotado or damage >= hp_total:
        exp = victory_exp
        pokedolares = victory_pokedolares
        tier = "alpha"
    else:
        exp = int(round(participation_exp + (victory_exp - participation_exp) * factor))
        pokedolares = int(round(participation_pokedolares + (victory_pokedolares - participation_pokedolares) * factor))

    return {
        "tier": tier,
        "exp": max(0, exp),
        "pokedolares": max(0, pokedolares),
        "damage_total": damage,
        "boss_hp_total": hp_total,
        "boss_derrotado": bool(boss_derrotado or damage >= hp_total),
    }


def sumar_pokedolares_usuario(cursor, usuario_id: int, monto: int) -> int:
    monto = max(0, int(monto or 0))
    cursor.execute(
        """
        SELECT id, pokedolares
        FROM usuarios
        WHERE id = %s
        FOR UPDATE
        """,
        (usuario_id,)
    )
    usuario_db = cursor.fetchone()
    if not usuario_db:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

    total = int(usuario_db["pokedolares"] or 0) + monto
    cursor.execute(
        """
        UPDATE usuarios
        SET pokedolares = %s
        WHERE id = %s
        """,
        (total, usuario_id)
    )
    return total


def aplicar_exp_a_equipo(cursor, usuario_id: int, usuario_pokemon_ids: list[int], exp_ganada: int, sumar_victoria: bool = False):
    ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)
    exp_ganada = max(0, int(exp_ganada or 0))

    p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
    bonus_atk_sp_sql, bonus_def_sp_sql = obtener_select_bonus_renacer_especiales(cursor, "up")
    up_has_special = stats_especiales_habilitados(cursor, "usuario_pokemon")
    resultado = []

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
            (usuario_pokemon_id, usuario_id)
        )
        poke = cursor.fetchone()
        if not poke:
            continue

        nivel_actual = int(poke["nivel"] or 1)
        exp_actual = int(poke["experiencia"] or 0)
        exp_total_actual = int(poke["experiencia_total"] or 0)
        victorias_actuales = int(poke["victorias_total"] or 0)

        nuevo_exp_total = exp_total_actual + exp_ganada
        nuevas_victorias = victorias_actuales + (1 if sumar_victoria else 0)

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
                )
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
                )
            )

        sincronizar_movimientos_usuario_pokemon(
            cursor,
            usuario_pokemon_id,
            int(poke["pokemon_id"]),
            nuevo_nivel,
            origen="nivel",
            auto_equipar=True,
        )

        resultado.append({
            "usuario_pokemon_id": int(usuario_pokemon_id),
            "nivel": int(nuevo_nivel),
            "experiencia": int(nueva_exp),
            "experiencia_total": int(nuevo_exp_total),
            "victorias_total": int(nuevas_victorias),
            "hp_max": int(stats["hp_max"]),
            "ataque": int(stats["ataque"]),
            "defensa": int(stats["defensa"]),
            "velocidad": int(stats["velocidad"]),
            "ataque_especial": int(stats["ataque_especial"]),
            "defensa_especial": int(stats["defensa_especial"]),
        })

    return resultado


def obtener_sesion_idle_activa(cursor, usuario_id: int, for_update: bool = False):
    sql_for_update = " FOR UPDATE" if for_update else ""
    cursor.execute(
        f"""
        SELECT *
        FROM idle_sesiones
        WHERE usuario_id = %s
          AND estado IN ('activa', 'reclamable')
        ORDER BY id DESC
        LIMIT 1
        {sql_for_update}
        """,
        (usuario_id,)
    )
    return cursor.fetchone()


def obtener_participacion_boss_por_token(cursor, token: str, usuario_id: int, for_update: bool = False):
    sql_for_update = " FOR UPDATE" if for_update else ""
    cursor.execute(
        f"""
        SELECT *
        FROM boss_participaciones
        WHERE token = %s
          AND usuario_id = %s
          AND activo = TRUE
          AND reclamado_en IS NULL
          AND expira_en > NOW()
        LIMIT 1
        {sql_for_update}
        """,
        (token, usuario_id)
    )
    return cursor.fetchone()


def simular_idle_resultado(cursor, session_row: dict) -> dict:
    tier = normalizar_tier_idle(session_row.get("tier_codigo"))
    cfg = IDLE_TIER_CONFIG[tier]
    snapshot = session_row.get("snapshot_equipo_json") or []
    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)

    equipo = list(snapshot or [])
    if not equipo:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La sesión Idle no tiene snapshot de equipo")

    duracion_segundos = int(session_row["duracion_segundos"] or 0)
    tick_segundos = int(cfg["tick_segundos"])
    ticks = max(1, duracion_segundos // tick_segundos)

    avg_nivel = sum(int(p.get("nivel") or 1) for p in equipo) / len(equipo)
    avg_hp = sum(int(p.get("hp_max") or p.get("hp_actual") or 1) for p in equipo) / len(equipo)
    avg_atk = sum(int(p.get("ataque") or 1) for p in equipo) / len(equipo)
    avg_def = sum(int(p.get("defensa") or 1) for p in equipo) / len(equipo)
    avg_spatk = sum(int(p.get("ataque_especial") or p.get("ataque") or 1) for p in equipo) / len(equipo)
    avg_spdef = sum(int(p.get("defensa_especial") or p.get("defensa") or 1) for p in equipo) / len(equipo)

    power_team = (avg_nivel * 1.8) + (avg_hp * 0.10) + (avg_atk * 0.15) + (avg_def * 0.12) + (avg_spatk * 0.14) + (avg_spdef * 0.11)
    power_enemy = max(1.0, power_team * float(cfg["enemy_power"]))

    ratio = power_team / power_enemy if power_enemy > 0 else 1.0
    success_rate = max(0.55, min(0.95, 0.70 + ((ratio - 1.0) * 0.25)))

    victorias = int(round(ticks * success_rate))
    derrotas = max(0, ticks - victorias)

    exp_ganada = int(victorias * int(cfg["base_exp"]))
    pokedolares_ganados = int(victorias * int(cfg["base_pokedolares"]))

    rng = random.Random(f"idle:{session_row['token']}:{tier}:{duracion_segundos}")
    items = []

    for drop in cfg.get("drop_pool", []):
        cantidad = 0
        chance = float(drop.get("chance") or 0)
        item_code = str(drop.get("item_code") or "").strip()
        item_id = int(drop.get("item_id") or 0)
        if item_id <= 0 and item_code:
            item_row = resolver_item_por_codigo(cursor, item_code)
            if item_row:
                item_id = int(item_row["id"])
        if item_id <= 0 or chance <= 0:
            continue

        for _ in range(ticks):
            if rng.random() <= chance:
                cantidad += 1

        if cantidad > 0:
            items.append({
                "item_id": item_id,
                "item_code": item_code or None,
                "cantidad": cantidad
            })

    return {
        "tier_codigo": tier,
        "tick_segundos": tick_segundos,
        "ticks": int(ticks),
        "success_rate": round(success_rate, 4),
        "victorias_estimadas": int(victorias),
        "derrotas_estimadas": int(derrotas),
        "exp_ganada": int(exp_ganada),
        "pokedolares_ganados": int(pokedolares_ganados),
        "items_ganados": items,
        "resumen": {
            "avg_nivel": round(avg_nivel, 2),
            "avg_hp": round(avg_hp, 2),
            "avg_atk": round(avg_atk, 2),
            "avg_def": round(avg_def, 2),
            "avg_spatk": round(avg_spatk, 2),
            "avg_spdef": round(avg_spdef, 2),
        }
    }


def agregar_items_usuario(cursor, usuario_id: int, items_ganados: list[dict]):
    for item in items_ganados or []:
        item_id = int(item.get("item_id") or 0)
        cantidad = int(item.get("cantidad") or 0)
        if item_id <= 0 or cantidad <= 0:
            continue

        cursor.execute(
            """
            INSERT INTO usuario_items (usuario_id, item_id, cantidad)
            VALUES (%s, %s, %s)
            ON CONFLICT (usuario_id, item_id)
            DO UPDATE SET cantidad = usuario_items.cantidad + EXCLUDED.cantidad
            """,
            (usuario_id, item_id, cantidad)
        )


@router_boss_idle.get("/battle/boss/estado")
def obtener_estado_boss(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        evento = obtener_o_crear_evento_boss_hoy(cursor)
        estado_real = estado_boss_para_fecha(evento["fecha_evento"])

        if evento.get("estado") != estado_real:
            cursor.execute(
                """
                UPDATE boss_eventos
                SET estado = %s, actualizado_en = NOW()
                WHERE id = %s
                """,
                (estado_real, int(evento["id"]))
            )
            conn.commit()
            evento["estado"] = estado_real

        detalle = obtener_boss_evento_detalle(cursor, int(evento["id"]))
        boss = construir_boss_desde_evento(detalle)

        ahora = ahora_server()
        inicio, fin = obtener_ventana_boss_para_fecha(evento["fecha_evento"])
        segundos_para_inicio = max(0, int((inicio - ahora).total_seconds()))
        segundos_para_fin = max(0, int((fin - ahora).total_seconds()))

        cursor.execute(
            """
            SELECT id, token, resultado, damage_total, boss_hp_total, boss_derrotado, creado_en, finalizado_en, reclamado_en
            FROM boss_participaciones
            WHERE boss_evento_id = %s
              AND usuario_id = %s
            LIMIT 1
            """,
            (int(evento["id"]), int(usuario["id"]))
        )
        participacion = cursor.fetchone()

        return {
            "ok": True,
            "server_timezone": "America/Lima",
            "hora_server": ahora.isoformat(),
            "fecha_evento": evento["fecha_evento"].isoformat(),
            "estado": estado_real,
            "activo": estado_real == "activo",
            "hora_inicio": inicio.isoformat(),
            "hora_fin": fin.isoformat(),
            "segundos_para_inicio": segundos_para_inicio,
            "segundos_para_fin": segundos_para_fin,
            "boss": boss,
            "participacion": {
                "id": int(participacion["id"]),
                "token": participacion["token"],
                "resultado": participacion["resultado"],
                "damage_total": int(participacion["damage_total"] or 0),
                "boss_hp_total": int(participacion["boss_hp_total"] or boss["hp_total"]),
                "boss_derrotado": bool(participacion["boss_derrotado"]),
                "creado_en": participacion["creado_en"].isoformat() if participacion.get("creado_en") else None,
                "finalizado_en": participacion["finalizado_en"].isoformat() if participacion.get("finalizado_en") else None,
                "reclamado_en": participacion["reclamado_en"].isoformat() if participacion.get("reclamado_en") else None,
            } if participacion else None
        }
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.post("/battle/boss/iniciar")
def iniciar_boss_diario(payload: BossIniciarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        evento = obtener_o_crear_evento_boss_hoy(cursor)
        estado_real = estado_boss_para_fecha(evento["fecha_evento"])

        if estado_real != "activo":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="El Boss del mundo no está activo en este momento"
            )

        detalle = obtener_boss_evento_detalle(cursor, int(evento["id"]))
        boss = construir_boss_desde_evento(detalle)

        usuario_pokemon_ids = payload.usuario_pokemon_ids or []
        if usuario_pokemon_ids:
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], usuario_pokemon_ids)
            if payload.guardar_equipo:
                guardar_equipo_usuario(cursor, usuario["id"], ids_limpios)
        else:
            ids_limpios = obtener_equipo_guardado_ids(cursor, usuario["id"])
            if not ids_limpios:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No tienes un equipo guardado para Boss")
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], ids_limpios)

        if len(ids_limpios) != 6:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El modo Boss requiere exactamente 6 Pokémon")

        snapshot = obtener_snapshot_equipo_para_modo(cursor, usuario["id"], ids_limpios)

        cursor.execute(
            """
            SELECT *
            FROM boss_participaciones
            WHERE boss_evento_id = %s
              AND usuario_id = %s
            LIMIT 1
            FOR UPDATE
            """,
            (int(evento["id"]), int(usuario["id"]))
        )
        existente = cursor.fetchone()

        if existente and existente.get("reclamado_en") is not None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya reclamaste tu participación del Boss de hoy")

        boss_token = existente["token"] if existente else crear_token_simple(24)

        if existente:
            cursor.execute(
                """
                UPDATE boss_participaciones
                SET token = %s,
                    equipo_usuario_pokemon_ids = %s::jsonb,
                    snapshot_equipo_json = %s::jsonb,
                    boss_hp_total = %s,
                    activo = TRUE,
                    resultado = 'pendiente',
                    damage_total = 0,
                    boss_derrotado = FALSE,
                    turnos = 0,
                    exp_ganada = 0,
                    pokedolares_ganados = 0,
                    reward_json = '{}'::jsonb,
                    creado_en = NOW(),
                    expira_en = NOW() + (%s * INTERVAL '1 second'),
                    finalizado_en = NULL,
                    cancelado_en = NULL
                WHERE id = %s
                """,
                (
                    boss_token,
                    json.dumps(ids_limpios),
                    json.dumps(snapshot),
                    int(boss["hp_total"]),
                    int(BOSS_SESSION_TTL_SEGUNDOS),
                    int(existente["id"]),
                )
            )
        else:
            cursor.execute(
                """
                INSERT INTO boss_participaciones
                    (boss_evento_id, usuario_id, token, equipo_usuario_pokemon_ids, snapshot_equipo_json,
                     resultado, damage_total, boss_hp_total, boss_derrotado, turnos, exp_ganada, pokedolares_ganados,
                     reward_json, activo, creado_en, expira_en)
                VALUES
                    (%s, %s, %s, %s::jsonb, %s::jsonb, 'pendiente', 0, %s, FALSE, 0, 0, 0, '{}'::jsonb, TRUE, NOW(), NOW() + (%s * INTERVAL '1 second'))
                """,
                (
                    int(evento["id"]),
                    int(usuario["id"]),
                    boss_token,
                    json.dumps(ids_limpios),
                    json.dumps(snapshot),
                    int(boss["hp_total"]),
                    int(BOSS_SESSION_TTL_SEGUNDOS),
                )
            )

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle-arena",
            accion="battle_start",
            detalle="modo:boss"
        )
        conn.commit()

        return {
            "ok": True,
            "modo": "boss",
            "boss_session_token": boss_token,
            "ttl_segundos": BOSS_SESSION_TTL_SEGUNDOS,
            "equipo_usuario_pokemon_ids": ids_limpios,
            "snapshot_equipo": snapshot,
            "boss": boss,
            "boss_evento_id": int(evento["id"]),
            "fecha_evento": evento["fecha_evento"].isoformat(),
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/boss/iniciar:", repr(error))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo iniciar el Boss")
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.post("/battle/boss/recompensa")
def reclamar_recompensa_boss(payload: BossRecompensaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        participacion = obtener_participacion_boss_por_token(cursor, payload.boss_session_token, usuario["id"], for_update=True)
        if not participacion:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="La sesión del Boss no existe, expiró o ya no está activa")

        if participacion.get("reclamado_en") is not None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La recompensa del Boss ya fue reclamada")

        detalle = obtener_boss_evento_detalle(cursor, int(participacion["boss_evento_id"]))
        recompensa = calcular_recompensa_boss(detalle, payload.damage_total, payload.boss_derrotado)
        multiplicadores = aplicar_multiplicadores_recompensa_batalla(
            cursor,
            int(usuario["id"]),
            int(recompensa["exp"]),
            int(recompensa["pokedolares"]),
        )
        recompensa["exp_base"] = int(recompensa["exp"])
        recompensa["pokedolares_base"] = int(recompensa["pokedolares"])
        recompensa["exp"] = int(multiplicadores["exp_final"])
        recompensa["pokedolares"] = int(multiplicadores["pokedolares_final"])
        recompensa["beneficios_activos"] = multiplicadores["beneficios_activos"]

        ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], normalizar_equipo_ids_json(participacion.get("equipo_usuario_pokemon_ids")))
        nuevos_pokedolares = sumar_pokedolares_usuario(cursor, usuario["id"], recompensa["pokedolares"])
        pokemon_actualizados = aplicar_exp_a_equipo(
            cursor,
            usuario["id"],
            ids_limpios,
            recompensa["exp"],
            sumar_victoria=bool(recompensa["boss_derrotado"]),
        )

        cursor.execute(
            """
            UPDATE boss_participaciones
            SET damage_total = %s,
                boss_hp_total = %s,
                boss_derrotado = %s,
                turnos = %s,
                exp_ganada = %s,
                pokedolares_ganados = %s,
                reward_json = %s::jsonb,
                activo = FALSE,
                resultado = %s,
                finalizado_en = COALESCE(finalizado_en, NOW()),
                reclamado_en = NOW()
            WHERE id = %s
            """,
            (
                int(recompensa["damage_total"]),
                int(recompensa["boss_hp_total"]),
                bool(recompensa["boss_derrotado"]),
                max(0, int(payload.turnos or 0)),
                int(recompensa["exp"]),
                int(recompensa["pokedolares"]),
                json.dumps(recompensa),
                "reclamada",
                int(participacion["id"]),
            )
        )

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle-arena",
            accion="battle_end",
            detalle=f"modo:boss tier:{recompensa['tier']}"
        )
        conn.commit()

        return {
            "ok": True,
            "modo": "boss",
            "boss_session_token": payload.boss_session_token,
            "reward_tier": recompensa["tier"],
            "damage_total": int(recompensa["damage_total"]),
            "boss_hp_total": int(recompensa["boss_hp_total"]),
            "boss_derrotado": bool(recompensa["boss_derrotado"]),
            "exp_ganada": int(recompensa["exp"]),
            "pokedolares_ganados": int(recompensa["pokedolares"]),
            "pokedolares_actuales": int(nuevos_pokedolares),
            "pokemon_actualizados": pokemon_actualizados,
            "exp_base": int(recompensa.get("exp_base") or recompensa["exp"]),
            "pokedolares_base": int(recompensa.get("pokedolares_base") or recompensa["pokedolares"]),
            "beneficios_activos": recompensa.get("beneficios_activos") or [],
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/boss/recompensa:", repr(error))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo reclamar la recompensa del Boss")
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.get("/battle/boss/ranking")
def obtener_ranking_boss(limit: int = 20, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        evento = obtener_o_crear_evento_boss_hoy(cursor)
        limit_seguro = max(1, min(100, int(limit or 20)))

        cursor.execute(
            """
            SELECT
                bp.usuario_id,
                u.nombre,
                u.foto,
                u.avatar_id,
                bp.damage_total,
                bp.boss_hp_total,
                bp.boss_derrotado,
                bp.turnos,
                bp.finalizado_en
            FROM boss_participaciones bp
            JOIN usuarios u ON u.id = bp.usuario_id
            WHERE bp.boss_evento_id = %s
            ORDER BY bp.damage_total DESC, bp.boss_derrotado DESC, bp.finalizado_en ASC NULLS LAST, bp.id ASC
            LIMIT %s
            """,
            (int(evento["id"]), limit_seguro)
        )
        rows = cursor.fetchall()

        ranking = []
        for idx, row in enumerate(rows, start=1):
            ranking.append({
                "puesto": idx,
                "usuario_id": int(row["usuario_id"]),
                "nombre": row["nombre"],
                "foto": row["foto"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "damage_total": int(row["damage_total"] or 0),
                "boss_hp_total": int(row["boss_hp_total"] or 0),
                "boss_derrotado": bool(row["boss_derrotado"]),
                "turnos": int(row["turnos"] or 0),
                "finalizado_en": row["finalizado_en"].isoformat() if row.get("finalizado_en") else None,
            })

        return {
            "ok": True,
            "fecha_evento": evento["fecha_evento"].isoformat(),
            "ranking": ranking
        }
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.get("/battle/idle/estado")
def obtener_estado_idle(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        sesion = obtener_sesion_idle_activa(cursor, usuario["id"], for_update=False)

        if not sesion:
            return {
                "ok": True,
                "activa": False,
                "server_timezone": "America/Lima",
                "hora_server": ahora_server().isoformat(),
                "sesion": None
            }

        ahora = ahora_server_naive()
        iniciada_en, termina_en, fechas_corregidas = normalizar_fechas_sesion_idle(cursor, sesion)

        total = int(sesion.get("duracion_segundos") or 0)
        if total <= 0 and iniciada_en and termina_en:
            total = max(1, int((termina_en - iniciada_en).total_seconds()))
        total = max(1, total)

        transcurrido = 0
        if iniciada_en:
            transcurrido = max(0, min(total, int((ahora - iniciada_en).total_seconds())))

        restante = max(0, total - transcurrido)
        progreso = int(round((transcurrido / total) * 100))

        if sesion["estado"] == "activa" and restante <= 0:
            cursor.execute(
                """
                UPDATE idle_sesiones
                SET estado = 'reclamable'
                WHERE id = %s
                """,
                (int(sesion["id"]),)
            )
            sesion["estado"] = "reclamable"
            fechas_corregidas = True

        if fechas_corregidas:
            conn.commit()

        return {
            "ok": True,
            "activa": sesion["estado"] in ("activa", "reclamable"),
            "server_timezone": "America/Lima",
            "hora_server": ahora_server().isoformat(),
            "sesion": {
                "token": sesion["token"],
                "tier_codigo": sesion["tier_codigo"],
                "duracion_segundos": int(sesion["duracion_segundos"]),
                "estado": sesion["estado"],
                "iniciado_en": serializar_datetime_lima(iniciada_en),
                "termina_en": serializar_datetime_lima(termina_en),
                "segundos_restantes": restante,
                "progreso_pct": progreso
            }
        }
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.post("/battle/idle/iniciar")
def iniciar_idle(payload: IdleIniciarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)

        tier_codigo = normalizar_tier_idle(payload.tier_codigo)
        duracion_segundos = normalizar_duracion_idle(payload.duracion_segundos)

        validar_requisitos_tier_idle(cursor, int(usuario["id"]), tier_codigo)

        sesion_activa = obtener_sesion_idle_activa(cursor, usuario["id"], for_update=True)
        if sesion_activa:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya tienes una sesión Idle activa")

        usuario_pokemon_ids = payload.usuario_pokemon_ids or []
        if usuario_pokemon_ids:
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], usuario_pokemon_ids)
            if payload.guardar_equipo:
                guardar_equipo_usuario(cursor, usuario["id"], ids_limpios)
        else:
            ids_limpios = obtener_equipo_guardado_ids(cursor, usuario["id"])
            if not ids_limpios:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No tienes un equipo guardado para Idle")
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], ids_limpios)

        if len(ids_limpios) != 6:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El modo Idle requiere exactamente 6 Pokémon")

        snapshot = obtener_snapshot_equipo_para_modo(cursor, usuario["id"], ids_limpios)
        idle_token = crear_token_simple(24)
        iniciado_en = ahora_server_naive()
        termina_en = iniciado_en + timedelta(seconds=duracion_segundos)

        cursor.execute(
            """
            INSERT INTO idle_sesiones
                (token, usuario_id, equipo_usuario_pokemon_ids, snapshot_equipo_json, tier_codigo, duracion_segundos,
                 estado, iniciado_en, termina_en, items_ganados_json)
            VALUES
                (%s, %s, %s::jsonb, %s::jsonb, %s, %s, 'activa', %s, %s, '[]'::jsonb)
            """,
            (
                idle_token,
                int(usuario["id"]),
                json.dumps(ids_limpios),
                json.dumps(snapshot),
                tier_codigo,
                duracion_segundos,
                iniciado_en,
                termina_en,
            )
        )

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle",
            accion="battle_start",
            detalle=f"modo:idle tier:{tier_codigo}"
        )
        conn.commit()

        return {
            "ok": True,
            "modo": "idle",
            "idle_session_token": idle_token,
            "tier_codigo": tier_codigo,
            "duracion_segundos": duracion_segundos,
            "iniciado_en": serializar_datetime_lima(iniciado_en),
            "termina_en": serializar_datetime_lima(termina_en),
            "equipo_usuario_pokemon_ids": ids_limpios,
            "snapshot_equipo": snapshot
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/idle/iniciar:", repr(error))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo iniciar el modo Idle")
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.post("/battle/idle/reclamar")
def reclamar_idle(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        sesion = obtener_sesion_idle_activa(cursor, usuario["id"], for_update=True)
        if not sesion:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No tienes una sesión Idle activa")

        ahora = ahora_server_naive()
        _, termina_en, _ = normalizar_fechas_sesion_idle(cursor, sesion)
        if termina_en and ahora < termina_en:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La sesión Idle aún no termina")

        resultado = sesion.get("resultado_json")
        if isinstance(resultado, str):
            resultado = json.loads(resultado)

        if not resultado:
            resultado = simular_idle_resultado(cursor, sesion)

        ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], normalizar_equipo_ids_json(sesion.get("equipo_usuario_pokemon_ids")))
        nuevos_pokedolares = sumar_pokedolares_usuario(cursor, usuario["id"], int(resultado["pokedolares_ganados"]))
        pokemon_actualizados = aplicar_exp_a_equipo(
            cursor,
            usuario["id"],
            ids_limpios,
            int(resultado["exp_ganada"]),
            sumar_victoria=False,
        )
        agregar_items_usuario(cursor, usuario["id"], resultado.get("items_ganados") or [])

        cursor.execute(
            """
            UPDATE idle_sesiones
            SET estado = 'reclamada',
                resultado_json = %s::jsonb,
                ticks_procesados = %s,
                victorias_estimadas = %s,
                derrotas_estimadas = %s,
                exp_ganada = %s,
                pokedolares_ganados = %s,
                items_ganados_json = %s::jsonb,
                reclamado_en = NOW()
            WHERE id = %s
            """,
            (
                json.dumps(resultado),
                int(resultado["ticks"]),
                int(resultado["victorias_estimadas"]),
                int(resultado["derrotas_estimadas"]),
                int(resultado["exp_ganada"]),
                int(resultado["pokedolares_ganados"]),
                json.dumps(resultado.get("items_ganados") or []),
                int(sesion["id"]),
            )
        )

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle",
            accion="battle_end",
            detalle=f"modo:idle tier:{sesion['tier_codigo']}"
        )
        conn.commit()

        return {
            "ok": True,
            "modo": "idle",
            "resultado": resultado,
            "pokedolares_actuales": int(nuevos_pokedolares),
            "pokemon_actualizados": pokemon_actualizados
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/idle/reclamar:", repr(error))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo reclamar el modo Idle")
    finally:
        cursor.close()
        release_connection(conn)


@router_boss_idle.post("/battle/idle/cancelar")
def cancelar_idle(payload: IdleCancelarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_boss_idle(cursor)
        sesion = obtener_sesion_idle_activa(cursor, usuario["id"], for_update=True)
        if not sesion:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No tienes una sesión Idle activa")

        if payload.idle_session_token and payload.idle_session_token != sesion["token"]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="La sesión Idle no pertenece al usuario")

        cursor.execute(
            """
            UPDATE idle_sesiones
            SET estado = 'cancelada',
                cancelado_en = NOW()
            WHERE id = %s
            """,
            (int(sesion["id"]),)
        )
        conn.commit()

        return {
            "ok": True,
            "modo": "idle",
            "mensaje": "La sesión Idle fue cancelada",
            "idle_session_token": sesion["token"]
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/idle/cancelar:", repr(error))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo cancelar el modo Idle")
    finally:
        cursor.close()
        release_connection(conn)

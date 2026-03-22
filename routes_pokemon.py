import asyncio
import json
import re
import random
import secrets
import time
from collections import deque
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel

from database import get_connection, get_cursor, release_connection
from auth import verify_google_token, create_access_token, get_current_user, get_current_user_from_token

router = APIRouter()
MAX_NIVEL_POKEMON = 200
AVATAR_ID_DEFAULT = "steven"
AVATAR_ID_REGEX = re.compile(r"^[a-z0-9_-]{1,60}$")
MAPS_NODE_ID_REGEX = re.compile(r"^[a-z0-9_-]{1,60}$")
MAPS_PRESENCIA_TTL_SEGUNDOS = 25
USUARIO_ACTIVIDAD_TTL_SEGUNDOS = 120
MAPS_WEBSOCKET_CONEXIONES = {}
MAPS_WEBSOCKET_LOCK = asyncio.Lock()
ENCUENTRO_TOKEN_TTL_SEGUNDOS = 60 * 5
BATTLE_SESSION_TTL_SEGUNDOS = 60 * 20
BATTLE_SESSION_MINIMA_SEGUNDOS = 8
BATTLE_RECOMPENSA_COOLDOWN_SEGUNDOS = 12
BATTLE_RECOMPENSA_ULTIMO_USO = {}
RECOMPENSAS_ARENA_PERMITIDAS = {
    (2500, 2500): {"codigo": "normal", "exp": 2500, "pokedolares": 2500, "bonus_nivel_rival": 0},
    (3500, 5000): {"codigo": "challenge", "exp": 3500, "pokedolares": 5000, "bonus_nivel_rival": 5},
    (4500, 10000): {"codigo": "expert", "exp": 4500, "pokedolares": 10000, "bonus_nivel_rival": 10},
    (6000, 15000): {"codigo": "master", "exp": 6000, "pokedolares": 15000, "bonus_nivel_rival": 15},
}
RECOMPENSAS_ARENA_POR_CODIGO = {
    valor["codigo"]: dict(valor)
    for valor in RECOMPENSAS_ARENA_PERMITIDAS.values()
}
ACTIVIDAD_PAGINAS_VALIDAS = {
    "index", "battle", "battle-arena", "maps", "mypokemon", "pokemart", "ranking", "login", "global"
}
ACTIVIDAD_ACCIONES_VALIDAS = {
    "heartbeat", "login", "logout", "view", "move", "battle_start", "battle_end", "capture", "shop", "evolution", "team_save"
}
MAPS_RATE_LIMIT_CONFIG = {
    "encuentro": {
        "window_seconds": 10,
        "max_requests": 60,
        "min_interval_ms": 40,
    },
    "intentar-captura": {
        "window_seconds": 10,
        "max_requests": 10,
        "min_interval_ms": 250,
    }
}

MAPS_RATE_LIMIT_MEMORIA = {
    endpoint: {}
    for endpoint in MAPS_RATE_LIMIT_CONFIG.keys()
}

TABLAS_OPERATIVAS_CLEANUP_CADA_SEGUNDOS = 300
TABLAS_OPERATIVAS_ULTIMO_CLEANUP = 0

CAPTURE_DEBUG_MARKER = "capture-debug-2026-03-21-v1"

# =========================================================
# PAYLOADS
# =========================================================

class GoogleLoginPayload(BaseModel):
    credential: str


class CapturaPayload(BaseModel):
    usuario_id: int | None = None
    pokemon_id: int
    nivel: int = 1
    es_shiny: bool = False


class EncuentroPayload(BaseModel):
    usuario_id: int | None = None
    zona_id: int


class IntentoCapturaPayload(BaseModel):
    usuario_id: int | None = None
    pokemon_id: int
    nivel: int
    es_shiny: bool = False
    hp_actual: int = 100
    hp_maximo: int = 100
    item_id: int
    encuentro_token: str | None = None


class ExpPayload(BaseModel):
    usuario_pokemon_id: int
    exp_ganada: int


class PokedolaresPayload(BaseModel):
    usuario_id: int
    monto: int


class RecompensaBatallaPayload(BaseModel):
    battle_session_token: str | None = None
    usuario_id: int | None = None
    usuario_pokemon_ids: list[int] = []
    exp_ganada: int = 25
    pokedolares_ganados: int = 500


class BattleIniciarPayload(BaseModel):
    dificultad: str = "normal"
    usuario_pokemon_ids: list[int] | None = None
    guardar_equipo: bool = True


class GuardarEquipoPayload(BaseModel):
    usuario_pokemon_ids: list[int]


class ActividadUsuarioPayload(BaseModel):
    pagina: str = "global"
    accion: str = "heartbeat"
    detalle: str | None = None
    sesion_token: str | None = None
    online: bool = True


class CompraItemPayload(BaseModel):
    usuario_id: int | None = None
    item_id: int
    cantidad: int = 1


class EvolucionItemPayload(BaseModel):
    usuario_pokemon_id: int
    item_id: int
    usuario_id: int | None = None


class SoltarPokemonPayload(BaseModel):
    usuario_pokemon_id: int
    usuario_id: int | None = None


class EvolucionNivelPayload(BaseModel):
    usuario_pokemon_id: int
    usuario_id: int | None = None


class ActualizarAvatarPayload(BaseModel):
    avatar_id: str


class PresenciaMapaPayload(BaseModel):
    zona_id: int
    nodo_id: str


# =========================================================
# HELPERS
# =========================================================

def calcular_stats(
    base_hp: int,
    base_ataque: int,
    base_defensa: int,
    base_velocidad: int,
    nivel: int,
    es_shiny: bool = False,
    bonus_hp_renacer: int = 0,
    bonus_ataque_renacer: int = 0,
    bonus_defensa_renacer: int = 0,
    bonus_velocidad_renacer: int = 0,
    base_ataque_especial: int | None = None,
    base_defensa_especial: int | None = None,
):
    bonus_hp_shiny = 2 if es_shiny else 0

    base_ataque_especial = int(base_ataque if base_ataque_especial is None else base_ataque_especial)
    base_defensa_especial = int(base_defensa if base_defensa_especial is None else base_defensa_especial)

    hp_max = int(base_hp) + (int(nivel) * (5 + bonus_hp_shiny)) + int(bonus_hp_renacer or 0)
    ataque = int(base_ataque) + (int(nivel) * 2) + int(bonus_ataque_renacer or 0)
    defensa = int(base_defensa) + (int(nivel) * 2) + int(bonus_defensa_renacer or 0)
    velocidad = int(base_velocidad) + int(nivel) + int(bonus_velocidad_renacer or 0)
    ataque_especial = int(base_ataque_especial) + (int(nivel) * 2)
    defensa_especial = int(base_defensa_especial) + (int(nivel) * 2)

    return {
        "hp_max": hp_max,
        "ataque": ataque,
        "defensa": defensa,
        "velocidad": velocidad,
        "ataque_especial": ataque_especial,
        "defensa_especial": defensa_especial,
    }


def calcular_experiencia_total_base(nivel: int, experiencia_actual: int = 0) -> int:
    nivel_seguro = max(1, int(nivel or 1))
    experiencia_segura = max(0, int(experiencia_actual or 0))
    return int((50 * ((nivel_seguro - 1) * nivel_seguro / 2)) + experiencia_segura)


def normalizar_avatar_id(avatar_id: str | None, default: str = AVATAR_ID_DEFAULT) -> str:
    valor = str(avatar_id or "").strip().lower()
    if not valor:
        return default
    if not AVATAR_ID_REGEX.fullmatch(valor):
        return default
    return valor


def validar_avatar_id(avatar_id: str | None) -> str:
    valor = str(avatar_id or "").strip().lower()
    if not valor:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="avatar_id es obligatorio"
        )

    if not AVATAR_ID_REGEX.fullmatch(valor):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="avatar_id inválido"
        )

    return valor


def validar_nodo_mapa_id(nodo_id: str | None) -> str:
    valor = str(nodo_id or "").strip().lower()

    if not valor:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="nodo_id es obligatorio"
        )

    if not MAPS_NODE_ID_REGEX.fullmatch(valor):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="nodo_id inválido"
        )

    return valor


def normalizar_limit(limit: int | None, default: int = 10, maximo: int = 100) -> int:
    try:
        valor = int(limit or default)
    except (TypeError, ValueError):
        valor = default

    return max(1, min(valor, maximo))


def validar_usuario_payload(usuario_id_payload: int | None, usuario_id_real: int):
    if usuario_id_payload is None:
        return

    try:
        usuario_id_payload = int(usuario_id_payload)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="usuario_id inválido"
        )

    if int(usuario_id_payload) != int(usuario_id_real):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No puedes operar sobre otro usuario"
        )


def obtener_ip_request(request: Request | None) -> str | None:
    if request is None:
        return None

    try:
        forwarded_for = str(request.headers.get("x-forwarded-for", "") or "").split(",")[0].strip()
        if forwarded_for:
            return forwarded_for[:64]
    except Exception:
        pass

    try:
        if request.client and request.client.host:
            return str(request.client.host)[:64]
    except Exception:
        pass

    return None



def limpiar_rate_limit_memoria(endpoint: str | None = None, now_ms: int | None = None):
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    endpoints = [endpoint] if endpoint else list(MAPS_RATE_LIMIT_MEMORIA.keys())

    for ep in endpoints:
        config = MAPS_RATE_LIMIT_CONFIG.get(ep)
        if not config:
            continue

        ventana_ms = int(config["window_seconds"]) * 1000
        buckets = MAPS_RATE_LIMIT_MEMORIA.setdefault(ep, {})

        for usuario_id in list(buckets.keys()):
            cola = buckets.get(usuario_id)
            if cola is None:
                buckets.pop(usuario_id, None)
                continue

            limite_inferior = now_ms - ventana_ms

            while cola and cola[0] < limite_inferior:
                cola.popleft()

            if not cola:
                buckets.pop(usuario_id, None)


def limpiar_tablas_operativas_si_corresponde(cursor):
    global TABLAS_OPERATIVAS_ULTIMO_CLEANUP

    ahora = int(time.time())
    if (ahora - int(TABLAS_OPERATIVAS_ULTIMO_CLEANUP or 0)) < TABLAS_OPERATIVAS_CLEANUP_CADA_SEGUNDOS:
        return

    TABLAS_OPERATIVAS_ULTIMO_CLEANUP = ahora

    try:
        if existe_tabla(cursor, "maps_request_log"):
            cursor.execute("""
                DELETE FROM maps_request_log
                WHERE created_at < NOW() - INTERVAL '3 days'
            """)

        if existe_tabla(cursor, "usuario_actividad"):
            cursor.execute("""
                UPDATE usuario_actividad
                SET online = FALSE,
                    actualizado_en = NOW()
                WHERE online = TRUE
                  AND ultima_actividad < NOW() - INTERVAL '10 minutes'
            """)

            cursor.execute("""
                DELETE FROM usuario_actividad
                WHERE ultima_actividad < NOW() - INTERVAL '2 days'
            """)

        if existe_tabla(cursor, "battle_sesiones"):
            cursor.execute("""
                UPDATE battle_sesiones
                SET activo = FALSE,
                    resultado = CASE
                        WHEN resultado = 'pendiente' THEN 'expirada'
                        ELSE resultado
                    END,
                    cancelado_en = COALESCE(cancelado_en, NOW())
                WHERE activo = TRUE
                  AND expira_en <= NOW()
            """)

            cursor.execute("""
                DELETE FROM battle_sesiones
                WHERE activo = FALSE
                  AND COALESCE(reclamado_en, finalizado_en, cancelado_en, expira_en, creado_en)
                      < NOW() - INTERVAL '30 days'
            """)

        if existe_tabla(cursor, "mapa_encuentros"):
            cursor.execute("""
                UPDATE mapa_encuentros
                SET activo = FALSE,
                    cancelado_en = COALESCE(cancelado_en, NOW())
                WHERE activo = TRUE
                  AND expira_en <= NOW()
            """)

            cursor.execute("""
                DELETE FROM mapa_encuentros
                WHERE activo = FALSE
                  AND COALESCE(consumido_en, cancelado_en, expira_en, creado_en)
                      < NOW() - INTERVAL '1 day'
            """)
    except Exception as error:
        print("Error limpiando tablas operativas:", error)


def debe_registrar_log_maps(endpoint: str, status_code: int, motivo: str | None = None) -> bool:
    endpoint = str(endpoint or "").strip().lower()
    status_code = int(status_code or 0)
    motivo_normalizado = str(motivo or "").strip().lower()

    if status_code >= 400:
        return True

    if endpoint == "intentar-captura" and motivo_normalizado == "capturado":
        return True

    return False


def registrar_log_maps(
    cursor,
    *,
    usuario_id: int,
    endpoint: str,
    request: Request | None = None,
    status_code: int = 200,
    motivo: str | None = None,
    token: str | None = None,
    zona_id: int | None = None,
    encuentro_id: int | None = None,
):
    limpiar_tablas_operativas_si_corresponde(cursor)

    if not existe_tabla(cursor, "maps_request_log"):
        return

    if not debe_registrar_log_maps(endpoint, status_code, motivo):
        return

    ip = obtener_ip_request(request)
    user_agent = None
    if request is not None:
        try:
            user_agent = str(request.headers.get("user-agent", "") or "")[:500]
        except Exception:
            user_agent = None

    cursor.execute(
        """
        INSERT INTO maps_request_log
        (usuario_id, endpoint, ip, user_agent, status_code, motivo, token, zona_id, encuentro_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(usuario_id),
            str(endpoint),
            ip,
            user_agent,
            int(status_code),
            motivo,
            token,
            zona_id,
            encuentro_id,
        )
    )


def validar_rate_limit_maps(cursor, usuario_id: int, endpoint: str) -> str | None:
    limpiar_tablas_operativas_si_corresponde(cursor)

    config = MAPS_RATE_LIMIT_CONFIG.get(endpoint)
    if not config:
        return None

    now_ms = int(time.time() * 1000)
    limpiar_rate_limit_memoria(endpoint, now_ms)

    buckets = MAPS_RATE_LIMIT_MEMORIA.setdefault(endpoint, {})
    usuario_key = int(usuario_id)
    cola = buckets.setdefault(usuario_key, deque())

    if cola:
        diferencia_ms = max(0, now_ms - int(cola[-1]))
        if diferencia_ms < int(config["min_interval_ms"]):
            return (
                f"Demasiadas acciones seguidas en {endpoint}. "
                f"Espera {int(config['min_interval_ms'])} ms antes de intentar otra vez."
            )

    if len(cola) >= int(config["max_requests"]):
        return (
            f"Demasiadas solicitudes recientes en {endpoint}. "
            f"Límite: {int(config['max_requests'])} cada {int(config['window_seconds'])} segundos."
        )

    cola.append(now_ms)
    return None

def limpiar_cache_temporal(diccionario: dict, now_ts: int | None = None):
    if now_ts is None:
        now_ts = int(time.time())

    expirados = [clave for clave, expira_en in diccionario.items() if int(expira_en or 0) <= now_ts]
    for clave in expirados:
        diccionario.pop(clave, None)


def validar_cooldown_recompensa_batalla(usuario_id: int):
    now_ts = int(time.time())
    limpiar_cache_temporal(BATTLE_RECOMPENSA_ULTIMO_USO, now_ts)

    ultimo_uso = int(BATTLE_RECOMPENSA_ULTIMO_USO.get(int(usuario_id), 0) or 0)
    restante = ultimo_uso - now_ts

    if restante > 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Espera {restante}s antes de reclamar otra recompensa de batalla"
        )


def registrar_cooldown_recompensa_batalla(usuario_id: int):
    BATTLE_RECOMPENSA_ULTIMO_USO[int(usuario_id)] = int(time.time()) + BATTLE_RECOMPENSA_COOLDOWN_SEGUNDOS


def obtener_recompensa_batalla_permitida(exp_ganada: int, pokedolares_ganados: int) -> dict:
    try:
        clave = (max(0, int(exp_ganada or 0)), max(0, int(pokedolares_ganados or 0)))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Recompensa de batalla inválida"
        )

    recompensa = RECOMPENSAS_ARENA_PERMITIDAS.get(clave)
    if not recompensa:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La recompensa enviada no coincide con una dificultad válida"
        )

    return dict(recompensa)


def obtener_recompensa_batalla_por_codigo(dificultad_codigo: str | None) -> dict:
    codigo = str(dificultad_codigo or "normal").strip().lower()
    recompensa = RECOMPENSAS_ARENA_POR_CODIGO.get(codigo)
    if not recompensa:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La dificultad de batalla no es válida"
        )
    return dict(recompensa)


def normalizar_pagina_actividad(pagina: str | None) -> str:
    valor = str(pagina or "global").strip().lower()
    return valor if valor in ACTIVIDAD_PAGINAS_VALIDAS else "global"


def normalizar_accion_actividad(accion: str | None) -> str:
    valor = str(accion or "heartbeat").strip().lower()
    return valor if valor in ACTIVIDAD_ACCIONES_VALIDAS else "heartbeat"


def stats_especiales_habilitados(cursor, table_name: str) -> bool:
    return existe_columna(cursor, table_name, "ataque_especial") and existe_columna(cursor, table_name, "defensa_especial")


def obtener_select_stats_especiales(cursor, table_name: str, alias: str):
    if stats_especiales_habilitados(cursor, table_name):
        return (
            f"{alias}.ataque_especial AS ataque_especial",
            f"{alias}.defensa_especial AS defensa_especial",
        )
    return (
        f"{alias}.ataque AS ataque_especial",
        f"{alias}.defensa AS defensa_especial",
    )



def registrar_actividad_usuario(cursor, usuario_id: int, pagina: str = "global", accion: str = "heartbeat", detalle: str | None = None, sesion_token: str | None = None, online: bool = True):
    limpiar_tablas_operativas_si_corresponde(cursor)

    if not existe_tabla(cursor, "usuario_actividad"):
        return None

    pagina = normalizar_pagina_actividad(pagina)
    accion = normalizar_accion_actividad(accion)
    detalle = str(detalle or "").strip() or None

    sesion_token = str(sesion_token or "").strip()
    if not sesion_token:
        sesion_token = f"fallback:{pagina}"

    sesion_token = sesion_token[:120]

    try:
        cursor.execute(
            """
            INSERT INTO usuario_actividad
                (usuario_id, pagina, accion, detalle, sesion_token, online, ultima_actividad, creado_en, actualizado_en)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (usuario_id, sesion_token)
            DO UPDATE SET
                pagina = EXCLUDED.pagina,
                accion = EXCLUDED.accion,
                detalle = EXCLUDED.detalle,
                online = EXCLUDED.online,
                ultima_actividad = NOW(),
                actualizado_en = NOW()
            RETURNING id, ultima_actividad
            """,
            (usuario_id, pagina, accion, detalle, sesion_token, bool(online))
        )
        return cursor.fetchone()
    except Exception as error:
        print("Aviso registrar_actividad_usuario, fallback manual:", error)

        cursor.execute(
            """
            SELECT id
            FROM usuario_actividad
            WHERE usuario_id = %s
              AND sesion_token = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (usuario_id, sesion_token)
        )
        existente = cursor.fetchone()

        if existente:
            cursor.execute(
                """
                UPDATE usuario_actividad
                SET pagina = %s,
                    accion = %s,
                    detalle = %s,
                    online = %s,
                    ultima_actividad = NOW(),
                    actualizado_en = NOW()
                WHERE id = %s
                RETURNING id, ultima_actividad
                """,
                (pagina, accion, detalle, bool(online), int(existente["id"]))
            )
            return cursor.fetchone()

        cursor.execute(
            """
            INSERT INTO usuario_actividad
                (usuario_id, pagina, accion, detalle, sesion_token, online, ultima_actividad, creado_en, actualizado_en)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
            RETURNING id, ultima_actividad
            """,
            (usuario_id, pagina, accion, detalle, sesion_token, bool(online))
        )
        return cursor.fetchone()


def obtener_usuarios_activos_resumen_data(ttl_segundos: int = USUARIO_ACTIVIDAD_TTL_SEGUNDOS):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if not existe_tabla(cursor, "usuario_actividad"):
            return {
                "ttl_segundos": int(ttl_segundos),
                "usuarios_activos": 0,
                "sesiones_activas": 0,
                "por_pagina": []
            }

        cursor.execute(
            """
            SELECT
                COUNT(DISTINCT usuario_id) AS usuarios_activos,
                COUNT(*) AS sesiones_activas
            FROM usuario_actividad
            WHERE online = TRUE
              AND ultima_actividad >= (NOW() - (%s * INTERVAL '1 second'))
            """,
            (int(ttl_segundos),)
        )
        meta = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT pagina, COUNT(DISTINCT usuario_id) AS usuarios
            FROM usuario_actividad
            WHERE online = TRUE
              AND ultima_actividad >= (NOW() - (%s * INTERVAL '1 second'))
            GROUP BY pagina
            ORDER BY usuarios DESC, pagina ASC
            """,
            (int(ttl_segundos),)
        )
        por_pagina = [
            {"pagina": row["pagina"], "usuarios": int(row["usuarios"] or 0)}
            for row in cursor.fetchall()
        ]

        return {
            "ttl_segundos": int(ttl_segundos),
            "usuarios_activos": int(meta.get("usuarios_activos") or 0),
            "sesiones_activas": int(meta.get("sesiones_activas") or 0),
            "por_pagina": por_pagina
        }
    finally:
        cursor.close()
        release_connection(conn)


def obtener_usuarios_activos_detalle_data(ttl_segundos: int = USUARIO_ACTIVIDAD_TTL_SEGUNDOS):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if not existe_tabla(cursor, "usuario_actividad"):
            return []

        cursor.execute(
            """
            WITH sesiones AS (
                SELECT
                    ua.usuario_id,
                    ua.pagina,
                    ua.accion,
                    ua.detalle,
                    ua.sesion_token,
                    ua.online,
                    ua.ultima_actividad,
                    ROW_NUMBER() OVER (PARTITION BY ua.usuario_id ORDER BY ua.ultima_actividad DESC, ua.id DESC) AS rn
                FROM usuario_actividad ua
                WHERE ua.online = TRUE
                  AND ua.ultima_actividad >= (NOW() - (%s * INTERVAL '1 second'))
            )
            SELECT
                s.usuario_id,
                u.nombre,
                u.correo,
                u.foto,
                u.avatar_url,
                u.avatar_id,
                s.pagina,
                s.accion,
                s.detalle,
                s.sesion_token,
                s.ultima_actividad
            FROM sesiones s
            JOIN usuarios u ON u.id = s.usuario_id
            WHERE s.rn = 1
            ORDER BY s.ultima_actividad DESC, u.nombre ASC
            """,
            (int(ttl_segundos),)
        )
        rows = cursor.fetchall()
        return [
            {
                "usuario_id": int(row["usuario_id"]),
                "nombre": row["nombre"],
                "correo": row["correo"],
                "foto": row["foto"],
                "avatar_url": row["avatar_url"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "pagina": row["pagina"],
                "accion": row["accion"],
                "detalle": row["detalle"],
                "sesion_token": row["sesion_token"],
                "ultima_actividad": row["ultima_actividad"].isoformat() if row["ultima_actividad"] else None,
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


def validar_ids_pokemon_equipo(cursor, usuario_id: int, usuario_pokemon_ids: list[int]) -> list[int]:
    ids_limpios = []
    for raw_id in usuario_pokemon_ids or []:
        try:
            pokemon_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if pokemon_id <= 0 or pokemon_id in ids_limpios:
            continue
        ids_limpios.append(pokemon_id)

    if not ids_limpios:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debes enviar entre 1 y 6 Pokémon válidos"
        )

    if len(ids_limpios) > 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El equipo no puede tener más de 6 Pokémon"
        )

    cursor.execute(
        """
        SELECT id
        FROM usuario_pokemon
        WHERE usuario_id = %s
          AND id = ANY(%s)
        ORDER BY id ASC
        """,
        (usuario_id, ids_limpios)
    )
    rows = cursor.fetchall()
    ids_validos = {int(row["id"]) for row in rows}

    faltantes = [pokemon_id for pokemon_id in ids_limpios if pokemon_id not in ids_validos]
    if faltantes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Hay Pokémon que no pertenecen al usuario"
        )

    return ids_limpios


def guardar_equipo_usuario(cursor, usuario_id: int, usuario_pokemon_ids: list[int]) -> list[int]:
    ids_limpios = validar_ids_pokemon_equipo(cursor, usuario_id, usuario_pokemon_ids)

    # Borramos el equipo actual del usuario
    cursor.execute(
        "DELETE FROM equipo_usuario WHERE usuario_id = %s",
        (usuario_id,)
    )

    tiene_creado_en = existe_columna(cursor, "equipo_usuario", "creado_en")
    tiene_actualizado_en = existe_columna(cursor, "equipo_usuario", "actualizado_en")

    for posicion, usuario_pokemon_id in enumerate(ids_limpios, start=1):
        if tiene_creado_en and tiene_actualizado_en:
            cursor.execute(
                """
                INSERT INTO equipo_usuario (usuario_id, usuario_pokemon_id, posicion, creado_en, actualizado_en)
                VALUES (%s, %s, %s, NOW(), NOW())
                """,
                (usuario_id, usuario_pokemon_id, posicion)
            )
        else:
            cursor.execute(
                """
                INSERT INTO equipo_usuario (usuario_id, usuario_pokemon_id, posicion)
                VALUES (%s, %s, %s)
                """,
                (usuario_id, usuario_pokemon_id, posicion)
            )

    return ids_limpios

def obtener_equipo_guardado_ids(cursor, usuario_id: int) -> list[int]:
    cursor.execute(
        """
        SELECT usuario_pokemon_id
        FROM equipo_usuario
        WHERE usuario_id = %s
        ORDER BY posicion ASC, id ASC
        """,
        (usuario_id,)
    )
    return [int(row["usuario_pokemon_id"]) for row in cursor.fetchall()]


def crear_token_simple(longitud: int = 32) -> str:
    return secrets.token_urlsafe(longitud)



def invalidar_encuentros_activos_usuario(cursor, usuario_id: int):
    limpiar_tablas_operativas_si_corresponde(cursor)

    if not existe_tabla(cursor, "mapa_encuentros"):
        return
    cursor.execute(
        """
        UPDATE mapa_encuentros
        SET activo = FALSE,
            cancelado_en = COALESCE(cancelado_en, NOW())
        WHERE usuario_id = %s
          AND activo = TRUE
        """,
        (usuario_id,)
    )

def obtener_encuentro_activo_por_token(cursor, token: str, usuario_id: int, for_update: bool = False):
    if not existe_tabla(cursor, "mapa_encuentros"):
        return None

    sql_for_update = " FOR UPDATE" if for_update else ""
    cursor.execute(
        f"""
        SELECT *
        FROM mapa_encuentros
        WHERE token = %s
          AND usuario_id = %s
          AND activo = TRUE
          AND consumido_en IS NULL
          AND expira_en > NOW()
        LIMIT 1
        {sql_for_update}
        """,
        (token, usuario_id)
    )
    return cursor.fetchone()



def invalidar_sesiones_battle_activas(cursor, usuario_id: int):
    limpiar_tablas_operativas_si_corresponde(cursor)

    if not existe_tabla(cursor, "battle_sesiones"):
        return
    cursor.execute(
        """
        UPDATE battle_sesiones
        SET activo = FALSE,
            resultado = CASE WHEN resultado = 'pendiente' THEN 'cancelada' ELSE resultado END,
            cancelado_en = COALESCE(cancelado_en, NOW())
        WHERE usuario_id = %s
          AND activo = TRUE
        """,
        (usuario_id,)
    )

def obtener_sesion_battle_por_token(cursor, token: str, usuario_id: int, for_update: bool = False):
    if not existe_tabla(cursor, "battle_sesiones"):
        return None

    sql_for_update = " FOR UPDATE" if for_update else ""
    cursor.execute(
        f"""
        SELECT *
        FROM battle_sesiones
        WHERE token = %s
          AND usuario_id = %s
          AND activo = TRUE
          AND expira_en > NOW()
        LIMIT 1
        {sql_for_update}
        """,
        (token, usuario_id)
    )
    return cursor.fetchone()


def normalizar_equipo_ids_json(valor) -> list[int]:
    if valor is None:
        return []
    if isinstance(valor, str):
        try:
            valor = json.loads(valor)
        except Exception:
            return []
    if not isinstance(valor, list):
        return []
    resultado = []
    for item in valor:
        try:
            numero = int(item)
        except (TypeError, ValueError):
            continue
        if numero > 0:
            resultado.append(numero)
    return resultado


def existe_tabla(cursor, table_name: str) -> bool:
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        ) AS existe
    """, (table_name,))
    row = cursor.fetchone()
    return bool(row["existe"]) if row else False


def existe_columna(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        ) AS existe
    """, (table_name, column_name))
    row = cursor.fetchone()
    return bool(row["existe"]) if row else False


def obtener_expresion_codigo_variante(cursor, alias: str = "up") -> str:
    """
    Si en el futuro agregas usuario_pokemon.variante_codigo,
    el ranking de capturas únicas lo usará automáticamente.
    Mientras tanto usa normal/shiny.
    """
    if existe_columna(cursor, "usuario_pokemon", "variante_codigo"):
        return f"COALESCE(NULLIF(TRIM({alias}.variante_codigo), ''), CASE WHEN {alias}.es_shiny THEN 'shiny' ELSE 'normal' END)"
    return f"CASE WHEN {alias}.es_shiny THEN 'shiny' ELSE 'normal' END"


def obtener_variantes_pokedex_data(cursor):
    """
    Intenta leer variantes desde una tabla opcional pokedex_variantes.
    Si no existe, usa normal/shiny por defecto.
    """
    variantes = []

    try:
        if existe_tabla(cursor, "pokedex_variantes"):
            if existe_columna(cursor, "pokedex_variantes", "codigo") and existe_columna(cursor, "pokedex_variantes", "nombre"):
                order_sql = "codigo ASC"
                if existe_columna(cursor, "pokedex_variantes", "orden"):
                    order_sql = "orden ASC, codigo ASC"

                where_sql = ""
                if existe_columna(cursor, "pokedex_variantes", "activo"):
                    where_sql = "WHERE activo = TRUE"

                cursor.execute(f"""
                    SELECT codigo, nombre
                    FROM pokedex_variantes
                    {where_sql}
                    ORDER BY {order_sql}
                """)
                rows = cursor.fetchall()

                for row in rows:
                    codigo = str(row["codigo"] or "").strip().lower()
                    nombre = str(row["nombre"] or codigo).strip()
                    if codigo:
                        variantes.append({
                            "codigo": codigo,
                            "nombre": nombre
                        })
    except Exception:
        variantes = []

    if variantes:
        return variantes

    resultado = [{"codigo": "normal", "nombre": "Normal"}]

    if existe_columna(cursor, "usuario_pokemon", "es_shiny"):
        resultado.append({"codigo": "shiny", "nombre": "Shiny"})

    return resultado


def obtener_meta_pokedex_global(cursor):
    cursor.execute("""
        SELECT COUNT(*) AS total_pokemon
        FROM pokemon
    """)
    total_pokemon = int(cursor.fetchone()["total_pokemon"] or 0)

    variantes_disponibles = obtener_variantes_pokedex_data(cursor)
    total_variantes_por_pokemon = max(1, len(variantes_disponibles))
    total_pokedex = total_pokemon * total_variantes_por_pokemon

    return {
        "total_pokemon_tabla": total_pokemon,
        "total_variantes_por_pokemon": total_variantes_por_pokemon,
        "total_pokedex": total_pokedex,
        "variantes_disponibles": variantes_disponibles
    }


def obtener_usuario_por_id(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT
                id,
                google_id,
                nombre,
                correo,
                foto,
                avatar_url,
                avatar_id,
                rol,
                pokedolares,
                fecha_registro
            FROM usuarios
            WHERE id = %s
        """, (usuario_id,))
        usuario = cursor.fetchone()

        if not usuario:
            return None

        data = dict(usuario)
        data["avatar_id"] = normalizar_avatar_id(data.get("avatar_id"))
        return data
    finally:
        cursor.close()
        release_connection(conn)


def obtener_items_usuario_data(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT
                i.id AS item_id,
                i.nombre,
                i.tipo,
                i.descripcion,
                i.bonus_captura,
                i.cura_hp,
                i.precio,
                ui.cantidad
            FROM usuario_items ui
            JOIN items i ON i.id = ui.item_id
            WHERE ui.usuario_id = %s
            ORDER BY i.nombre
        """, (usuario_id,))
        rows = cursor.fetchall()

        return [
            {
                "item_id": row["item_id"],
                "nombre": row["nombre"],
                "tipo": row["tipo"],
                "descripcion": row["descripcion"],
                "bonus_captura": row["bonus_captura"],
                "cura_hp": row["cura_hp"],
                "precio": row["precio"],
                "cantidad": row["cantidad"]
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


def obtener_pokemon_usuario_data(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        up_atk_sp_sql, up_def_sp_sql = obtener_select_stats_especiales(cursor, "usuario_pokemon", "up")

        cursor.execute(f"""
            SELECT
                up.id,
                p.id AS pokemon_id,
                p.nombre,
                p.tipo,
                p.imagen,
                p.rareza,
                p.generacion,
                p.tiene_mega,
                up.nivel,
                up.experiencia,
                COALESCE(up.experiencia_total, 0) AS experiencia_total,
                COALESCE(up.victorias_total, 0) AS victorias_total,
                up.hp_actual,
                up.hp_max,
                up.ataque,
                up.defensa,
                up.velocidad,
                {up_atk_sp_sql},
                {up_def_sp_sql},
                up.es_shiny,
                up.renacimientos,
                up.puntos_renacer_disponibles,
                up.bonus_hp_renacer,
                up.bonus_ataque_renacer,
                up.bonus_defensa_renacer,
                up.bonus_velocidad_renacer,
                up.fecha_captura
            FROM usuario_pokemon up
            JOIN pokemon p ON p.id = up.pokemon_id
            WHERE up.usuario_id = %s
            ORDER BY up.fecha_captura DESC, up.id DESC
        """, (usuario_id,))
        rows = cursor.fetchall()

        resultado = []
        for row in rows:
            resultado.append({
                "id": row["id"],
                "pokemon_id": row["pokemon_id"],
                "nombre": row["nombre"],
                "tipo": row["tipo"],
                "imagen": row["imagen"],
                "rareza": row["rareza"],
                "generacion": row["generacion"],
                "tiene_mega": row["tiene_mega"],
                "nivel": row["nivel"],
                "experiencia": row["experiencia"],
                "experiencia_total": row["experiencia_total"],
                "victorias_total": row["victorias_total"],
                "hp_actual": row["hp_actual"],
                "hp_max": row["hp_max"],
                "ataque": row["ataque"],
                "defensa": row["defensa"],
                "velocidad": row["velocidad"],
                "ataque_especial": int(row.get("ataque_especial") or row["ataque"] or 0),
                "defensa_especial": int(row.get("defensa_especial") or row["defensa"] or 0),
                "es_shiny": bool(row["es_shiny"]),
                "renacimientos": row["renacimientos"],
                "puntos_renacer_disponibles": row["puntos_renacer_disponibles"],
                "bonus_hp_renacer": row["bonus_hp_renacer"],
                "bonus_ataque_renacer": row["bonus_ataque_renacer"],
                "bonus_defensa_renacer": row["bonus_defensa_renacer"],
                "bonus_velocidad_renacer": row["bonus_velocidad_renacer"],
                "fecha_captura": row["fecha_captura"].isoformat() if row["fecha_captura"] else None
            })

        return resultado
    finally:
        cursor.close()
        release_connection(conn)


def obtener_equipo_usuario_data(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if not existe_tabla(cursor, "equipo_usuario"):
            return []

        up_atk_sp_sql, up_def_sp_sql = obtener_select_stats_especiales(cursor, "usuario_pokemon", "up")

        cursor.execute(f"""
            SELECT
                eu.posicion,
                eu.usuario_pokemon_id,
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
                {up_atk_sp_sql},
                {up_def_sp_sql},
                up.es_shiny,
                p.nombre,
                p.tipo,
                p.imagen,
                p.rareza,
                p.generacion,
                p.tiene_mega
            FROM equipo_usuario eu
            JOIN usuario_pokemon up ON up.id = eu.usuario_pokemon_id
            JOIN pokemon p ON p.id = up.pokemon_id
            WHERE eu.usuario_id = %s
            ORDER BY eu.posicion ASC, eu.id ASC
        """, (usuario_id,))
        rows = cursor.fetchall()

        return [
            {
                "posicion": int(row["posicion"]),
                "es_lider": int(row["posicion"]) == 1,
                "id": int(row["usuario_pokemon_id"]),
                "usuario_id": int(row["usuario_id"]),
                "pokemon_id": int(row["pokemon_id"]),
                "nombre": row["nombre"],
                "tipo": row["tipo"],
                "imagen": row["imagen"],
                "rareza": row["rareza"],
                "generacion": row["generacion"],
                "tiene_mega": row["tiene_mega"],
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
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


def crear_inventario_inicial(cursor, usuario_id: int):
    items_iniciales = [
        (usuario_id, 1, 5),   # Poke Ball
        (usuario_id, 2, 2),   # Super Ball
        (usuario_id, 4, 3),   # Pocion
    ]

    for uid, item_id, cantidad in items_iniciales:
        cursor.execute("""
            INSERT INTO usuario_items (usuario_id, item_id, cantidad)
            VALUES (%s, %s, %s)
            ON CONFLICT (usuario_id, item_id)
            DO NOTHING
        """, (uid, item_id, cantidad))


def obtener_ranking_entrenadores_data(limit: int = 10):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        limit = normalizar_limit(limit)

        cursor.execute("""
            SELECT
                u.id AS usuario_id,
                u.nombre,
                u.correo,
                u.foto,
                u.avatar_url,
                u.avatar_id,
                u.pokedolares,
                u.fecha_registro,
                COUNT(up.id) AS total_pokemon,
                COALESCE(SUM(up.experiencia_total), 0) AS experiencia_total_sum,
                COALESCE(SUM(up.victorias_total), 0) AS victorias_total_sum,
                COALESCE(MAX(up.nivel), 0) AS nivel_maximo
            FROM usuarios u
            LEFT JOIN usuario_pokemon up ON up.usuario_id = u.id
            GROUP BY
                u.id, u.nombre, u.correo, u.foto, u.avatar_url, u.avatar_id, u.pokedolares, u.fecha_registro
            ORDER BY
                experiencia_total_sum DESC,
                victorias_total_sum DESC,
                u.pokedolares DESC,
                u.id ASC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()

        resultado = []
        for idx, row in enumerate(rows, start=1):
            resultado.append({
                "puesto": idx,
                "usuario_id": row["usuario_id"],
                "nombre": row["nombre"],
                "correo": row["correo"],
                "foto": row["foto"],
                "avatar_url": row["avatar_url"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "pokedolares": row["pokedolares"],
                "fecha_registro": row["fecha_registro"].isoformat() if row["fecha_registro"] else None,
                "total_pokemon": int(row["total_pokemon"] or 0),
                "experiencia_total_sum": int(row["experiencia_total_sum"] or 0),
                "victorias_total_sum": int(row["victorias_total_sum"] or 0),
                "nivel_maximo": int(row["nivel_maximo"] or 0)
            })

        return resultado
    finally:
        cursor.close()
        release_connection(conn)


def obtener_ranking_pokemon_experiencia_data(limit: int = 10):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        limit = normalizar_limit(limit)

        cursor.execute("""
            SELECT
                up.id AS usuario_pokemon_id,
                up.usuario_id,
                u.nombre AS entrenador_nombre,
                u.avatar_id,
                p.id AS pokemon_id,
                p.nombre AS pokemon_nombre,
                p.tipo,
                p.imagen,
                up.nivel,
                up.es_shiny,
                COALESCE(up.experiencia_total, 0) AS experiencia_total,
                COALESCE(up.victorias_total, 0) AS victorias_total,
                up.fecha_captura
            FROM usuario_pokemon up
            JOIN usuarios u ON u.id = up.usuario_id
            JOIN pokemon p ON p.id = up.pokemon_id
            ORDER BY
                experiencia_total DESC,
                victorias_total DESC,
                up.nivel DESC,
                up.id ASC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()

        resultado = []
        for idx, row in enumerate(rows, start=1):
            resultado.append({
                "puesto": idx,
                "usuario_pokemon_id": row["usuario_pokemon_id"],
                "usuario_id": row["usuario_id"],
                "entrenador_nombre": row["entrenador_nombre"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "pokemon_id": row["pokemon_id"],
                "pokemon_nombre": row["pokemon_nombre"],
                "tipo": row["tipo"],
                "imagen": row["imagen"],
                "nivel": row["nivel"],
                "es_shiny": bool(row["es_shiny"]),
                "experiencia_total": int(row["experiencia_total"] or 0),
                "victorias_total": int(row["victorias_total"] or 0),
                "fecha_captura": row["fecha_captura"].isoformat() if row["fecha_captura"] else None
            })

        return resultado
    finally:
        cursor.close()
        release_connection(conn)


def obtener_ranking_pokemon_victorias_data(limit: int = 10):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        limit = normalizar_limit(limit)

        cursor.execute("""
            SELECT
                up.id AS usuario_pokemon_id,
                up.usuario_id,
                u.nombre AS entrenador_nombre,
                u.avatar_id,
                p.id AS pokemon_id,
                p.nombre AS pokemon_nombre,
                p.tipo,
                p.imagen,
                up.nivel,
                up.es_shiny,
                COALESCE(up.experiencia_total, 0) AS experiencia_total,
                COALESCE(up.victorias_total, 0) AS victorias_total,
                up.fecha_captura
            FROM usuario_pokemon up
            JOIN usuarios u ON u.id = up.usuario_id
            JOIN pokemon p ON p.id = up.pokemon_id
            ORDER BY
                victorias_total DESC,
                experiencia_total DESC,
                up.nivel DESC,
                up.id ASC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()

        resultado = []
        for idx, row in enumerate(rows, start=1):
            resultado.append({
                "puesto": idx,
                "usuario_pokemon_id": row["usuario_pokemon_id"],
                "usuario_id": row["usuario_id"],
                "entrenador_nombre": row["entrenador_nombre"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "pokemon_id": row["pokemon_id"],
                "pokemon_nombre": row["pokemon_nombre"],
                "tipo": row["tipo"],
                "imagen": row["imagen"],
                "nivel": row["nivel"],
                "es_shiny": bool(row["es_shiny"]),
                "experiencia_total": int(row["experiencia_total"] or 0),
                "victorias_total": int(row["victorias_total"] or 0),
                "fecha_captura": row["fecha_captura"].isoformat() if row["fecha_captura"] else None
            })

        return resultado
    finally:
        cursor.close()
        release_connection(conn)


def obtener_ranking_capturas_unicas_data(limit: int = 10):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        limit = normalizar_limit(limit)
        meta = obtener_meta_pokedex_global(cursor)
        expresion_variante = obtener_expresion_codigo_variante(cursor, "up")

        cursor.execute(f"""
            SELECT
                u.id AS usuario_id,
                u.nombre,
                u.correo,
                u.foto,
                u.avatar_url,
                u.avatar_id,
                u.fecha_registro,
                COUNT(DISTINCT CASE WHEN COALESCE(up.es_shiny, FALSE) = FALSE THEN up.pokemon_id END) AS total_normales,
                COUNT(DISTINCT CASE WHEN COALESCE(up.es_shiny, FALSE) = TRUE THEN up.pokemon_id END) AS total_shiny,
                COUNT(DISTINCT CONCAT(up.pokemon_id::text, '|', {expresion_variante})) AS total_unicos,
                COALESCE(SUM(up.experiencia_total), 0) AS experiencia_total_sum,
                COALESCE(SUM(up.victorias_total), 0) AS victorias_total_sum
            FROM usuarios u
            LEFT JOIN usuario_pokemon up ON up.usuario_id = u.id
            GROUP BY
                u.id, u.nombre, u.correo, u.foto, u.avatar_url, u.avatar_id, u.fecha_registro
            ORDER BY
                total_unicos DESC,
                experiencia_total_sum DESC,
                victorias_total_sum DESC,
                u.id ASC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()

        resultado = []
        total_pokedex = int(meta["total_pokedex"] or 0)

        for idx, row in enumerate(rows, start=1):
            total_unicos = int(row["total_unicos"] or 0)
            avance = round((total_unicos / total_pokedex) * 100, 1) if total_pokedex > 0 else 0.0

            resultado.append({
                "puesto": idx,
                "usuario_id": row["usuario_id"],
                "nombre": row["nombre"],
                "correo": row["correo"],
                "foto": row["foto"],
                "avatar_url": row["avatar_url"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "fecha_registro": row["fecha_registro"].isoformat() if row["fecha_registro"] else None,
                "total_normales": int(row["total_normales"] or 0),
                "total_shiny": int(row["total_shiny"] or 0),
                "total_unicos": total_unicos,
                "total_pokedex": total_pokedex,
                "avance": avance,
                "experiencia_total_sum": int(row["experiencia_total_sum"] or 0),
                "victorias_total_sum": int(row["victorias_total_sum"] or 0)
            })

        return {
            "meta_pokedex": meta,
            "ranking": resultado
        }
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# MAPS / PRESENCIA HELPERS
# =========================================================
def construir_payload_presencia_usuario(usuario: dict, zona_id: int, nodo_id: str, updated_at=None):
    marca_tiempo = updated_at
    if marca_tiempo is None:
        marca_tiempo = datetime.now(timezone.utc)

    if hasattr(marca_tiempo, "isoformat"):
        marca_tiempo = marca_tiempo.isoformat()

    return {
        "usuario_id": int(usuario["id"]),
        "nombre": usuario.get("nombre"),
        "foto": usuario.get("foto"),
        "avatar_url": usuario.get("avatar_url"),
        "avatar_id": normalizar_avatar_id(usuario.get("avatar_id")),
        "zona_id": int(zona_id),
        "nodo_id": str(nodo_id),
        "updated_at": marca_tiempo
    }


def limpiar_presencia_expirada(cursor, ttl_segundos: int = MAPS_PRESENCIA_TTL_SEGUNDOS):
    cursor.execute(
        """
        DELETE FROM mapa_presencia
        WHERE updated_at < (NOW() - (%s * INTERVAL '1 second'))
        """,
        (int(ttl_segundos),)
    )


def registrar_presencia_usuario(cursor, usuario_id: int, zona_id: int, nodo_id: str):
    cursor.execute("DELETE FROM mapa_presencia WHERE usuario_id = %s", (usuario_id,))
    cursor.execute(
        """
        INSERT INTO mapa_presencia (usuario_id, zona_id, nodo_id, updated_at)
        VALUES (%s, %s, %s, NOW())
        RETURNING updated_at
        """,
        (usuario_id, zona_id, nodo_id)
    )
    row = cursor.fetchone()
    return row["updated_at"] if row else None


def eliminar_presencia_usuario(cursor, usuario_id: int):
    cursor.execute("DELETE FROM mapa_presencia WHERE usuario_id = %s", (usuario_id,))


def obtener_presencia_usuario_actual(cursor, usuario_id: int):
    cursor.execute(
        """
        SELECT usuario_id, zona_id, nodo_id, updated_at
        FROM mapa_presencia
        WHERE usuario_id = %s
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """,
        (usuario_id,)
    )
    return cursor.fetchone()


def validar_zona_existente(cursor, zona_id: int):
    cursor.execute("SELECT id FROM zonas WHERE id = %s LIMIT 1", (zona_id,))
    zona = cursor.fetchone()
    if not zona:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Zona no encontrada"
        )


def obtener_jugadores_presencia_zona_data(zona_id: int, excluir_usuario_id: int | None = None):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        limpiar_presencia_expirada(cursor)

        params = [zona_id, MAPS_PRESENCIA_TTL_SEGUNDOS]
        where_extra = ""

        if excluir_usuario_id is not None:
            where_extra = "AND mp.usuario_id <> %s"
            params.append(excluir_usuario_id)

        cursor.execute(
            f"""
            SELECT
                mp.usuario_id,
                mp.zona_id,
                mp.nodo_id,
                mp.updated_at,
                u.nombre,
                u.foto,
                u.avatar_url,
                u.avatar_id
            FROM mapa_presencia mp
            JOIN usuarios u ON u.id = mp.usuario_id
            WHERE mp.zona_id = %s
              AND mp.updated_at >= (NOW() - (%s * INTERVAL '1 second'))
              {where_extra}
            ORDER BY mp.updated_at DESC, u.nombre ASC
            """,
            tuple(params)
        )
        rows = cursor.fetchall()

        return [
            {
                "usuario_id": int(row["usuario_id"]),
                "nombre": row["nombre"],
                "foto": row["foto"],
                "avatar_url": row["avatar_url"],
                "avatar_id": normalizar_avatar_id(row.get("avatar_id")),
                "zona_id": int(row["zona_id"]),
                "nodo_id": row["nodo_id"],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


async def maps_ws_registrar(zona_id: int, usuario_id: int, websocket: WebSocket) -> str:
    connection_id = f"{usuario_id}:{id(websocket)}"

    async with MAPS_WEBSOCKET_LOCK:
        zona_conexiones = MAPS_WEBSOCKET_CONEXIONES.setdefault(zona_id, {})
        zona_conexiones[connection_id] = {
            "usuario_id": usuario_id,
            "websocket": websocket
        }

    return connection_id


async def maps_ws_quitar(zona_id: int | None, connection_id: str | None):
    if zona_id is None or not connection_id:
        return

    async with MAPS_WEBSOCKET_LOCK:
        zona_conexiones = MAPS_WEBSOCKET_CONEXIONES.get(zona_id)
        if not zona_conexiones:
            return

        zona_conexiones.pop(connection_id, None)

        if not zona_conexiones:
            MAPS_WEBSOCKET_CONEXIONES.pop(zona_id, None)


async def maps_ws_es_conexion_activa(zona_id: int | None, connection_id: str | None) -> bool:
    if zona_id is None or not connection_id:
        return False

    async with MAPS_WEBSOCKET_LOCK:
        zona_conexiones = MAPS_WEBSOCKET_CONEXIONES.get(zona_id, {})
        return connection_id in zona_conexiones


async def maps_ws_desregistrar_otras_conexiones_usuario(usuario_id: int, excluir_connection_id: str | None = None):
    conexiones_a_cerrar = []

    async with MAPS_WEBSOCKET_LOCK:
        for zona_id, zona_conexiones in list(MAPS_WEBSOCKET_CONEXIONES.items()):
            for connection_id, data in list(zona_conexiones.items()):
                if int(data.get("usuario_id") or 0) != int(usuario_id):
                    continue

                if excluir_connection_id and connection_id == excluir_connection_id:
                    continue

                conexiones_a_cerrar.append({
                    "zona_id": zona_id,
                    "connection_id": connection_id,
                    "websocket": data.get("websocket")
                })
                zona_conexiones.pop(connection_id, None)

            if not zona_conexiones:
                MAPS_WEBSOCKET_CONEXIONES.pop(zona_id, None)

    return conexiones_a_cerrar


async def maps_ws_broadcast_zona(zona_id: int, payload: dict, excluir_connection_id: str | None = None):
    async with MAPS_WEBSOCKET_LOCK:
        zona_conexiones = MAPS_WEBSOCKET_CONEXIONES.get(zona_id, {})
        conexiones = list(zona_conexiones.items())

    caidas = []

    for connection_id, data in conexiones:
        if excluir_connection_id and connection_id == excluir_connection_id:
            continue

        websocket = data.get("websocket")
        if not websocket:
            caidas.append(connection_id)
            continue

        try:
            await websocket.send_json(payload)
        except Exception:
            caidas.append(connection_id)

    for connection_id in caidas:
        await maps_ws_quitar(zona_id, connection_id)


def obtener_token_desde_websocket(websocket: WebSocket) -> str:
    auth_header = websocket.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    token = websocket.query_params.get("token", "")
    if token.lower().startswith("bearer "):
        token = token[7:]

    return token.strip()


# =========================================================
# AUTH
# =========================================================

@router.post("/auth/google-login")
def google_login(payload: GoogleLoginPayload):
    google_info = verify_google_token(payload.credential)

    google_id = google_info.get("sub")
    correo = google_info.get("email")
    nombre = google_info.get("name") or "Entrenador"
    foto = google_info.get("picture")
    rol = "usuario"
    avatar_id_default = AVATAR_ID_DEFAULT

    if not google_id or not correo:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Google no devolvió datos suficientes"
        )

    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT
                id,
                nombre,
                correo,
                foto,
                avatar_url,
                avatar_id,
                rol,
                pokedolares,
                fecha_registro
            FROM usuarios
            WHERE google_id = %s OR correo = %s
            LIMIT 1
        """, (google_id, correo))
        usuario = cursor.fetchone()

        if usuario:
            usuario_id = usuario["id"]

            cursor.execute("""
                UPDATE usuarios
                SET google_id = %s,
                    nombre = %s,
                    correo = %s,
                    foto = %s,
                    avatar_id = COALESCE(NULLIF(TRIM(avatar_id), ''), %s)
                WHERE id = %s
            """, (google_id, nombre, correo, foto, avatar_id_default, usuario_id))
            conn.commit()

            usuario_actualizado = obtener_usuario_por_id(usuario_id)
            token = create_access_token(
                user_id=usuario_actualizado["id"],
                correo=usuario_actualizado["correo"],
                rol=usuario_actualizado["rol"]
            )

            return {
                "access_token": token,
                "token_type": "bearer",
                "usuario": usuario_actualizado,
                "mensaje": "Usuario existente"
            }

        cursor.execute("""
            INSERT INTO usuarios (google_id, nombre, correo, foto, avatar_url, avatar_id, rol)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (google_id, nombre, correo, foto, foto, avatar_id_default, rol))
        nuevo_id = cursor.fetchone()["id"]

        crear_inventario_inicial(cursor, nuevo_id)
        conn.commit()

        usuario_nuevo = obtener_usuario_por_id(nuevo_id)
        token = create_access_token(
            user_id=usuario_nuevo["id"],
            correo=usuario_nuevo["correo"],
            rol=usuario_nuevo["rol"]
        )

        return {
            "access_token": token,
            "token_type": "bearer",
            "usuario": usuario_nuevo,
            "mensaje": "Usuario creado"
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/auth/me")
def auth_me(usuario=Depends(get_current_user)):
    usuario_actual = obtener_usuario_por_id(usuario["id"])
    if not usuario_actual:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )

    return {
        "autenticado": True,
        "usuario": usuario_actual
    }


# =========================================================
# USUARIO / ME
# =========================================================

@router.get("/usuario/me")
def obtener_usuario_me(usuario=Depends(get_current_user)):
    usuario_actual = obtener_usuario_por_id(usuario["id"])
    if not usuario_actual:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )
    return usuario_actual


@router.put("/usuario/me/avatar")
def actualizar_avatar_me(payload: ActualizarAvatarPayload, usuario=Depends(get_current_user)):
    avatar_id = validar_avatar_id(payload.avatar_id)

    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            UPDATE usuarios
            SET avatar_id = %s
            WHERE id = %s
        """, (avatar_id, usuario["id"]))
        conn.commit()

        usuario_actualizado = obtener_usuario_por_id(usuario["id"])

        return {
            "ok": True,
            "mensaje": "Avatar actualizado correctamente",
            "usuario": usuario_actualizado
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/usuario/me/items")
def obtener_items_me(usuario=Depends(get_current_user)):
    return obtener_items_usuario_data(usuario["id"])


@router.get("/usuario/me/pokemon")
def obtener_pokemon_me(usuario=Depends(get_current_user)):
    return obtener_pokemon_usuario_data(usuario["id"])


@router.get("/usuario/me/equipo")
def obtener_equipo_me(usuario=Depends(get_current_user)):
    return {
        "ok": True,
        "usuario_id": int(usuario["id"]),
        "equipo": obtener_equipo_usuario_data(usuario["id"])
    }


@router.post("/usuario/me/equipo")
def guardar_equipo_me(payload: GuardarEquipoPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        ids_guardados = guardar_equipo_usuario(cursor, usuario["id"], payload.usuario_pokemon_ids)

        try:
            registrar_actividad_usuario(
                cursor,
                usuario["id"],
                pagina="battle",
                accion="team_save",
                detalle=f"equipo:{len(ids_guardados)}"
            )
        except Exception as actividad_error:
            print("Aviso actividad guardar equipo:", repr(actividad_error))

        conn.commit()

        return {
            "ok": True,
            "mensaje": "Equipo guardado correctamente",
            "usuario_id": int(usuario["id"]),
            "equipo_ids": ids_guardados,
            "equipo": obtener_equipo_usuario_data(usuario["id"])
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /usuario/me/equipo:", repr(error))
        return {
            "ok": False,
            "mensaje": "No se pudo guardar el equipo"
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/usuario/{usuario_id}/equipo")
def obtener_equipo_usuario_publico(usuario_id: int):
    return {
        "ok": True,
        "usuario_id": int(usuario_id),
        "equipo": obtener_equipo_usuario_data(usuario_id)
    }


@router.post("/usuario/actividad")
def registrar_actividad(payload: ActividadUsuarioPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        row = registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina=payload.pagina,
            accion=payload.accion,
            detalle=payload.detalle,
            sesion_token=payload.sesion_token,
            online=payload.online,
        )
        conn.commit()
        return {
            "ok": True,
            "usuario_id": int(usuario["id"]),
            "pagina": normalizar_pagina_actividad(payload.pagina),
            "accion": normalizar_accion_actividad(payload.accion),
            "online": bool(payload.online),
            "ultima_actividad": row["ultima_actividad"].isoformat() if row and row.get("ultima_actividad") else datetime.now(timezone.utc).isoformat(),
        }
    except Exception as error:
        conn.rollback()
        print("Error en /usuario/actividad:", error)
        return {"ok": False, "mensaje": "No se pudo registrar la actividad"}
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/actividad/activos/resumen")
def actividad_activos_resumen(usuario=Depends(get_current_user), ttl_segundos: int = USUARIO_ACTIVIDAD_TTL_SEGUNDOS):
    return obtener_usuarios_activos_resumen_data(ttl_segundos)


@router.get("/actividad/activos/detalle")
def actividad_activos_detalle(usuario=Depends(get_current_user), ttl_segundos: int = USUARIO_ACTIVIDAD_TTL_SEGUNDOS):
    return {
        "ttl_segundos": int(ttl_segundos),
        "usuarios": obtener_usuarios_activos_detalle_data(ttl_segundos)
    }


# =========================================================
# RANKING
# =========================================================

@router.get("/ranking/entrenadores")
def ranking_entrenadores(limit: int = 10):
    return obtener_ranking_entrenadores_data(limit)


@router.get("/ranking/pokemon/experiencia")
def ranking_pokemon_experiencia(limit: int = 10):
    return obtener_ranking_pokemon_experiencia_data(limit)


@router.get("/ranking/pokemon/victorias")
def ranking_pokemon_victorias(limit: int = 10):
    return obtener_ranking_pokemon_victorias_data(limit)


@router.get("/ranking/capturas-unicas")
def ranking_capturas_unicas(limit: int = 10):
    return obtener_ranking_capturas_unicas_data(limit)


@router.get("/ranking/resumen")
def ranking_resumen(limit: int = 10):
    limit = normalizar_limit(limit)
    capturas = obtener_ranking_capturas_unicas_data(limit)

    return {
        "meta_pokedex": capturas["meta_pokedex"],
        "capturas_unicas": capturas["ranking"],
        "entrenadores": obtener_ranking_entrenadores_data(limit),
        "pokemon_experiencia": obtener_ranking_pokemon_experiencia_data(limit),
        "pokemon_victorias": obtener_ranking_pokemon_victorias_data(limit)
    }


# =========================================================
# POKEMON
# =========================================================

@router.get("/pokemon")
def get_pokemon():
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
        cursor.execute(f"""
            SELECT id, nombre, tipo, ataque, defensa, hp, imagen, velocidad, nivel, rareza, generacion, tiene_mega,
                   {p_atk_sp_sql}, {p_def_sp_sql}
            FROM pokemon p
            ORDER BY id
        """)
        rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "nombre": row["nombre"],
                "tipo": row["tipo"],
                "ataque": row["ataque"],
                "defensa": row["defensa"],
                "hp": row["hp"],
                "imagen": row["imagen"],
                "velocidad": row["velocidad"],
                "nivel": row["nivel"],
                "rareza": row["rareza"],
                "generacion": row["generacion"],
                "tiene_mega": row["tiene_mega"],
                "ataque_especial": int(row.get("ataque_especial") or row["ataque"] or 0),
                "defensa_especial": int(row.get("defensa_especial") or row["defensa"] or 0),
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/pokemon/{pokemon_id}/evoluciones")
def obtener_evoluciones(pokemon_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT 
                p1.id,
                p1.nombre,
                e.nivel,
                p2.id AS evolucion_id,
                p2.nombre AS evolucion_nombre
            FROM evoluciones e
            JOIN pokemon p1 ON p1.id = e.pokemon_id
            JOIN pokemon p2 ON p2.id = e.evoluciona_a
            WHERE p1.id = %s
        """, (pokemon_id,))
        rows = cursor.fetchall()

        return [
            {
                "id": r["id"],
                "nombre": r["nombre"],
                "nivel": r["nivel"],
                "evolucion_id": r["evolucion_id"],
                "evolucion_nombre": r["evolucion_nombre"]
            }
            for r in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/pokemon/{pokemon_id}/detalle-evolucion")
def obtener_detalle_evolucion(pokemon_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT
                er.id,
                er.metodo,
                er.nivel_requerido,
                er.item_id,
                er.prioridad,
                er.activo,
                p2.id AS evolucion_id,
                p2.nombre AS evolucion_nombre,
                i.nombre AS item_nombre
            FROM evolucion_reglas er
            JOIN pokemon p2 ON p2.id = er.evoluciona_a
            LEFT JOIN items i ON i.id = er.item_id
            WHERE er.pokemon_id = %s
              AND er.activo = TRUE
              AND er.metodo = 'item'
            ORDER BY er.prioridad ASC, p2.nombre ASC
        """, (pokemon_id,))
        rows_item = cursor.fetchall()

        if rows_item:
            return [
                {
                    "id": row["id"],
                    "tipo_metodo": row["metodo"],
                    "evolucion_id": row["evolucion_id"],
                    "evolucion_nombre": row["evolucion_nombre"],
                    "nivel": row["nivel_requerido"],
                    "item_id": row["item_id"],
                    "item_nombre": row["item_nombre"],
                    "prioridad": row["prioridad"],
                    "activo": bool(row["activo"]),
                }
                for row in rows_item
            ]

        cursor.execute("""
            SELECT
                er.id,
                er.metodo,
                er.nivel_requerido,
                er.item_id,
                er.prioridad,
                er.activo,
                p2.id AS evolucion_id,
                p2.nombre AS evolucion_nombre,
                i.nombre AS item_nombre
            FROM evolucion_reglas er
            JOIN pokemon p2 ON p2.id = er.evoluciona_a
            LEFT JOIN items i ON i.id = er.item_id
            WHERE er.pokemon_id = %s
              AND er.activo = TRUE
              AND er.metodo = 'nivel'
            ORDER BY er.prioridad ASC, p2.nombre ASC
        """, (pokemon_id,))
        rows_nivel = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "tipo_metodo": row["metodo"],
                "evolucion_id": row["evolucion_id"],
                "evolucion_nombre": row["evolucion_nombre"],
                "nivel": row["nivel_requerido"],
                "item_id": row["item_id"],
                "item_nombre": row["item_nombre"],
                "prioridad": row["prioridad"],
                "activo": bool(row["activo"]),
            }
            for row in rows_nivel
        ]
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# USUARIO COMPATIBILIDAD
# =========================================================

@router.get("/usuario/{usuario_id}")
def obtener_usuario(usuario_id: int):
    usuario = obtener_usuario_por_id(usuario_id)
    if not usuario:
        return {"mensaje": "Usuario no encontrado"}
    return usuario


@router.get("/usuario/{usuario_id}/pokemon")
def obtener_pokemon_usuario(usuario_id: int):
    return obtener_pokemon_usuario_data(usuario_id)


@router.get("/usuario/{usuario_id}/items")
def obtener_items_usuario(usuario_id: int):
    return obtener_items_usuario_data(usuario_id)


@router.post("/usuario/capturar")
def capturar_pokemon(payload: CapturaPayload, usuario=Depends(get_current_user)):
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Ruta deshabilitada por seguridad. Usa /maps/intentar-captura."
    )


@router.post("/usuario/ganar-exp")
def ganar_experiencia(payload: ExpPayload, usuario=Depends(get_current_user)):
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Ruta deshabilitada por seguridad. La EXP ahora se otorga desde rutas controladas."
    )


@router.post("/battle/iniciar")
def iniciar_batalla_arena(payload: BattleIniciarPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        if not existe_tabla(cursor, "battle_sesiones"):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="La tabla battle_sesiones aún no existe. Ejecuta la migración primero."
            )

        recompensa = obtener_recompensa_batalla_por_codigo(payload.dificultad)

        usuario_pokemon_ids = payload.usuario_pokemon_ids or []
        if usuario_pokemon_ids:
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], usuario_pokemon_ids)
            if payload.guardar_equipo:
                guardar_equipo_usuario(cursor, usuario["id"], ids_limpios)
        else:
            ids_limpios = obtener_equipo_guardado_ids(cursor, usuario["id"])
            if not ids_limpios:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No tienes un equipo guardado para batalla"
                )
            ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], ids_limpios)

        invalidar_sesiones_battle_activas(cursor, usuario["id"])

        battle_token = crear_token_simple(24)
        cursor.execute(
            """
            INSERT INTO battle_sesiones
                (token, usuario_id, dificultad_codigo, bonus_nivel_rival, exp_ganada, pokedolares_ganados,
                 equipo_usuario_pokemon_ids, activo, resultado, creado_en, expira_en)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s::jsonb, TRUE, 'pendiente', NOW(), NOW() + (%s * INTERVAL '1 second'))
            RETURNING id, creado_en, expira_en
            """,
            (
                battle_token,
                usuario["id"],
                recompensa["codigo"],
                int(recompensa.get("bonus_nivel_rival") or 0),
                int(recompensa["exp"]),
                int(recompensa["pokedolares"]),
                json.dumps(ids_limpios),
                int(BATTLE_SESSION_TTL_SEGUNDOS),
            )
        )
        sesion = cursor.fetchone()

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle-arena",
            accion="battle_start",
            detalle=f"dificultad:{recompensa['codigo']}"
        )
        conn.commit()

        return {
            "ok": True,
            "battle_session_token": battle_token,
            "dificultad_codigo": recompensa["codigo"],
            "bonus_nivel_rival": int(recompensa.get("bonus_nivel_rival") or 0),
            "exp_ganada": int(recompensa["exp"]),
            "pokedolares_ganados": int(recompensa["pokedolares"]),
            "equipo_usuario_pokemon_ids": ids_limpios,
            "creado_en": sesion["creado_en"].isoformat() if sesion and sesion.get("creado_en") else None,
            "expira_en": sesion["expira_en"].isoformat() if sesion and sesion.get("expira_en") else None,
            "ttl_segundos": BATTLE_SESSION_TTL_SEGUNDOS,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/iniciar:", error)
        return {"ok": False, "mensaje": "No se pudo iniciar la sesión de batalla"}
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/battle/recompensa-victoria")
def recompensa_victoria_batalla(payload: RecompensaBatallaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_cooldown_recompensa_batalla(usuario["id"])

        if not payload.battle_session_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="battle_session_token es obligatorio"
            )

        if not existe_tabla(cursor, "battle_sesiones"):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="La tabla battle_sesiones aún no existe. Ejecuta la migración primero."
            )

        sesion = obtener_sesion_battle_por_token(cursor, payload.battle_session_token, usuario["id"], for_update=True)
        if not sesion:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="La sesión de batalla no existe, expiró o ya no está activa"
            )

        if sesion.get("reclamado_en") is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="La recompensa de esta batalla ya fue reclamada"
            )

        creado_en = sesion.get("creado_en")
        if creado_en and (datetime.now() - creado_en).total_seconds() < BATTLE_SESSION_MINIMA_SEGUNDOS:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="La batalla todavía es demasiado reciente para reclamar recompensa"
            )

        exp_ganada = int(sesion["exp_ganada"] or 0)
        pokedolares_ganados = int(sesion["pokedolares_ganados"] or 0)
        recompensa = obtener_recompensa_batalla_permitida(exp_ganada, pokedolares_ganados)
        ids_limpios = validar_ids_pokemon_equipo(cursor, usuario["id"], normalizar_equipo_ids_json(sesion.get("equipo_usuario_pokemon_ids")))

        cursor.execute("""
            SELECT id, pokedolares
            FROM usuarios
            WHERE id = %s
            FOR UPDATE
        """, (usuario["id"],))
        usuario_db = cursor.fetchone()

        if not usuario_db:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Usuario no encontrado"
            )

        nuevos_pokedolares = int(usuario_db["pokedolares"] or 0) + pokedolares_ganados
        cursor.execute("""
            UPDATE usuarios
            SET pokedolares = %s
            WHERE id = %s
        """, (nuevos_pokedolares, usuario["id"]))

        p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
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

        cursor.execute("""
            UPDATE battle_sesiones
            SET activo = FALSE,
                resultado = 'reclamada',
                finalizado_en = COALESCE(finalizado_en, NOW()),
                reclamado_en = NOW()
            WHERE id = %s
        """, (sesion["id"],))

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="battle-arena",
            accion="battle_end",
            detalle=f"recompensa:{recompensa['codigo']}"
        )
        conn.commit()
        registrar_cooldown_recompensa_batalla(usuario["id"])

        return {
            "ok": True,
            "mensaje": "Recompensas aplicadas correctamente",
            "usuario_id": usuario["id"],
            "battle_session_token": payload.battle_session_token,
            "recompensa_codigo": recompensa["codigo"],
            "pokedolares_ganados": pokedolares_ganados,
            "pokedolares_actuales": nuevos_pokedolares,
            "exp_ganada": exp_ganada,
            "pokemon_actualizados": pokemon_actualizados
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /battle/recompensa-victoria:", error)
        return {
            "ok": False,
            "mensaje": "No se pudieron aplicar las recompensas de batalla"
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/usuario/soltar-pokemon")
def soltar_pokemon(payload: SoltarPokemonPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        cursor.execute("""
            SELECT id
            FROM usuario_pokemon
            WHERE id = %s AND usuario_id = %s
        """, (payload.usuario_pokemon_id, usuario["id"]))
        pokemon = cursor.fetchone()

        if not pokemon:
            return {"mensaje": "Pokémon no encontrado en tu colección"}

        cursor.execute("""
            DELETE FROM usuario_pokemon
            WHERE id = %s AND usuario_id = %s
        """, (payload.usuario_pokemon_id, usuario["id"]))
        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="mypokemon",
            accion="view",
            detalle=f"soltar:{payload.usuario_pokemon_id}"
        )
        conn.commit()

        return {
            "ok": True,
            "mensaje": "Pokémon liberado correctamente",
            "usuario_pokemon_id": payload.usuario_pokemon_id
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /usuario/soltar-pokemon:", error)
        return {
            "ok": False,
            "mensaje": "No se pudo liberar el Pokémon"
        }
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# ZONAS / MAPS
# =========================================================

@router.get("/zonas")
def obtener_zonas():
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT
                z.id AS zona_id,
                z.nombre AS zona_nombre,
                z.descripcion,
                z.tipo_ambiente,
                z.nivel_min,
                z.nivel_max,
                z.imagen,
                zp.pokemon_id,
                p.nombre AS pokemon_nombre,
                zp.probabilidad,
                zp.nivel_min AS pokemon_nivel_min,
                zp.nivel_max AS pokemon_nivel_max,
                zp.puede_ser_shiny
            FROM zonas z
            LEFT JOIN zona_pokemon zp ON zp.zona_id = z.id
            LEFT JOIN pokemon p ON p.id = zp.pokemon_id
            ORDER BY z.id, zp.probabilidad DESC, p.nombre ASC
        """)
        rows = cursor.fetchall()

        zonas_map = {}

        for row in rows:
            zona_id = row["zona_id"]

            if zona_id not in zonas_map:
                zonas_map[zona_id] = {
                    "id": row["zona_id"],
                    "nombre": row["zona_nombre"],
                    "descripcion": row["descripcion"],
                    "tipo_ambiente": row["tipo_ambiente"],
                    "nivel_min": row["nivel_min"],
                    "nivel_max": row["nivel_max"],
                    "imagen": row["imagen"],
                    "pokemones": []
                }

            if row["pokemon_id"] is not None:
                zonas_map[zona_id]["pokemones"].append({
                    "pokemon_id": row["pokemon_id"],
                    "nombre": row["pokemon_nombre"],
                    "probabilidad": float(row["probabilidad"]),
                    "nivel_min": row["pokemon_nivel_min"],
                    "nivel_max": row["pokemon_nivel_max"],
                    "puede_ser_shiny": bool(row["puede_ser_shiny"])
                })

        return list(zonas_map.values())

    except Exception as error:
        print("Error en /zonas:", error)
        raise HTTPException(
            status_code=500,
            detail="No se pudieron obtener las zonas"
        )
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/maps/presencia/{zona_id}")
def obtener_presencia_maps(zona_id: int, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_zona_existente(cursor, zona_id)
    finally:
        cursor.close()
        release_connection(conn)

    jugadores = obtener_jugadores_presencia_zona_data(zona_id, excluir_usuario_id=usuario["id"])

    return {
        "ok": True,
        "zona_id": int(zona_id),
        "ttl_segundos": MAPS_PRESENCIA_TTL_SEGUNDOS,
        "jugadores": jugadores
    }


@router.post("/maps/presencia")
def actualizar_presencia_maps(payload: PresenciaMapaPayload, usuario=Depends(get_current_user)):
    zona_id = int(payload.zona_id or 0)
    nodo_id = validar_nodo_mapa_id(payload.nodo_id)

    if zona_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="zona_id inválida"
        )

    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_zona_existente(cursor, zona_id)
        limpiar_presencia_expirada(cursor)
        updated_at = registrar_presencia_usuario(cursor, usuario["id"], zona_id, nodo_id)
        conn.commit()

        return {
            "ok": True,
            "jugador": construir_payload_presencia_usuario(usuario, zona_id, nodo_id, updated_at)
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)


@router.delete("/maps/presencia")
def eliminar_presencia_maps(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        eliminar_presencia_usuario(cursor, usuario["id"])
        conn.commit()
        return {
            "ok": True,
            "mensaje": "Presencia eliminada"
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)


@router.websocket("/ws/maps")
async def websocket_maps(websocket: WebSocket):
    token = obtener_token_desde_websocket(websocket)

    try:
        usuario = get_current_user_from_token(token)
    except HTTPException:
        await websocket.close(code=4401)
        return

    await websocket.accept()

    zona_actual = None
    connection_id = None

    try:
        await websocket.send_json({
            "type": "connected",
            "usuario_id": int(usuario["id"]),
            "ttl_segundos": MAPS_PRESENCIA_TTL_SEGUNDOS
        })

        while True:
            data = await websocket.receive_json()
            action = str(data.get("action") or data.get("type") or "").strip().lower()

            if action == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            if action == "join":
                zona_id = int(data.get("zona_id") or 0)
                nodo_id = validar_nodo_mapa_id(data.get("nodo_id"))

                if zona_id <= 0:
                    await websocket.send_json({
                        "type": "error",
                        "message": "zona_id inválida"
                    })
                    continue

                conn = get_connection()
                cursor = get_cursor(conn)

                try:
                    validar_zona_existente(cursor, zona_id)
                    limpiar_presencia_expirada(cursor)

                    presencia_anterior = obtener_presencia_usuario_actual(cursor, usuario["id"])
                    updated_at = registrar_presencia_usuario(cursor, usuario["id"], zona_id, nodo_id)
                    conn.commit()
                except Exception as error:
                    conn.rollback()
                    await websocket.send_json({
                        "type": "error",
                        "message": str(error)
                    })
                    cursor.close()
                    release_connection(conn)
                    continue
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    try:
                        release_connection(conn)
                    except Exception:
                        pass

                if zona_actual is not None and connection_id is not None and zona_actual != zona_id:
                    await maps_ws_quitar(zona_actual, connection_id)

                zona_actual = zona_id

                if connection_id is None:
                    connection_id = await maps_ws_registrar(zona_actual, int(usuario["id"]), websocket)
                else:
                    await maps_ws_registrar(zona_actual, int(usuario["id"]), websocket)

                otras_conexiones = await maps_ws_desregistrar_otras_conexiones_usuario(
                    int(usuario["id"]),
                    excluir_connection_id=connection_id
                )

                for item in otras_conexiones:
                    ws_anterior = item.get("websocket")
                    if not ws_anterior:
                        continue
                    try:
                        await ws_anterior.close(code=4409)
                    except Exception:
                        pass

                if presencia_anterior and int(presencia_anterior["zona_id"]) != int(zona_id):
                    await maps_ws_broadcast_zona(
                        int(presencia_anterior["zona_id"]),
                        {
                            "type": "remove",
                            "usuario_id": int(usuario["id"])
                        },
                        excluir_connection_id=None
                    )

                snapshot = obtener_jugadores_presencia_zona_data(zona_actual, excluir_usuario_id=usuario["id"])
                jugador_payload = construir_payload_presencia_usuario(usuario, zona_actual, nodo_id, updated_at)

                await websocket.send_json({
                    "type": "snapshot",
                    "zona_id": int(zona_actual),
                    "jugadores": snapshot,
                    "self": jugador_payload
                })

                await maps_ws_broadcast_zona(
                    zona_actual,
                    {
                        "type": "upsert",
                        "jugador": jugador_payload
                    },
                    excluir_connection_id=connection_id
                )
                continue

            if action == "move":
                if zona_actual is None:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Debes enviar join antes de move"
                    })
                    continue

                nodo_id = validar_nodo_mapa_id(data.get("nodo_id"))

                conn = get_connection()
                cursor = get_cursor(conn)

                try:
                    limpiar_presencia_expirada(cursor)
                    updated_at = registrar_presencia_usuario(cursor, usuario["id"], zona_actual, nodo_id)
                    conn.commit()
                except Exception as error:
                    conn.rollback()
                    await websocket.send_json({
                        "type": "error",
                        "message": str(error)
                    })
                    cursor.close()
                    release_connection(conn)
                    continue
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    try:
                        release_connection(conn)
                    except Exception:
                        pass

                jugador_payload = construir_payload_presencia_usuario(usuario, zona_actual, nodo_id, updated_at)

                await maps_ws_broadcast_zona(
                    zona_actual,
                    {
                        "type": "upsert",
                        "jugador": jugador_payload
                    },
                    excluir_connection_id=connection_id
                )

                await websocket.send_json({
                    "type": "ack",
                    "action": "move",
                    "self": jugador_payload
                })
                continue

            if action == "leave":
                conexion_activa = await maps_ws_es_conexion_activa(zona_actual, connection_id)

                if conexion_activa and zona_actual is not None:
                    conn = get_connection()
                    cursor = get_cursor(conn)
                    try:
                        eliminar_presencia_usuario(cursor, usuario["id"])
                        conn.commit()
                    except Exception:
                        conn.rollback()
                    finally:
                        cursor.close()
                        release_connection(conn)

                    await maps_ws_broadcast_zona(
                        zona_actual,
                        {
                            "type": "remove",
                            "usuario_id": int(usuario["id"])
                        },
                        excluir_connection_id=connection_id
                    )

                await maps_ws_quitar(zona_actual, connection_id)
                zona_actual = None
                connection_id = None

                await websocket.send_json({"type": "left"})
                continue

            await websocket.send_json({
                "type": "error",
                "message": "Acción no soportada"
            })

    except WebSocketDisconnect:
        pass
    except Exception as error:
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(error)
            })
        except Exception:
            pass
    finally:
        conexion_activa = await maps_ws_es_conexion_activa(zona_actual, connection_id)

        if conexion_activa and zona_actual is not None:
            conn = get_connection()
            cursor = get_cursor(conn)
            try:
                eliminar_presencia_usuario(cursor, usuario["id"])
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                cursor.close()
                release_connection(conn)

            await maps_ws_broadcast_zona(
                zona_actual,
                {
                    "type": "remove",
                    "usuario_id": int(usuario["id"])
                },
                excluir_connection_id=connection_id
            )

        await maps_ws_quitar(zona_actual, connection_id)


@router.post("/maps/encuentro")
def generar_encuentro(request: Request, payload: EncuentroPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        bloqueo_rate = validar_rate_limit_maps(cursor, usuario["id"], "encuentro")
        if bloqueo_rate:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="encuentro",
                request=request,
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                motivo=bloqueo_rate,
                zona_id=int(payload.zona_id),
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=bloqueo_rate,
            )

        if not existe_tabla(cursor, "mapa_encuentros"):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="La tabla mapa_encuentros aún no existe. Ejecuta la migración primero."
            )

        p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
        cursor.execute(f"""
            SELECT 
                zp.pokemon_id,
                zp.probabilidad,
                zp.nivel_min,
                zp.nivel_max,
                zp.puede_ser_shiny,
                p.nombre,
                p.tipo,
                p.imagen,
                p.ataque,
                p.defensa,
                p.hp,
                p.velocidad,
                p.rareza,
                p.generacion,
                p.tiene_mega,
                {p_atk_sp_sql},
                {p_def_sp_sql}
            FROM zona_pokemon zp
            INNER JOIN pokemon p ON p.id = zp.pokemon_id
            WHERE zp.zona_id = %s
        """, (payload.zona_id,))
        rows = cursor.fetchall()

        if not rows:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="encuentro",
                request=request,
                status_code=status.HTTP_404_NOT_FOUND,
                motivo="No hay Pokémon configurados para esta zona",
                zona_id=int(payload.zona_id),
            )
            conn.commit()
            return {"error": "No hay Pokémon configurados para esta zona"}

        pesos = [float(r["probabilidad"] or 0) for r in rows]
        elegido = random.choices(rows, weights=pesos, k=1)[0] if sum(pesos) > 0 else rows[0]

        nivel_min = int(elegido["nivel_min"] or 1)
        nivel_max = int(elegido["nivel_max"] or nivel_min)
        nivel = random.randint(nivel_min, nivel_max)
        es_shiny = bool(elegido["puede_ser_shiny"]) and (random.randint(1, 10) == 1)

        stats = calcular_stats(
            int(elegido["hp"] or 1),
            int(elegido["ataque"] or 1),
            int(elegido["defensa"] or 1),
            int(elegido["velocidad"] or 1),
            nivel,
            es_shiny,
            0,
            0,
            0,
            0,
            int(elegido.get("ataque_especial") or elegido["ataque"] or 1),
            int(elegido.get("defensa_especial") or elegido["defensa"] or 1),
        )

        invalidar_encuentros_activos_usuario(cursor, usuario["id"])
        encuentro_token = crear_token_simple(24)
        cursor.execute(
            """
            INSERT INTO mapa_encuentros
                (token, usuario_id, zona_id, pokemon_id, nivel, es_shiny, hp_max, ataque, defensa, velocidad,
                 activo, creado_en, expira_en)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW() + (%s * INTERVAL '1 second'))
            RETURNING id, creado_en, expira_en
            """,
            (
                encuentro_token,
                usuario["id"],
                int(payload.zona_id),
                int(elegido["pokemon_id"]),
                int(nivel),
                bool(es_shiny),
                int(stats["hp_max"]),
                int(stats["ataque"]),
                int(stats["defensa"]),
                int(stats["velocidad"]),
                int(ENCUENTRO_TOKEN_TTL_SEGUNDOS),
            )
        )
        encuentro_row = cursor.fetchone()

        registrar_log_maps(
            cursor,
            usuario_id=usuario["id"],
            endpoint="encuentro",
            request=request,
            status_code=status.HTTP_200_OK,
            token=encuentro_token,
            zona_id=int(payload.zona_id),
            encuentro_id=int(encuentro_row["id"]) if encuentro_row and encuentro_row.get("id") else None,
        )
        conn.commit()

        return {
            "pokemon_id": int(elegido["pokemon_id"]),
            "nombre": elegido["nombre"],
            "tipo": elegido["tipo"],
            "imagen": elegido["imagen"],
            "rareza": elegido["rareza"],
            "generacion": elegido["generacion"],
            "tiene_mega": bool(elegido["tiene_mega"]),
            "ataque": stats["ataque"],
            "defensa": stats["defensa"],
            "hp": stats["hp_max"],
            "hp_max": stats["hp_max"],
            "velocidad": stats["velocidad"],
            "ataque_especial": stats["ataque_especial"],
            "defensa_especial": stats["defensa_especial"],
            "nivel": nivel,
            "es_shiny": es_shiny,
            "encuentro_token": encuentro_token,
            "encuentro_ttl_segundos": ENCUENTRO_TOKEN_TTL_SEGUNDOS,
            "expira_en": encuentro_row["expira_en"].isoformat() if encuentro_row and encuentro_row.get("expira_en") else None,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /maps/encuentro:", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo generar el encuentro"
        )
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/maps/intentar-captura")
def intentar_captura(request: Request, payload: IntentoCapturaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        bloqueo_rate = validar_rate_limit_maps(cursor, usuario["id"], "intentar-captura")
        if bloqueo_rate:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                motivo=bloqueo_rate,
                token=payload.encuentro_token,
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=bloqueo_rate,
            )

        if not payload.encuentro_token:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="encuentro_token es obligatorio",
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="encuentro_token es obligatorio"
            )

        if not existe_tabla(cursor, "mapa_encuentros"):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="La tabla mapa_encuentros aún no existe. Ejecuta la migración primero."
            )

        encuentro = obtener_encuentro_activo_por_token(
            cursor,
            payload.encuentro_token,
            usuario["id"],
            for_update=True
        )
        if not encuentro:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="Encuentro inválido, expirado o ya no activo",
                token=payload.encuentro_token,
            )
            conn.commit()
            return {
                "capturado": False,
                "mensaje": "Este encuentro expiró o ya no es válido"
            }

        encuentro_id = int(encuentro["id"])

        if int(payload.pokemon_id or 0) != int(encuentro["pokemon_id"]):
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="pokemon_id no coincide con el encuentro",
                token=payload.encuentro_token,
                zona_id=int(encuentro["zona_id"]),
                encuentro_id=encuentro_id,
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El Pokémon del encuentro no coincide con el enviado"
            )

        if int(payload.nivel or 0) != int(encuentro["nivel"]):
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="nivel no coincide con el encuentro",
                token=payload.encuentro_token,
                zona_id=int(encuentro["zona_id"]),
                encuentro_id=encuentro_id,
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El nivel del encuentro no coincide con el enviado"
            )

        if bool(payload.es_shiny) != bool(encuentro["es_shiny"]):
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="es_shiny no coincide con el encuentro",
                token=payload.encuentro_token,
                zona_id=int(encuentro["zona_id"]),
                encuentro_id=encuentro_id,
            )
            conn.commit()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="La variante shiny del encuentro no coincide con la enviada"
            )

        cursor.execute(
            """
            SELECT ui.id, ui.item_id, ui.cantidad, i.nombre, i.bonus_captura
            FROM usuario_items ui
            INNER JOIN items i ON i.id = ui.item_id
            WHERE ui.usuario_id = %s
              AND ui.item_id = %s
            FOR UPDATE
            """,
            (usuario["id"], payload.item_id)
        )
        item_usuario = cursor.fetchone()

        if not item_usuario or int(item_usuario["cantidad"] or 0) <= 0:
            registrar_log_maps(
                cursor,
                usuario_id=usuario["id"],
                endpoint="intentar-captura",
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                motivo="Item inexistente o sin stock",
                token=payload.encuentro_token,
                zona_id=int(encuentro["zona_id"]),
                encuentro_id=encuentro_id,
            )
            conn.commit()
            return {
                "capturado": False,
                "mensaje": "No tienes ese item disponible"
            }

        bonus_captura = int(item_usuario["bonus_captura"] or 0)
        nombre_item = str(item_usuario["nombre"] or "").strip()

        if nombre_item == "Poke Ball":
            probabilidad = 50
        elif nombre_item == "Super Ball":
            probabilidad = 65
        elif nombre_item == "Ultra Ball":
            probabilidad = 80
        elif nombre_item == "Master Ball":
            probabilidad = 100
        else:
            probabilidad = 35 + bonus_captura

        if bool(encuentro["es_shiny"]) and nombre_item.lower() != "master ball":
            probabilidad -= 10

        probabilidad = max(1, min(100, probabilidad))
        capturado = random.randint(1, 100) <= probabilidad

        cursor.execute(
            """
            UPDATE usuario_items
            SET cantidad = cantidad - 1
            WHERE id = %s
            """,
            (item_usuario["id"],)
        )

        if capturado:
            cursor.execute(
                """
                UPDATE mapa_encuentros
                SET activo = FALSE,
                    consumido_en = NOW()
                WHERE id = %s
                  AND activo = TRUE
                  AND consumido_en IS NULL
                """,
                (encuentro_id,)
            )

            if cursor.rowcount != 1:
                registrar_log_maps(
                    cursor,
                    usuario_id=usuario["id"],
                    endpoint="intentar-captura",
                    request=request,
                    status_code=status.HTTP_409_CONFLICT,
                    motivo="El encuentro ya fue consumido por otra solicitud",
                    token=payload.encuentro_token,
                    zona_id=int(encuentro["zona_id"]),
                    encuentro_id=encuentro_id,
                )
                conn.commit()
                return {
                    "capturado": False,
                    "mensaje": "Este encuentro ya fue utilizado en otra solicitud"
                }

            cursor.execute(
                """
                SELECT nombre
                FROM pokemon
                WHERE id = %s
                LIMIT 1
                """,
                (int(encuentro["pokemon_id"]),)
            )
            pokemon_db = cursor.fetchone()
            nombre_pokemon = pokemon_db["nombre"] if pokemon_db and pokemon_db.get("nombre") else f"Pokémon #{int(encuentro['pokemon_id'])}"

            cursor.execute(
                """
                INSERT INTO usuario_pokemon (
                    usuario_id, pokemon_id, nivel, experiencia, experiencia_total, es_shiny,
                    hp_actual, hp_max, ataque, defensa, velocidad,
                    ataque_especial, defensa_especial,
                    bonus_hp_renacer, bonus_ataque_renacer, bonus_defensa_renacer, bonus_velocidad_renacer,
                    victorias_total
                )
                VALUES (%s, %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0)
                RETURNING id
                """,
                (
                    usuario["id"],
                    int(encuentro["pokemon_id"]),
                    int(encuentro["nivel"]),
                    calcular_experiencia_total_base(int(encuentro["nivel"]), 0),
                    bool(encuentro["es_shiny"]),
                    int(encuentro["hp_max"]),
                    int(encuentro["hp_max"]),
                    int(encuentro["ataque"]),
                    int(encuentro["defensa"]),
                    int(encuentro["velocidad"]),
                    int(encuentro.get("ataque_especial") or encuentro["ataque"]),
                    int(encuentro.get("defensa_especial") or encuentro["defensa"]),
                )
            )
            usuario_pokemon_insertado = cursor.fetchone()
            usuario_pokemon_id = int(usuario_pokemon_insertado["id"]) if usuario_pokemon_insertado else None

            cursor.execute("""
                SELECT
                    current_database() AS db_name,
                    current_schema() AS schema_name,
                    txid_current() AS txid
            """)
            debug_ctx = cursor.fetchone()

            debug_row_precommit = None
            if usuario_pokemon_id:
                cursor.execute("""
                    SELECT id, usuario_id, pokemon_id, nivel
                    FROM usuario_pokemon
                    WHERE id = %s
                    LIMIT 1
                """, (usuario_pokemon_id,))
                debug_row_precommit = cursor.fetchone()

            print("[CAPTURE DEBUG][PRE-COMMIT]", {
                "marker": CAPTURE_DEBUG_MARKER,
                "usuario_pokemon_id": usuario_pokemon_id,
                "db_name": debug_ctx["db_name"] if debug_ctx else None,
                "schema_name": debug_ctx["schema_name"] if debug_ctx else None,
                "txid": debug_ctx["txid"] if debug_ctx else None,
                "debug_row_exists_precommit": bool(debug_row_precommit),
                "debug_row_precommit": dict(debug_row_precommit) if debug_row_precommit else None,
            })

            # DEBUG TEMPORAL: desactivado para aislar problema
            registrar_log_maps(
                 cursor,
                 usuario_id=usuario["id"],
                 endpoint="intentar-captura",
                 request=request,
                 status_code=status.HTTP_200_OK,
                 motivo="capturado",
                 token=payload.encuentro_token,
                 zona_id=int(encuentro["zona_id"]),
                 encuentro_id=encuentro_id,
             )

            # DEBUG TEMPORAL: desactivado para aislar problema
            # try:
            #     registrar_actividad_usuario(
            #         cursor,
            #         usuario["id"],
            #         pagina="maps",
            #         accion="capture",
            #         detalle=f"pokemon:{int(encuentro['pokemon_id'])}"
            #     )
            # except Exception as actividad_error:
            #     print("Aviso actividad capture maps:", actividad_error)

            conn.commit()

            debug_row_postcommit = None
            if usuario_pokemon_id:
                cursor.execute("""
                    SELECT id, usuario_id, pokemon_id, nivel
                    FROM usuario_pokemon
                    WHERE id = %s
                    LIMIT 1
                """, (usuario_pokemon_id,))
                debug_row_postcommit = cursor.fetchone()

            print("[CAPTURE DEBUG][POST-COMMIT]", {
                "marker": CAPTURE_DEBUG_MARKER,
                "usuario_pokemon_id": usuario_pokemon_id,
                "db_name": debug_ctx["db_name"] if debug_ctx else None,
                "schema_name": debug_ctx["schema_name"] if debug_ctx else None,
                "txid": debug_ctx["txid"] if debug_ctx else None,
                "debug_row_exists_postcommit": bool(debug_row_postcommit),
                "debug_row_postcommit": dict(debug_row_postcommit) if debug_row_postcommit else None,
            })

            return {
                "capturado": True,
                "mensaje": f"¡Has capturado a {nombre_pokemon} con {nombre_item}!",
                "probabilidad": probabilidad,
                "usuario_pokemon_id": usuario_pokemon_id,
                "item_id": int(payload.item_id),
                "debug_marker": CAPTURE_DEBUG_MARKER,
                "debug_db_name": debug_ctx["db_name"] if debug_ctx else None,
                "debug_schema_name": debug_ctx["schema_name"] if debug_ctx else None,
                "debug_txid": debug_ctx["txid"] if debug_ctx else None,
                "debug_row_exists_precommit": bool(debug_row_precommit),
                "debug_row_exists_postcommit": bool(debug_row_postcommit),
            }

        registrar_log_maps(
            cursor,
            usuario_id=usuario["id"],
            endpoint="intentar-captura",
            request=request,
            status_code=status.HTTP_200_OK,
            motivo="fallo-captura",
            token=payload.encuentro_token,
            zona_id=int(encuentro["zona_id"]),
            encuentro_id=encuentro_id,
        )
        conn.commit()

        return {
            "capturado": False,
            "mensaje": f"{nombre_item} usada, pero {encuentro.get('nombre') or 'el Pokémon'} escapó.",
            "probabilidad": probabilidad,
            "item_id": int(payload.item_id),
            "encuentro_sigue_activo": True,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /maps/intentar-captura:", repr(error))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo procesar el intento de captura"
        )
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/tienda/items")
def obtener_items_tienda():
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT id, nombre, tipo, descripcion, bonus_captura, cura_hp, precio
            FROM items
            ORDER BY tipo, precio
        """)
        rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "nombre": row["nombre"],
                "tipo": row["tipo"],
                "descripcion": row["descripcion"],
                "bonus_captura": row["bonus_captura"],
                "cura_hp": row["cura_hp"],
                "precio": row["precio"]
            }
            for row in rows
        ]
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/tienda/comprar")
def comprar_item(payload: CompraItemPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        cantidad = int(payload.cantidad or 0)
        if cantidad <= 0:
            return {"ok": False, "mensaje": "La cantidad debe ser mayor a 0"}

        if cantidad > 999:
            return {"ok": False, "mensaje": "La cantidad máxima por compra es 999"}

        cursor.execute("""
            SELECT id, nombre, precio
            FROM items
            WHERE id = %s
        """, (payload.item_id,))
        item = cursor.fetchone()

        if not item:
            return {"ok": False, "mensaje": "Item no encontrado"}

        cursor.execute("""
            SELECT id, pokedolares
            FROM usuarios
            WHERE id = %s
        """, (usuario["id"],))
        usuario_db = cursor.fetchone()

        if not usuario_db:
            return {"ok": False, "mensaje": "Usuario no encontrado"}

        total = int(item["precio"] or 0) * cantidad

        if int(usuario_db["pokedolares"] or 0) < total:
            return {"ok": False, "mensaje": "No tienes suficientes pokedólares"}

        nuevo_saldo = int(usuario_db["pokedolares"] or 0) - total

        cursor.execute("""
            UPDATE usuarios
            SET pokedolares = %s
            WHERE id = %s
        """, (nuevo_saldo, usuario["id"]))

        cursor.execute("""
            INSERT INTO usuario_items (usuario_id, item_id, cantidad)
            VALUES (%s, %s, %s)
            ON CONFLICT (usuario_id, item_id)
            DO UPDATE SET cantidad = usuario_items.cantidad + EXCLUDED.cantidad
            RETURNING cantidad
        """, (usuario["id"], payload.item_id, cantidad))

        cantidad_actual = int(cursor.fetchone()["cantidad"] or 0)

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="pokemart",
            accion="shop",
            detalle=f"item:{item['id']}x{cantidad}"
        )
        conn.commit()

        return {
            "ok": True,
            "mensaje": f"Compraste {cantidad} x {item['nombre']}",
            "item_id": item["id"],
            "item_nombre": item["nombre"],
            "cantidad_comprada": cantidad,
            "cantidad_actual": cantidad_actual,
            "pokedolares_actual": nuevo_saldo,
            "usuario_id": usuario["id"]
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /tienda/comprar:", error)
        return {
            "ok": False,
            "mensaje": "No se pudo completar la compra."
        }
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# EVOLUCIONES
# =========================================================

@router.post("/pokemon/evolucionar-item")
def evolucionar_con_item(payload: EvolucionItemPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        cursor.execute("""
            SELECT
                pokemon_id,
                nivel,
                experiencia,
                es_shiny,
                bonus_hp_renacer,
                bonus_ataque_renacer,
                bonus_defensa_renacer,
                bonus_velocidad_renacer
            FROM usuario_pokemon
            WHERE id = %s AND usuario_id = %s
        """, (payload.usuario_pokemon_id, usuario["id"]))
        poke = cursor.fetchone()

        if not poke:
            return {"ok": False, "mensaje": "Pokémon del usuario no encontrado"}

        cursor.execute("""
            SELECT evoluciona_a
            FROM evoluciones_item
            WHERE pokemon_id = %s AND item_id = %s
        """, (poke["pokemon_id"], payload.item_id))
        evo = cursor.fetchone()

        if not evo:
            return {"ok": False, "mensaje": "Ese Pokémon no evoluciona con este item"}

        cursor.execute("""
            SELECT id, cantidad
            FROM usuario_items
            WHERE usuario_id = %s AND item_id = %s
        """, (usuario["id"], payload.item_id))
        item_user = cursor.fetchone()

        if not item_user or int(item_user["cantidad"] or 0) <= 0:
            return {"ok": False, "mensaje": "No tienes ese item"}

        cursor.execute("""
            UPDATE usuario_items
            SET cantidad = cantidad - 1
            WHERE id = %s
        """, (item_user["id"],))

        p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
        cursor.execute(f"""
            SELECT hp, ataque, defensa, velocidad, nombre, {p_atk_sp_sql}, {p_def_sp_sql}
            FROM pokemon p
            WHERE id = %s
        """, (evo["evoluciona_a"],))
        base = cursor.fetchone()

        if not base:
            return {"ok": False, "mensaje": "La evolución configurada no existe"}

        stats = calcular_stats(
            base["hp"],
            base["ataque"],
            base["defensa"],
            base["velocidad"],
            poke["nivel"],
            bool(poke["es_shiny"]),
            poke["bonus_hp_renacer"],
            poke["bonus_ataque_renacer"],
            poke["bonus_defensa_renacer"],
            poke["bonus_velocidad_renacer"],
            base.get("ataque_especial", base["ataque"]),
            base.get("defensa_especial", base["defensa"]),
        )

        if stats_especiales_habilitados(cursor, "usuario_pokemon"):
            cursor.execute("""
                UPDATE usuario_pokemon
                SET pokemon_id = %s,
                    hp_actual = %s,
                    hp_max = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s,
                    ataque_especial = %s,
                    defensa_especial = %s
                WHERE id = %s AND usuario_id = %s
            """, (
                evo["evoluciona_a"],
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                stats["ataque_especial"],
                stats["defensa_especial"],
                payload.usuario_pokemon_id,
                usuario["id"]
            ))
        else:
            cursor.execute("""
                UPDATE usuario_pokemon
                SET pokemon_id = %s,
                    hp_actual = %s,
                    hp_max = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s
                WHERE id = %s AND usuario_id = %s
            """, (
                evo["evoluciona_a"],
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                payload.usuario_pokemon_id,
                usuario["id"]
            ))

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="mypokemon",
            accion="evolution",
            detalle=f"item:{evo['evoluciona_a']}"
        )
        conn.commit()
        return {
            "ok": True,
            "mensaje": "¡Tu Pokémon evolucionó con éxito!",
            "usuario_pokemon_id": payload.usuario_pokemon_id,
            "pokemon_id": evo["evoluciona_a"],
            "pokemon_nombre": base["nombre"]
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /pokemon/evolucionar-item:", error)
        return {"ok": False, "mensaje": "No se pudo evolucionar el Pokémon con item"}
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/usuario/pokemon/{usuario_pokemon_id}/evolucion")
def revisar_evolucion(usuario_pokemon_id: int, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT 
                up.id,
                up.usuario_id,
                up.pokemon_id,
                up.nivel,
                p.nombre
            FROM usuario_pokemon up
            JOIN pokemon p ON p.id = up.pokemon_id
            WHERE up.id = %s AND up.usuario_id = %s
        """, (usuario_pokemon_id, usuario["id"]))
        poke = cursor.fetchone()

        if not poke:
            return {
                "puede_evolucionar": False,
                "listo_ahora": False,
                "motivo": "Pokémon no encontrado"
            }

        cursor.execute("""
            SELECT
                er.id,
                er.metodo,
                er.nivel_requerido,
                er.item_id,
                er.prioridad,
                p2.id AS evolucion_id,
                p2.nombre AS evolucion_nombre,
                i.nombre AS item_nombre
            FROM evolucion_reglas er
            JOIN pokemon p2 ON p2.id = er.evoluciona_a
            LEFT JOIN items i ON i.id = er.item_id
            WHERE er.pokemon_id = %s AND er.activo = TRUE
            ORDER BY er.prioridad ASC
        """, (poke["pokemon_id"],))
        reglas = cursor.fetchall()

        if not reglas:
            return {
                "puede_evolucionar": False,
                "listo_ahora": False,
                "motivo": "Este Pokémon no tiene evolución registrada"
            }

        opciones = []
        listo_ahora = False
        tipo_principal = None

        for regla in reglas:
            if regla["metodo"] == "nivel":
                puede = poke["nivel"] >= (regla["nivel_requerido"] or 999)

                if puede:
                    listo_ahora = True

                opciones.append({
                    "tipo": "nivel",
                    "evolucion_id": regla["evolucion_id"],
                    "evoluciona_a": regla["evolucion_id"],
                    "evolucion_nombre": regla["evolucion_nombre"],
                    "nivel_requerido": regla["nivel_requerido"],
                    "item_id": None,
                    "item_nombre": None,
                    "tiene_item": False,
                    "cantidad": 0,
                    "listo": puede
                })

                if tipo_principal is None:
                    tipo_principal = "nivel"

            elif regla["metodo"] == "item":
                cursor.execute("""
                    SELECT cantidad
                    FROM usuario_items
                    WHERE usuario_id = %s AND item_id = %s
                """, (poke["usuario_id"], regla["item_id"]))
                inv = cursor.fetchone()

                cantidad = inv["cantidad"] if inv else 0
                tiene_item = cantidad > 0

                if tiene_item:
                    listo_ahora = True

                opciones.append({
                    "tipo": "item",
                    "evoluciona_a": regla["evolucion_id"],
                    "evolucion_nombre": regla["evolucion_nombre"],
                    "nivel_requerido": None,
                    "item_id": regla["item_id"],
                    "item_nombre": regla["item_nombre"],
                    "tiene_item": tiene_item,
                    "cantidad": cantidad,
                    "listo": tiene_item
                })

                if tipo_principal is None:
                    tipo_principal = "item"

        return {
            "puede_evolucionar": True,
            "listo_ahora": listo_ahora,
            "tipo": tipo_principal,
            "pokemon_actual": poke["nombre"],
            "pokemon_id": poke["pokemon_id"],
            "nivel_actual": poke["nivel"],
            "opciones": opciones
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/pokemon/evolucionar-nivel")
def evolucionar_por_nivel(payload: EvolucionNivelPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        cursor.execute("""
            SELECT
                up.id,
                up.pokemon_id,
                up.nivel,
                up.es_shiny,
                up.bonus_hp_renacer,
                up.bonus_ataque_renacer,
                up.bonus_defensa_renacer,
                up.bonus_velocidad_renacer
            FROM usuario_pokemon up
            WHERE up.id = %s AND up.usuario_id = %s
        """, (payload.usuario_pokemon_id, usuario["id"]))
        poke = cursor.fetchone()

        if not poke:
            return {"ok": False, "mensaje": "Pokémon no encontrado"}

        cursor.execute("""
            SELECT e.evoluciona_a
            FROM evoluciones e
            WHERE e.pokemon_id = %s AND e.nivel <= %s
            ORDER BY e.nivel DESC
            LIMIT 1
        """, (poke["pokemon_id"], poke["nivel"]))
        evo = cursor.fetchone()

        if not evo:
            return {"ok": False, "mensaje": "Este Pokémon aún no puede evolucionar por nivel"}

        p_atk_sp_sql, p_def_sp_sql = obtener_select_stats_especiales(cursor, "pokemon", "p")
        cursor.execute(f"""
            SELECT hp, ataque, defensa, velocidad, nombre, {p_atk_sp_sql}, {p_def_sp_sql}
            FROM pokemon p
            WHERE id = %s
        """, (evo["evoluciona_a"],))
        base = cursor.fetchone()

        if not base:
            return {"ok": False, "mensaje": "La evolución configurada no existe"}

        stats = calcular_stats(
            base["hp"],
            base["ataque"],
            base["defensa"],
            base["velocidad"],
            poke["nivel"],
            bool(poke["es_shiny"]),
            poke["bonus_hp_renacer"],
            poke["bonus_ataque_renacer"],
            poke["bonus_defensa_renacer"],
            poke["bonus_velocidad_renacer"],
            base.get("ataque_especial", base["ataque"]),
            base.get("defensa_especial", base["defensa"]),
        )

        if stats_especiales_habilitados(cursor, "usuario_pokemon"):
            cursor.execute("""
                UPDATE usuario_pokemon
                SET pokemon_id = %s,
                    hp_actual = %s,
                    hp_max = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s,
                    ataque_especial = %s,
                    defensa_especial = %s
                WHERE id = %s AND usuario_id = %s
            """, (
                evo["evoluciona_a"],
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                stats["ataque_especial"],
                stats["defensa_especial"],
                payload.usuario_pokemon_id,
                usuario["id"]
            ))
        else:
            cursor.execute("""
                UPDATE usuario_pokemon
                SET pokemon_id = %s,
                    hp_actual = %s,
                    hp_max = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s
                WHERE id = %s AND usuario_id = %s
            """, (
                evo["evoluciona_a"],
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                payload.usuario_pokemon_id,
                usuario["id"]
            ))

        registrar_actividad_usuario(
            cursor,
            usuario["id"],
            pagina="mypokemon",
            accion="evolution",
            detalle=f"nivel:{evo['evoluciona_a']}"
        )
        conn.commit()
        return {
            "ok": True,
            "mensaje": "¡Tu Pokémon evolucionó por nivel!",
            "usuario_pokemon_id": payload.usuario_pokemon_id,
            "pokemon_id": evo["evoluciona_a"],
            "pokemon_nombre": base["nombre"]
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /pokemon/evolucionar-nivel:", error)
        return {"ok": False, "mensaje": "No se pudo evolucionar el Pokémon por nivel"}
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# POKEDEX
# =========================================================

@router.get("/pokedex/resumen/{usuario_id}")
def resumen_pokedex(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        meta = obtener_meta_pokedex_global(cursor)
        total_pokemon = int(meta["total_pokemon_tabla"] or 0)

        expresion_variante = obtener_expresion_codigo_variante(cursor, "up")

        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(up.pokemon_id::text, '|', {expresion_variante})) AS total_capturados
            FROM usuario_pokemon up
            WHERE up.usuario_id = %s
        """, (usuario_id,))
        total_capturados = int(cursor.fetchone()["total_capturados"] or 0)

        cursor.execute("""
            SELECT COUNT(DISTINCT pokemon_id) AS total_normales
            FROM usuario_pokemon
            WHERE usuario_id = %s AND es_shiny = FALSE
        """, (usuario_id,))
        total_normales = int(cursor.fetchone()["total_normales"] or 0)

        total_shiny = 0
        if existe_columna(cursor, "usuario_pokemon", "es_shiny"):
            cursor.execute("""
                SELECT COUNT(DISTINCT pokemon_id) AS total_shiny
                FROM usuario_pokemon
                WHERE usuario_id = %s AND es_shiny = TRUE
            """, (usuario_id,))
            total_shiny = int(cursor.fetchone()["total_shiny"] or 0)

        total_pokedex = int(meta["total_pokedex"] or 0)
        avance = round((total_capturados / total_pokedex) * 100, 1) if total_pokedex > 0 else 0

        return {
            "total_pokemon_tabla": total_pokemon,
            "total_variantes_por_pokemon": int(meta["total_variantes_por_pokemon"] or 0),
            "variantes_disponibles": meta["variantes_disponibles"],
            "total_pokedex": total_pokedex,
            "total_normales": total_normales,
            "total_shiny": total_shiny,
            "total_capturados": total_capturados,
            "avance": avance
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/pokemon/evoluciones-cache")
def obtener_evoluciones_cache():
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT id
            FROM pokemon
            ORDER BY id
        """)
        todos = cursor.fetchall()

        resultado = {str(row["id"]): [] for row in todos}

        cursor.execute("""
            SELECT
                er.pokemon_id,
                er.id,
                er.metodo,
                er.nivel_requerido,
                er.item_id,
                er.prioridad,
                er.activo,
                p2.id AS evolucion_id,
                p2.nombre AS evolucion_nombre,
                i.nombre AS item_nombre
            FROM evolucion_reglas er
            JOIN pokemon p2 ON p2.id = er.evoluciona_a
            LEFT JOIN items i ON i.id = er.item_id
            WHERE er.activo = TRUE
            ORDER BY er.pokemon_id ASC, er.prioridad ASC, p2.nombre ASC
        """)
        rows = cursor.fetchall()

        for row in rows:
            pokemon_id = str(row["pokemon_id"])

            resultado[pokemon_id].append({
                "id": row["id"],
                "tipo_metodo": row["metodo"],
                "evolucion_id": row["evolucion_id"],
                "evolucion_nombre": row["evolucion_nombre"],
                "nivel": row["nivel_requerido"],
                "item_id": row["item_id"],
                "item_nombre": row["item_nombre"],
                "prioridad": row["prioridad"],
                "activo": bool(row["activo"]),
            })

        return resultado
    finally:
        cursor.close()
        release_connection(conn)

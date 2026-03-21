import asyncio
import re
import random
import secrets
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel

from database import get_connection, get_cursor, release_connection
from auth import verify_google_token, create_access_token, get_current_user, get_current_user_from_token, JWT_SECRET, JWT_ALGORITHM
from jose import jwt, JWTError

router = APIRouter()
MAX_NIVEL_POKEMON = 200
AVATAR_ID_DEFAULT = "steven"
AVATAR_ID_REGEX = re.compile(r"^[a-z0-9_-]{1,60}$")
MAPS_NODE_ID_REGEX = re.compile(r"^[a-z0-9_-]{1,60}$")
MAPS_PRESENCIA_TTL_SEGUNDOS = 25
MAPS_WEBSOCKET_CONEXIONES = {}
MAPS_WEBSOCKET_LOCK = asyncio.Lock()
ENCUENTRO_TOKEN_TTL_SEGUNDOS = 60 * 5
BATTLE_RECOMPENSA_COOLDOWN_SEGUNDOS = 12
BATTLE_RECOMPENSA_ULTIMO_USO = {}
MAPS_ENCUENTROS_CONSUMIDOS = {}
RECOMPENSAS_ARENA_PERMITIDAS = {
    (2500, 2500): {"codigo": "normal", "exp": 2500, "pokedolares": 2500},
    (3500, 5000): {"codigo": "challenge", "exp": 3500, "pokedolares": 5000},
    (4500, 10000): {"codigo": "expert", "exp": 4500, "pokedolares": 10000},
    (6000, 15000): {"codigo": "master", "exp": 6000, "pokedolares": 15000},
}


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
    usuario_id: int | None = None
    usuario_pokemon_ids: list[int]
    exp_ganada: int = 25
    pokedolares_ganados: int = 500


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
):
    bonus_hp_shiny = 2 if es_shiny else 0

    hp_max = base_hp + (nivel * (5 + bonus_hp_shiny)) + bonus_hp_renacer
    ataque = base_ataque + (nivel * 2) + bonus_ataque_renacer
    defensa = base_defensa + (nivel * 2) + bonus_defensa_renacer
    velocidad = base_velocidad + nivel + bonus_velocidad_renacer

    return {
        "hp_max": hp_max,
        "ataque": ataque,
        "defensa": defensa,
        "velocidad": velocidad
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


def crear_token_encuentro(usuario_id: int, zona_id: int, pokemon_id: int, nivel: int, es_shiny: bool) -> str:
    now_ts = int(time.time())
    payload = {
        "type": "maps_encounter",
        "sub": str(usuario_id),
        "zona_id": int(zona_id),
        "pokemon_id": int(pokemon_id),
        "nivel": int(nivel),
        "es_shiny": bool(es_shiny),
        "jti": secrets.token_urlsafe(18),
        "iat": now_ts,
        "exp": now_ts + ENCUENTRO_TOKEN_TTL_SEGUNDOS
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decodificar_token_encuentro(token: str) -> dict:
    if not token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="encuentro_token es obligatorio"
        )

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="encuentro_token inválido o expirado"
        )

    if payload.get("type") != "maps_encounter":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="encuentro_token inválido"
        )

    try:
        return {
            "usuario_id": int(payload["sub"]),
            "zona_id": int(payload["zona_id"]),
            "pokemon_id": int(payload["pokemon_id"]),
            "nivel": int(payload["nivel"]),
            "es_shiny": bool(payload.get("es_shiny", False)),
            "jti": str(payload["jti"])
        }
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="encuentro_token incompleto"
        )


def encuentro_ya_fue_consumido(jti: str) -> bool:
    now_ts = int(time.time())
    limpiar_cache_temporal(MAPS_ENCUENTROS_CONSUMIDOS, now_ts)
    return str(jti) in MAPS_ENCUENTROS_CONSUMIDOS


def marcar_encuentro_consumido(jti: str):
    MAPS_ENCUENTROS_CONSUMIDOS[str(jti)] = int(time.time()) + ENCUENTRO_TOKEN_TTL_SEGUNDOS


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
        cursor.execute("""
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
        cursor.execute("""
            SELECT id, nombre, tipo, ataque, defensa, hp, imagen, velocidad, nivel, rareza, generacion, tiene_mega
            FROM pokemon
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


@router.post("/battle/recompensa-victoria")
def recompensa_victoria_batalla(payload: RecompensaBatallaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])
        validar_cooldown_recompensa_batalla(usuario["id"])

        recompensa = obtener_recompensa_batalla_permitida(payload.exp_ganada, payload.pokedolares_ganados)
        exp_ganada = int(recompensa["exp"])
        pokedolares_ganados = int(recompensa["pokedolares"])

        ids_limpios = []
        for raw_id in payload.usuario_pokemon_ids:
            try:
                pokemon_id = int(raw_id)
            except (TypeError, ValueError):
                continue

            if pokemon_id <= 0 or pokemon_id in ids_limpios:
                continue

            ids_limpios.append(pokemon_id)

        if not ids_limpios:
            return {
                "ok": False,
                "mensaje": "No se encontraron Pokémon válidos para otorgar recompensas"
            }

        if len(ids_limpios) > 6:
            return {
                "ok": False,
                "mensaje": "No puedes reclamar recompensa para más de 6 Pokémon"
            }

        cursor.execute("""
            SELECT id, pokedolares
            FROM usuarios
            WHERE id = %s
        """, (usuario["id"],))
        usuario_db = cursor.fetchone()

        if not usuario_db:
            return {
                "ok": False,
                "mensaje": "Usuario no encontrado"
            }

        nuevos_pokedolares = int(usuario_db["pokedolares"] or 0) + pokedolares_ganados

        cursor.execute("""
            UPDATE usuarios
            SET pokedolares = %s
            WHERE id = %s
        """, (nuevos_pokedolares, usuario["id"]))

        pokemon_actualizados = []

        for usuario_pokemon_id in ids_limpios:
            cursor.execute("""
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
                    up.bonus_velocidad_renacer
                FROM usuario_pokemon up
                WHERE up.id = %s AND up.usuario_id = %s
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

            cursor.execute("""
                SELECT hp, ataque, defensa, velocidad
                FROM pokemon
                WHERE id = %s
            """, (poke["pokemon_id"],))
            base = cursor.fetchone()

            if not base:
                continue

            if nivel_actual >= MAX_NIVEL_POKEMON:
                stats = calcular_stats(
                    base["hp"],
                    base["ataque"],
                    base["defensa"],
                    base["velocidad"],
                    MAX_NIVEL_POKEMON,
                    bool(poke["es_shiny"]),
                    poke["bonus_hp_renacer"],
                    poke["bonus_ataque_renacer"],
                    poke["bonus_defensa_renacer"],
                    poke["bonus_velocidad_renacer"]
                )

                cursor.execute("""
                    UPDATE usuario_pokemon
                    SET nivel = %s,
                        experiencia = %s,
                        experiencia_total = %s,
                        victorias_total = %s,
                        hp_max = %s,
                        hp_actual = LEAST(hp_actual, %s),
                        ataque = %s,
                        defensa = %s,
                        velocidad = %s
                    WHERE id = %s
                """, (
                    MAX_NIVEL_POKEMON,
                    0,
                    nuevo_exp_total,
                    nuevas_victorias,
                    stats["hp_max"],
                    stats["hp_max"],
                    stats["ataque"],
                    stats["defensa"],
                    stats["velocidad"],
                    usuario_pokemon_id
                ))

                pokemon_actualizados.append({
                    "usuario_pokemon_id": usuario_pokemon_id,
                    "nivel": MAX_NIVEL_POKEMON,
                    "experiencia": 0,
                    "experiencia_total": nuevo_exp_total,
                    "victorias_total": nuevas_victorias,
                    "hp_max": stats["hp_max"],
                    "ataque": stats["ataque"],
                    "defensa": stats["defensa"],
                    "velocidad": stats["velocidad"],
                    "mensaje": "Nivel máximo alcanzado, EXP total y victoria acumuladas"
                })
                continue

            nueva_exp = exp_actual + exp_ganada
            nuevo_nivel = nivel_actual

            while nuevo_nivel < MAX_NIVEL_POKEMON and nueva_exp >= (nuevo_nivel * 50):
                nueva_exp -= (nuevo_nivel * 50)
                nuevo_nivel += 1

            if nuevo_nivel >= MAX_NIVEL_POKEMON:
                nuevo_nivel = MAX_NIVEL_POKEMON
                nueva_exp = 0

            stats = calcular_stats(
                base["hp"],
                base["ataque"],
                base["defensa"],
                base["velocidad"],
                nuevo_nivel,
                bool(poke["es_shiny"]),
                poke["bonus_hp_renacer"],
                poke["bonus_ataque_renacer"],
                poke["bonus_defensa_renacer"],
                poke["bonus_velocidad_renacer"]
            )

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
                usuario_pokemon_id
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
                "velocidad": stats["velocidad"]
            })

        conn.commit()
        registrar_cooldown_recompensa_batalla(usuario["id"])

        return {
            "ok": True,
            "mensaje": "Recompensas aplicadas correctamente",
            "usuario_id": usuario["id"],
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
def generar_encuentro(payload: EncuentroPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        cursor.execute("""
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
                p.tiene_mega
            FROM zona_pokemon zp
            INNER JOIN pokemon p ON p.id = zp.pokemon_id
            WHERE zp.zona_id = %s
        """, (payload.zona_id,))
        rows = cursor.fetchall()

        if not rows:
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
            es_shiny
        )

        encuentro_token = crear_token_encuentro(
            usuario_id=usuario["id"],
            zona_id=payload.zona_id,
            pokemon_id=int(elegido["pokemon_id"]),
            nivel=nivel,
            es_shiny=es_shiny
        )

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
            "nivel": nivel,
            "es_shiny": es_shiny,
            "encuentro_token": encuentro_token,
            "encuentro_ttl_segundos": ENCUENTRO_TOKEN_TTL_SEGUNDOS
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/maps/intentar-captura")
def intentar_captura(payload: IntentoCapturaPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        validar_usuario_payload(payload.usuario_id, usuario["id"])

        encuentro = decodificar_token_encuentro(payload.encuentro_token)

        if int(encuentro["usuario_id"]) != int(usuario["id"]):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="El encuentro no pertenece al usuario autenticado"
            )

        if encuentro_ya_fue_consumido(encuentro["jti"]):
            return {
                "capturado": False,
                "mensaje": "Este encuentro ya fue capturado y ya no es válido"
            }

        if int(payload.pokemon_id or 0) != int(encuentro["pokemon_id"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El Pokémon del encuentro no coincide con el enviado"
            )

        if int(payload.nivel or 0) != int(encuentro["nivel"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El nivel del encuentro no coincide con el enviado"
            )

        if bool(payload.es_shiny) != bool(encuentro["es_shiny"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="La variante del encuentro no coincide con la enviada"
            )

        cursor.execute("""
            SELECT id, nombre, bonus_captura
            FROM items
            WHERE id = %s
        """, (payload.item_id,))
        item_db = cursor.fetchone()

        if not item_db:
            return {
                "capturado": False,
                "mensaje": "El item seleccionado no existe"
            }

        cursor.execute("""
            SELECT id, cantidad
            FROM usuario_items
            WHERE usuario_id = %s AND item_id = %s
        """, (usuario["id"], payload.item_id))
        item_usuario = cursor.fetchone()

        if not item_usuario or item_usuario["cantidad"] <= 0:
            return {
                "capturado": False,
                "mensaje": f"No tienes {item_db['nombre']}"
            }

        nombre_item = item_db["nombre"]
        es_shiny = bool(encuentro["es_shiny"])

        if nombre_item == "Poke Ball":
            probabilidad = 50
        elif nombre_item == "Super Ball":
            probabilidad = 65
        elif nombre_item == "Ultra Ball":
            probabilidad = 80
        elif nombre_item == "Master Ball":
            probabilidad = 100
        else:
            probabilidad = 35 + int(item_db["bonus_captura"] or 0)

        if es_shiny and nombre_item != "Master Ball":
            probabilidad -= 10

        probabilidad = max(1, min(probabilidad, 100))
        captura_exitosa = random.randint(1, 100) <= probabilidad

        cursor.execute("""
            UPDATE usuario_items
            SET cantidad = cantidad - 1
            WHERE id = %s
        """, (item_usuario["id"],))

        if captura_exitosa:
            cursor.execute("""
                SELECT hp, ataque, defensa, velocidad
                FROM pokemon
                WHERE id = %s
            """, (encuentro["pokemon_id"],))
            base = cursor.fetchone()

            if not base:
                conn.rollback()
                return {
                    "capturado": False,
                    "mensaje": "Pokémon no encontrado"
                }

            nivel_captura = max(1, min(int(encuentro["nivel"] or 1), MAX_NIVEL_POKEMON))

            stats = calcular_stats(
                base["hp"],
                base["ataque"],
                base["defensa"],
                base["velocidad"],
                nivel_captura,
                es_shiny
            )

            experiencia_total_inicial = calcular_experiencia_total_base(nivel_captura, 0)

            cursor.execute("""
                INSERT INTO usuario_pokemon
                (
                    usuario_id, pokemon_id, nivel, experiencia, experiencia_total, victorias_total,
                    hp_actual, hp_max, ataque, defensa, velocidad, es_shiny
                )
                VALUES (%s, %s, %s, 0, %s, 0, %s, %s, %s, %s, %s, %s)
            """, (
                usuario["id"],
                encuentro["pokemon_id"],
                nivel_captura,
                experiencia_total_inicial,
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                es_shiny
            ))

            conn.commit()
            marcar_encuentro_consumido(encuentro["jti"])

            return {
                "capturado": True,
                "mensaje": f"¡{nombre_item} usada con éxito! Pokémon capturado.",
                "probabilidad": probabilidad,
                "item_id": payload.item_id,
                "experiencia_total": experiencia_total_inicial,
                "victorias_total": 0
            }

        conn.commit()
        return {
            "capturado": False,
            "mensaje": f"{nombre_item} usada, pero el Pokémon escapó.",
            "probabilidad": probabilidad,
            "item_id": payload.item_id
        }
    except HTTPException:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)


# =========================================================
# TIENDA
# =========================================================

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

        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad, nombre
            FROM pokemon
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
            poke["bonus_velocidad_renacer"]
        )

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

        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad, nombre
            FROM pokemon
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
            poke["bonus_velocidad_renacer"]
        )

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
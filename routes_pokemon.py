from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
import random

from database import get_connection, get_cursor, release_connection
from auth import verify_google_token, create_access_token, get_current_user

router = APIRouter()
MAX_NIVEL_POKEMON = 200

# =========================================================
# PAYLOADS
# =========================================================

class GoogleLoginPayload(BaseModel):
    credential: str


class CapturaPayload(BaseModel):
    usuario_id: int
    pokemon_id: int
    nivel: int = 1
    es_shiny: bool = False


class EncuentroPayload(BaseModel):
    usuario_id: int
    zona_id: int


class IntentoCapturaPayload(BaseModel):
    usuario_id: int
    pokemon_id: int
    nivel: int
    es_shiny: bool = False
    hp_actual: int = 100
    hp_maximo: int = 100
    item_id: int


class ExpPayload(BaseModel):
    usuario_pokemon_id: int
    exp_ganada: int

class PokedolaresPayload(BaseModel):
    usuario_id: int
    monto: int

class RecompensaBatallaPayload(BaseModel):
    usuario_id: int
    usuario_pokemon_ids: list[int]
    exp_ganada: int = 25
    pokedolares_ganados: int = 500


class CompraItemPayload(BaseModel):
    usuario_id: int
    item_id: int
    cantidad: int = 1


class EvolucionItemPayload(BaseModel):
    usuario_pokemon_id: int
    item_id: int
    usuario_id: int


class SoltarPokemonPayload(BaseModel):
    usuario_pokemon_id: int
    usuario_id: int


class EvolucionNivelPayload(BaseModel):
    usuario_pokemon_id: int
    usuario_id: int


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


def obtener_usuario_por_id(usuario_id: int):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT id, google_id, nombre, correo, foto, avatar_url, rol, pokedolares, fecha_registro
            FROM usuarios
            WHERE id = %s
        """, (usuario_id,))
        usuario = cursor.fetchone()
        return dict(usuario) if usuario else None
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
    avatar_url = foto
    rol = "usuario"

    if not google_id or not correo:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Google no devolvió datos suficientes"
        )

    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT id, nombre, correo, foto, avatar_url, rol, pokedolares, fecha_registro
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
                    avatar_url = %s
                WHERE id = %s
            """, (google_id, nombre, correo, foto, avatar_url, usuario_id))
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
            INSERT INTO usuarios (google_id, nombre, correo, foto, avatar_url, rol)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (google_id, nombre, correo, foto, avatar_url, rol))
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
    return {
        "autenticado": True,
        "usuario": usuario
    }


# =========================================================
# USUARIO / ME
# =========================================================

@router.get("/usuario/me")
def obtener_usuario_me(usuario=Depends(get_current_user)):
    return usuario


@router.get("/usuario/me/items")
def obtener_items_me(usuario=Depends(get_current_user)):
    return obtener_items_usuario_data(usuario["id"])


@router.get("/usuario/me/pokemon")
def obtener_pokemon_me(usuario=Depends(get_current_user)):
    return obtener_pokemon_usuario_data(usuario["id"])


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
def capturar_pokemon(payload: CapturaPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad
            FROM pokemon
            WHERE id = %s
        """, (payload.pokemon_id,))
        base = cursor.fetchone()

        if not base:
            return {"mensaje": "Pokémon no encontrado"}

        stats = calcular_stats(
            base["hp"],
            base["ataque"],
            base["defensa"],
            base["velocidad"],
            payload.nivel,
            payload.es_shiny
        )

        cursor.execute("""
            INSERT INTO usuario_pokemon
            (usuario_id, pokemon_id, nivel, experiencia, hp_actual, hp_max, ataque, defensa, velocidad, es_shiny)
            VALUES (%s, %s, %s, 0, %s, %s, %s, %s, %s, %s)
        """, (
            payload.usuario_id,
            payload.pokemon_id,
            payload.nivel,
            stats["hp_max"],
            stats["hp_max"],
            stats["ataque"],
            stats["defensa"],
            stats["velocidad"],
            payload.es_shiny
        ))

        conn.commit()

        return {
            "mensaje": "Pokémon agregado al usuario",
            "stats": stats
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/usuario/ganar-exp")
def ganar_experiencia(payload: ExpPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT
                up.id,
                up.pokemon_id,
                up.nivel,
                up.experiencia,
                up.es_shiny,
                up.bonus_hp_renacer,
                up.bonus_ataque_renacer,
                up.bonus_defensa_renacer,
                up.bonus_velocidad_renacer
            FROM usuario_pokemon up
            WHERE up.id = %s
        """, (payload.usuario_pokemon_id,))
        poke = cursor.fetchone()

        if not poke:
            return {"mensaje": "Pokémon del usuario no encontrado"}

        nivel_actual = int(poke["nivel"] or 1)
        exp_actual = int(poke["experiencia"] or 0)
        exp_ganada = max(0, int(payload.exp_ganada or 0))

        if nivel_actual >= MAX_NIVEL_POKEMON:
            return {
                "mensaje": "Este Pokémon ya alcanzó el nivel máximo",
                "nivel": MAX_NIVEL_POKEMON,
                "experiencia": 0
            }

        nueva_exp = exp_actual + exp_ganada
        nuevo_nivel = nivel_actual

        while nuevo_nivel < MAX_NIVEL_POKEMON and nueva_exp >= (nuevo_nivel * 50):
            nueva_exp -= (nuevo_nivel * 50)
            nuevo_nivel += 1

        if nuevo_nivel >= MAX_NIVEL_POKEMON:
            nuevo_nivel = MAX_NIVEL_POKEMON
            nueva_exp = 0

        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad
            FROM pokemon
            WHERE id = %s
        """, (poke["pokemon_id"],))
        base = cursor.fetchone()

        if not base:
            return {"mensaje": "Pokémon base no encontrado"}

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
                hp_max = %s,
                hp_actual = %s,
                ataque = %s,
                defensa = %s,
                velocidad = %s
            WHERE id = %s
        """, (
            nuevo_nivel,
            nueva_exp,
            stats["hp_max"],
            stats["hp_max"],
            stats["ataque"],
            stats["defensa"],
            stats["velocidad"],
            payload.usuario_pokemon_id
        ))

        conn.commit()

        return {
            "mensaje": "Experiencia actualizada",
            "nivel": nuevo_nivel,
            "experiencia": nueva_exp,
            "stats": stats
        }
    finally:
        cursor.close()
        release_connection(conn)

@router.post("/battle/recompensa-victoria")
def recompensa_victoria_batalla(payload: RecompensaBatallaPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        if payload.usuario_id <= 0:
            return {
                "ok": False,
                "mensaje": "Usuario inválido"
            }

        if payload.pokedolares_ganados < 0:
            return {
                "ok": False,
                "mensaje": "Los pokedólares no pueden ser negativos"
            }

        if payload.exp_ganada < 0:
            return {
                "ok": False,
                "mensaje": "La experiencia no puede ser negativa"
            }

        cursor.execute("""
            SELECT id, pokedolares
            FROM usuarios
            WHERE id = %s
        """, (payload.usuario_id,))
        usuario = cursor.fetchone()

        if not usuario:
            return {
                "ok": False,
                "mensaje": "Usuario no encontrado"
            }

        nuevos_pokedolares = int(usuario["pokedolares"] or 0) + int(payload.pokedolares_ganados)

        cursor.execute("""
            UPDATE usuarios
            SET pokedolares = %s
            WHERE id = %s
        """, (nuevos_pokedolares, payload.usuario_id))

        pokemon_actualizados = []

        for usuario_pokemon_id in payload.usuario_pokemon_ids:
            cursor.execute("""
                SELECT
                    up.id,
                    up.usuario_id,
                    up.pokemon_id,
                    up.nivel,
                    up.experiencia,
                    up.es_shiny,
                    up.bonus_hp_renacer,
                    up.bonus_ataque_renacer,
                    up.bonus_defensa_renacer,
                    up.bonus_velocidad_renacer
                FROM usuario_pokemon up
                WHERE up.id = %s AND up.usuario_id = %s
            """, (usuario_pokemon_id, payload.usuario_id))
            poke = cursor.fetchone()

            if not poke:
                continue

            nivel_actual = int(poke["nivel"] or 1)
            exp_actual = int(poke["experiencia"] or 0)
            exp_ganada = max(0, int(payload.exp_ganada or 0))

            if nivel_actual >= MAX_NIVEL_POKEMON:
                pokemon_actualizados.append({
                    "usuario_pokemon_id": usuario_pokemon_id,
                    "nivel": MAX_NIVEL_POKEMON,
                    "experiencia": 0,
                    "mensaje": "Nivel máximo alcanzado"
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

            cursor.execute("""
                SELECT hp, ataque, defensa, velocidad
                FROM pokemon
                WHERE id = %s
            """, (poke["pokemon_id"],))
            base = cursor.fetchone()

            if not base:
                continue

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
                    hp_max = %s,
                    hp_actual = %s,
                    ataque = %s,
                    defensa = %s,
                    velocidad = %s
                WHERE id = %s
            """, (
                nuevo_nivel,
                nueva_exp,
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
                "hp_max": stats["hp_max"],
                "ataque": stats["ataque"],
                "defensa": stats["defensa"],
                "velocidad": stats["velocidad"]
            })

        conn.commit()

        return {
            "ok": True,
            "mensaje": "Recompensas aplicadas correctamente",
            "usuario_id": payload.usuario_id,
            "pokedolares_ganados": payload.pokedolares_ganados,
            "pokedolares_actuales": nuevos_pokedolares,
            "exp_ganada": payload.exp_ganada,
            "pokemon_actualizados": pokemon_actualizados
        }

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
def soltar_pokemon(payload: SoltarPokemonPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        cursor.execute("""
            SELECT id
            FROM usuario_pokemon
            WHERE id = %s AND usuario_id = %s
        """, (payload.usuario_pokemon_id, payload.usuario_id))
        pokemon = cursor.fetchone()

        if not pokemon:
            return {"mensaje": "Pokémon no encontrado en tu colección"}

        cursor.execute("""
            DELETE FROM usuario_pokemon
            WHERE id = %s AND usuario_id = %s
        """, (payload.usuario_pokemon_id, payload.usuario_id))
        conn.commit()

        return {"mensaje": "Pokémon liberado correctamente"}
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


@router.post("/maps/encuentro")
def generar_encuentro(payload: EncuentroPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
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
            "es_shiny": es_shiny
        }
    finally:
        cursor.close()
        release_connection(conn)


@router.post("/maps/intentar-captura")
def intentar_captura(payload: IntentoCapturaPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
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
        """, (payload.usuario_id, payload.item_id))
        item_usuario = cursor.fetchone()

        if not item_usuario or item_usuario["cantidad"] <= 0:
            return {
                "capturado": False,
                "mensaje": f"No tienes {item_db['nombre']}"
            }

        nombre_item = item_db["nombre"]

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

        if payload.es_shiny and nombre_item != "Master Ball":
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
            """, (payload.pokemon_id,))
            base = cursor.fetchone()

            if not base:
                conn.rollback()
                return {
                    "capturado": False,
                    "mensaje": "Pokémon no encontrado"
                }

            stats = calcular_stats(
                base["hp"],
                base["ataque"],
                base["defensa"],
                base["velocidad"],
                payload.nivel,
                payload.es_shiny
            )

            cursor.execute("""
                INSERT INTO usuario_pokemon
                (usuario_id, pokemon_id, nivel, experiencia, hp_actual, hp_max, ataque, defensa, velocidad, es_shiny)
                VALUES (%s, %s, %s, 0, %s, %s, %s, %s, %s, %s)
            """, (
                payload.usuario_id,
                payload.pokemon_id,
                payload.nivel,
                stats["hp_max"],
                stats["hp_max"],
                stats["ataque"],
                stats["defensa"],
                stats["velocidad"],
                payload.es_shiny
            ))

            conn.commit()

            return {
                "capturado": True,
                "mensaje": f"¡{nombre_item} usada con éxito! Pokémon capturado.",
                "probabilidad": probabilidad,
                "item_id": payload.item_id
            }

        conn.commit()
        return {
            "capturado": False,
            "mensaje": f"{nombre_item} usada, pero el Pokémon escapó.",
            "probabilidad": probabilidad,
            "item_id": payload.item_id
        }
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
def comprar_item(payload: CompraItemPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
        if payload.cantidad <= 0:
            return {"mensaje": "La cantidad debe ser mayor a 0"}

        cursor.execute("""
            SELECT id, nombre, precio
            FROM items
            WHERE id = %s
        """, (payload.item_id,))
        item = cursor.fetchone()

        if not item:
            return {"mensaje": "Item no encontrado"}

        cursor.execute("""
            SELECT id, pokedolares
            FROM usuarios
            WHERE id = %s
        """, (payload.usuario_id,))
        usuario = cursor.fetchone()

        if not usuario:
            return {"mensaje": "Usuario no encontrado"}

        total = item["precio"] * payload.cantidad

        if usuario["pokedolares"] < total:
            return {"mensaje": "No tienes suficientes pokedólares"}

        nuevo_saldo = usuario["pokedolares"] - total

        cursor.execute("""
            UPDATE usuarios
            SET pokedolares = %s
            WHERE id = %s
        """, (nuevo_saldo, payload.usuario_id))

        cursor.execute("""
            INSERT INTO usuario_items (usuario_id, item_id, cantidad)
            VALUES (%s, %s, %s)
            ON CONFLICT (usuario_id, item_id)
            DO UPDATE SET cantidad = usuario_items.cantidad + EXCLUDED.cantidad
            RETURNING cantidad
        """, (payload.usuario_id, payload.item_id, payload.cantidad))

        cantidad_actual = cursor.fetchone()["cantidad"]

        conn.commit()

        return {
            "ok": True,
            "mensaje": f"Compraste {payload.cantidad} x {item['nombre']}",
            "item_id": item["id"],
            "item_nombre": item["nombre"],
            "cantidad_comprada": payload.cantidad,
            "cantidad_actual": cantidad_actual,
            "pokedolares_actual": nuevo_saldo
        }
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
def evolucionar_con_item(payload: EvolucionItemPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
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
        """, (payload.usuario_pokemon_id, payload.usuario_id))
        poke = cursor.fetchone()

        if not poke:
            return {"mensaje": "Pokémon del usuario no encontrado"}

        cursor.execute("""
            SELECT evoluciona_a
            FROM evoluciones_item
            WHERE pokemon_id = %s AND item_id = %s
        """, (poke["pokemon_id"], payload.item_id))
        evo = cursor.fetchone()

        if not evo:
            return {"mensaje": "Ese Pokémon no evoluciona con este item"}

        cursor.execute("""
            SELECT id, cantidad
            FROM usuario_items
            WHERE usuario_id = %s AND item_id = %s
        """, (payload.usuario_id, payload.item_id))
        item_user = cursor.fetchone()

        if not item_user or item_user["cantidad"] <= 0:
            return {"mensaje": "No tienes ese item"}

        cursor.execute("""
            UPDATE usuario_items
            SET cantidad = cantidad - 1
            WHERE id = %s
        """, (item_user["id"],))

        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad
            FROM pokemon
            WHERE id = %s
        """, (evo["evoluciona_a"],))
        base = cursor.fetchone()

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
            WHERE id = %s
        """, (
            evo["evoluciona_a"],
            stats["hp_max"],
            stats["hp_max"],
            stats["ataque"],
            stats["defensa"],
            stats["velocidad"],
            payload.usuario_pokemon_id
        ))

        conn.commit()
        return {"mensaje": "¡Tu Pokémon evolucionó con éxito!"}
    finally:
        cursor.close()
        release_connection(conn)


@router.get("/usuario/pokemon/{usuario_pokemon_id}/evolucion")
def revisar_evolucion(usuario_pokemon_id: int):
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
            WHERE up.id = %s
        """, (usuario_pokemon_id,))
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
def evolucionar_por_nivel(payload: EvolucionNivelPayload):
    conn = get_connection()
    cursor = get_cursor(conn)

    try:
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
        """, (payload.usuario_pokemon_id, payload.usuario_id))
        poke = cursor.fetchone()

        if not poke:
            return {"mensaje": "Pokémon no encontrado"}

        cursor.execute("""
            SELECT e.evoluciona_a
            FROM evoluciones e
            WHERE e.pokemon_id = %s AND e.nivel <= %s
            ORDER BY e.nivel DESC
            LIMIT 1
        """, (poke["pokemon_id"], poke["nivel"]))
        evo = cursor.fetchone()

        if not evo:
            return {"mensaje": "Este Pokémon aún no puede evolucionar por nivel"}

        cursor.execute("""
            SELECT hp, ataque, defensa, velocidad
            FROM pokemon
            WHERE id = %s
        """, (evo["evoluciona_a"],))
        base = cursor.fetchone()

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
            WHERE id = %s
        """, (
            evo["evoluciona_a"],
            stats["hp_max"],
            stats["hp_max"],
            stats["ataque"],
            stats["defensa"],
            stats["velocidad"],
            payload.usuario_pokemon_id
        ))

        conn.commit()
        return {"mensaje": "¡Tu Pokémon evolucionó por nivel!"}
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
        cursor.execute("""
            SELECT COUNT(*) AS total_pokemon
            FROM pokemon
        """)
        total_pokemon = cursor.fetchone()["total_pokemon"]

        cursor.execute("""
            SELECT COUNT(DISTINCT pokemon_id) AS total_normales
            FROM usuario_pokemon
            WHERE usuario_id = %s AND es_shiny = FALSE
        """, (usuario_id,))
        total_normales = cursor.fetchone()["total_normales"]

        cursor.execute("""
            SELECT COUNT(DISTINCT pokemon_id) AS total_shiny
            FROM usuario_pokemon
            WHERE usuario_id = %s AND es_shiny = TRUE
        """, (usuario_id,))
        total_shiny = cursor.fetchone()["total_shiny"]

        total_pokedex = total_pokemon * 2
        total_capturados = total_normales + total_shiny
        avance = round((total_capturados / total_pokedex) * 100, 1) if total_pokedex > 0 else 0

        return {
            "total_pokemon_tabla": total_pokemon,
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
        # 1. Traer todos los pokemon
        cursor.execute("""
            SELECT id
            FROM pokemon
            ORDER BY id
        """)
        todos = cursor.fetchall()

        resultado = {str(row["id"]): [] for row in todos}

        # 2. Traer todas las reglas activas
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
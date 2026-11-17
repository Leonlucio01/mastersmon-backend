"""MastersMon Alola V4: Vulpix (Alola) -> Ninetales (Alola), Ice Stone.

Routes are included BEFORE routes_pokemon in app.py. Non-regional captures
always delegate to the existing operational evolution handlers. No migration,
no .env flags, and no new public.evolucion_reglas entry are necessary.
"""
from fastapi import APIRouter, Depends, HTTPException
from psycopg2 import errors, sql

from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from evolution_service import EvolutionError
from routes_pokemon import (
    EvolucionItemPayload,
    revisar_evolucion as revisar_evolucion_original,
    evolucionar_con_item as evolucionar_con_item_original,
    obtener_pokemon_me as obtener_pokemon_me_original,
    calcular_stats,
    _registrar_actividad_evolucion,
    validar_usuario_payload,
)

router_alola_evolution_v4 = APIRouter(tags=["Alola V4 - Evolution"])
ORIGIN_SPECIES = 37
ORIGIN_FORM = 10205
DEST_SPECIES = 38
DEST_FORM = 10206
ITEM_CODE = "ice_stone"


def _regional(capture):
    return int(capture["pokemon_id"]) == ORIGIN_SPECIES and capture.get("forma_id") == ORIGIN_FORM


def _fetch_capture(cursor, capture_id, user_id, *, lock=False):
    cursor.execute(
        "SELECT up.*, up.xmin::text AS evolution_state FROM public.usuario_pokemon up "
        "WHERE up.id=%s AND up.usuario_id=%s" + (" FOR UPDATE NOWAIT" if lock else ""),
        (capture_id, user_id),
    )
    return cursor.fetchone()


def _ice_stone(cursor):
    # Only this exact canonical item is accepted, not another item with a similar name.
    cursor.execute(
        "SELECT id, nombre FROM public.items WHERE LOWER(codigo)=%s LIMIT 2", (ITEM_CODE,)
    )
    items = cursor.fetchall()
    if len(items) != 1:
        raise EvolutionError(409, "Ice Stone no está configurada de forma única en PokéMart")
    return items[0]


def _assert_forms(cursor):
    cursor.execute("""
        SELECT r.form_id, r.species_id, r.region,
               a.sprite_normal, a.sprite_shiny
        FROM public.mm30_p6_form_registry r
        JOIN public.mm30_p8_form_assets a ON a.form_id = r.form_id
        WHERE (r.form_id=%s AND r.species_id=%s AND r.capturable=TRUE)
           OR (r.form_id=%s AND r.species_id=%s)
    """, (ORIGIN_FORM, ORIGIN_SPECIES, DEST_FORM, DEST_SPECIES))
    found = {int(row["form_id"]): row for row in cursor.fetchall()}
    for form_id in (ORIGIN_FORM, DEST_FORM):
        row = found.get(form_id)
        if not row or row["region"] != "alola" or not row["sprite_normal"] or not row["sprite_shiny"]:
            raise EvolutionError(409, "El catálogo regional de Alola no está completo")


def _destination_stats(cursor):
    cursor.execute("""
        SELECT s.identifier, fs.base_stat
        FROM public.mm30_p1_form_stats fs
        JOIN public.mm30_p1_stats s ON s.id=fs.stat_id
        WHERE fs.form_id=%s
    """, (DEST_FORM,))
    stats = {r["identifier"]: int(r["base_stat"]) for r in cursor.fetchall()}
    keys = ("hp", "attack", "defense", "speed", "special-attack", "special-defense")
    if any(stats.get(k, 0) <= 0 for k in keys):
        raise EvolutionError(409, "Faltan estadísticas canónicas para Ninetales de Alola")
    return stats


def _inspect_regional(cursor, capture, user_id):
    _assert_forms(cursor)
    stone = _ice_stone(cursor)
    cursor.execute("""
        SELECT COALESCE(cantidad, 0) AS cantidad
        FROM public.usuario_items
        WHERE usuario_id=%s AND item_id=%s
    """, (user_id, stone["id"]))
    row = cursor.fetchone()
    amount = max(0, int(row["cantidad"] or 0)) if row else 0
    option = {
        "id": None, "tipo_metodo": "item", "tipo": "item",
        "evolucion_id": DEST_SPECIES, "evoluciona_a": DEST_SPECIES,
        "evolucion_nombre": "Ninetales (Alola)",
        "forma_destino_id": DEST_FORM,
        "nivel": None, "nivel_requerido": None,
        "item_id": int(stone["id"]), "item_nombre": stone["nombre"],
        "prioridad": 0, "activo": True,
        "cantidad": amount, "tiene_item": amount > 0, "listo": amount > 0,
    }
    return {
        "pokemon_actual": "Vulpix (Alola)", "pokemon_id": ORIGIN_SPECIES,
        "forma_id": ORIGIN_FORM, "nivel_actual": int(capture["nivel"]),
        "estado_evolucion": capture["evolution_state"],
        "puede_evolucionar": True, "listo_ahora": amount > 0,
        "tipo": "item", "opciones": [option], "motivo": None,
    }


@router_alola_evolution_v4.get("/usuario/pokemon/{usuario_pokemon_id}/evolucion")
def revisar_evolucion_alola(usuario_pokemon_id: int, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        capture = _fetch_capture(cursor, usuario_pokemon_id, usuario["id"])
        if capture and _regional(capture):
            return _inspect_regional(cursor, capture, usuario["id"])
    except EvolutionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            release_connection(conn)
    # Original rules, including Vulpix Kanto, Wurmple, and Shedinja, are untouched.
    return revisar_evolucion_original(usuario_pokemon_id, usuario)



@router_alola_evolution_v4.get("/usuario/me/pokemon")
def obtener_pokemon_me_alola(usuario=Depends(get_current_user)):
    """Reuse all existing collection fields; resolve evolved regional identity only."""
    rows = obtener_pokemon_me_original(usuario)
    for row in rows:
        if int(row.get("pokemon_id") or 0) == DEST_SPECIES and int(row.get("forma_id") or 0) == DEST_FORM:
            row["nombre"] = "Ninetales (Alola)"
            row["tipo"] = "Ice/Fairy"
            row["region"] = "alola"
            shiny = bool(row.get("es_shiny"))
            row["imagen"] = ("img/pokemon-png/sprites_shiny/0038_a_s.png"
                            if shiny else "img/pokemon-png/sprites_normal/0038_a.png")
    return rows

def _evolve_regional(cursor, capture, payload, user_id):
    _assert_forms(cursor)
    stone = _ice_stone(cursor)
    if int(payload.item_id) != int(stone["id"]):
        raise EvolutionError(400, "Vulpix de Alola necesita Ice Stone")
    if payload.evolucion_id is not None and int(payload.evolucion_id) != DEST_SPECIES:
        raise EvolutionError(400, "La forma de Alola solo evoluciona a Ninetales de Alola")
    if payload.pokemon_origen_id is not None and int(payload.pokemon_origen_id) != ORIGIN_SPECIES:
        raise EvolutionError(409, "La especie de origen ha cambiado")
    if payload.estado_evolucion is not None and str(payload.estado_evolucion) != str(capture["evolution_state"]):
        raise EvolutionError(409, "El Pokémon cambió; vuelve a comprobar la evolución")

    cursor.execute("""
        SELECT id, cantidad FROM public.usuario_items
        WHERE usuario_id=%s AND item_id=%s FOR UPDATE NOWAIT
    """, (user_id, stone["id"]))
    inventory = cursor.fetchone()
    if not inventory or int(inventory["cantidad"] or 0) < 1:
        raise EvolutionError(400, "Necesitas una Ice Stone")

    cursor.execute("SELECT id FROM public.pokemon WHERE id=%s FOR SHARE", (DEST_SPECIES,))
    if not cursor.fetchone():
        raise EvolutionError(409, "Ninetales no existe en el catálogo nacional")

    b = _destination_stats(cursor)
    stats = calcular_stats(
        b["hp"], b["attack"], b["defense"], b["speed"],
        int(capture["nivel"]), bool(capture["es_shiny"]),
        capture.get("bonus_hp_renacer", 0), capture.get("bonus_ataque_renacer", 0),
        capture.get("bonus_defensa_renacer", 0), capture.get("bonus_velocidad_renacer", 0),
        b["special-attack"], b["special-defense"],
        capture.get("bonus_ataque_especial_renacer", 0),
        capture.get("bonus_defensa_especial_renacer", 0),
    )
    cursor.execute("""
        UPDATE public.usuario_items SET cantidad=cantidad-1
        WHERE id=%s AND usuario_id=%s AND item_id=%s AND cantidad>0
        RETURNING cantidad
    """, (inventory["id"], user_id, stone["id"]))
    remaining = cursor.fetchone()
    if not remaining:
        raise EvolutionError(409, "El inventario cambió; vuelve a verificar")

    changes = {
        "pokemon_id": DEST_SPECIES, "forma_id": DEST_FORM,
        "hp_actual": stats["hp_max"], "hp_max": stats["hp_max"],
        "ataque": stats["ataque"], "defensa": stats["defensa"],
        "velocidad": stats["velocidad"],
    }
    for field in ("ataque_especial", "defensa_especial"):
        if field in capture:
            changes[field] = stats[field]
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = %s").format(sql.Identifier(k)) for k in changes
    )
    cursor.execute(sql.SQL("""
        UPDATE public.usuario_pokemon SET {}
        WHERE id=%s AND usuario_id=%s AND pokemon_id=%s AND forma_id=%s AND xmin::text=%s
        RETURNING id
    """).format(assignments), tuple(changes.values()) + (
        capture["id"], user_id, ORIGIN_SPECIES, ORIGIN_FORM, capture["evolution_state"]
    ))
    if not cursor.fetchone():
        raise EvolutionError(409, "La captura cambió; vuelve a verificar")

    # Never assign Ninetales Kanto learnset to the Alola form. Both forms remain
    # excluded from team/battle until the form-aware moves engine is complete.
    _registrar_actividad_evolucion(cursor, user_id, "item", DEST_SPECIES)
    return {
        "ok": True, "mensaje": "¡Vulpix de Alola evolucionó a Ninetales de Alola!",
        "usuario_pokemon_id": int(capture["id"]), "pokemon_id": DEST_SPECIES,
        "forma_id": DEST_FORM, "pokemon_nombre": "Ninetales (Alola)",
        "item_consumido": "Ice Stone", "item_restante": int(remaining["cantidad"]),
        "batalla_regional_pendiente": True,
    }


@router_alola_evolution_v4.post("/pokemon/evolucionar-item")
def evolucionar_alola_item(payload: EvolucionItemPayload, usuario=Depends(get_current_user)):
    validar_usuario_payload(payload.usuario_id, usuario["id"])
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        # Preliminary read is only for dispatch. Regional mutation uses a fresh
        # SELECT FOR UPDATE and revalidates the identity under the lock.
        capture = _fetch_capture(cursor, payload.usuario_pokemon_id, usuario["id"])
        if not capture or not _regional(capture):
            cursor.close()
            release_connection(conn)
            cursor = None
            conn = None
            return evolucionar_con_item_original(payload, usuario)
        capture = _fetch_capture(cursor, payload.usuario_pokemon_id, usuario["id"], lock=True)
        if not capture or not _regional(capture):
            raise EvolutionError(409, "El Pokémon cambió; vuelve a consultar la evolución")
        result = _evolve_regional(cursor, capture, payload, usuario["id"])
        conn.commit()
        return result
    except EvolutionError as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from None
    except errors.LockNotAvailable:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=409, detail="Evolución en curso; inténtalo de nuevo") from None
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    except Exception:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo evolucionar el Pokémon regional") from None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            release_connection(conn)

"""One operational source for evolution reads and writes: public.evolucion_reglas.

The caller owns the transaction. No connection, commit or credential is hidden here.
Only approved simple level/item rules are supported. Special methods and non-null
forms fail closed until their game policy is approved.
"""
from psycopg2 import errors, sql


class EvolutionError(Exception):
    def __init__(self, status_code, message):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def load_rules(cursor, pokemon_id=None, *, lock=False):
    where = " AND er.pokemon_id = %s" if pokemon_id is not None else ""
    params = (pokemon_id,) if pokemon_id is not None else ()
    cursor.execute("""
        SELECT er.id, er.pokemon_id, p1.nombre AS pokemon_nombre,
               er.evoluciona_a AS evolucion_id, p2.nombre AS evolucion_nombre,
               er.metodo, er.nivel_requerido, er.item_id, i.nombre AS item_nombre,
               er.prioridad, er.activo
        FROM public.evolucion_reglas er
        JOIN public.pokemon p1 ON p1.id = er.pokemon_id
        JOIN public.pokemon p2 ON p2.id = er.evoluciona_a
        LEFT JOIN public.items i ON i.id = er.item_id
        WHERE er.activo = TRUE
          AND ((er.metodo = 'nivel' AND er.nivel_requerido > 0 AND er.item_id IS NULL)
            OR (er.metodo = 'item' AND er.item_id IS NOT NULL
                AND er.nivel_requerido IS NULL AND i.id IS NOT NULL))
    """ + where + " ORDER BY er.prioridad, er.id, er.evoluciona_a"
        + (" FOR SHARE OF er" if lock else ""), params)
    return list(cursor.fetchall())


def catalog_option(rule):
    return {
        "id": rule["id"], "tipo_metodo": rule["metodo"],
        "evolucion_id": rule["evolucion_id"], "evoluciona_a": rule["evolucion_id"],
        "evolucion_nombre": rule["evolucion_nombre"],
        "nivel": rule["nivel_requerido"], "nivel_requerido": rule["nivel_requerido"],
        "item_id": rule["item_id"], "item_nombre": rule["item_nombre"],
        "prioridad": rule["prioridad"], "activo": bool(rule["activo"]),
    }


def inspect_capture(cursor, capture_id, user_id):
    cursor.execute("""
        SELECT up.*, up.xmin::text AS evolution_state, p.nombre
        FROM public.usuario_pokemon up JOIN public.pokemon p ON p.id = up.pokemon_id
        WHERE up.id = %s AND up.usuario_id = %s
    """, (capture_id, user_id))
    capture = cursor.fetchone()
    if not capture:
        return {"puede_evolucionar": False, "listo_ahora": False,
                "motivo": "Pokémon no encontrado", "opciones": []}
    common = {"pokemon_actual": capture["nombre"], "pokemon_id": capture["pokemon_id"],
              "nivel_actual": capture["nivel"], "estado_evolucion": capture["evolution_state"]}
    if capture.get("forma_id") is not None:
        return dict(common, puede_evolucionar=False, listo_ahora=False, opciones=[],
                    motivo="La evolución de formas alternativas aún no está habilitada")
    rules = load_rules(cursor, capture["pokemon_id"])
    cursor.execute("SELECT item_id, cantidad FROM public.usuario_items WHERE usuario_id = %s", (user_id,))
    inventory = {r["item_id"]: max(0, int(r["cantidad"] or 0)) for r in cursor.fetchall()}
    options = []
    for rule in rules:
        quantity = inventory.get(rule["item_id"], 0) if rule["metodo"] == "item" else 0
        ready = (capture["nivel"] >= rule["nivel_requerido"] if rule["metodo"] == "nivel" else quantity > 0)
        options.append(dict(catalog_option(rule), tipo=rule["metodo"], cantidad=quantity,
                            tiene_item=quantity > 0, listo=ready))
    # Legacy clients choose the first option of `tipo`; keep that choice executable.
    options.sort(key=lambda r: (not r["listo"], r["prioridad"], r["id"], r["evolucion_id"]))
    return dict(common, puede_evolucionar=bool(options), listo_ahora=any(r["listo"] for r in options),
                tipo=options[0]["tipo"] if options else None, opciones=options,
                motivo=None if options else "Este Pokémon no tiene evolución habilitada")


def evolve_capture(cursor, *, user_id, capture_id, method, item_id=None, target_id=None,
                   expected_species=None, expected_state=None,
                   calculate_stats, sync_moves, record_activity):
    """Validate and mutate exactly one capture atomically in the caller transaction."""
    try:
        cursor.execute("""
            SELECT up.*, up.xmin::text AS evolution_state
            FROM public.usuario_pokemon up
            WHERE up.id = %s AND up.usuario_id = %s
            FOR UPDATE NOWAIT
        """, (capture_id, user_id))
        capture = cursor.fetchone()
        if not capture:
            raise EvolutionError(404, "Pokémon no encontrado")
        if ((expected_species is not None and expected_species != capture["pokemon_id"])
                or (expected_state is not None and expected_state != capture["evolution_state"])):
            raise EvolutionError(409, "El Pokémon cambió; vuelve a comprobar la evolución")
        if capture.get("forma_id") is not None:
            raise EvolutionError(409, "La evolución de formas alternativas aún no está habilitada")
        rules = [r for r in load_rules(cursor, capture["pokemon_id"], lock=True) if r["metodo"] == method]
        if method == "item":
            rules = [r for r in rules if r["item_id"] == item_id]
        if target_id is not None:
            rules = [r for r in rules if r["evolucion_id"] == target_id]
        if not rules:
            raise EvolutionError(400, "Este Pokémon no puede evolucionar con el método o destino solicitado")
        if len(rules) != 1:
            raise EvolutionError(409, "Selecciona un destino de evolución válido; hay varias reglas posibles")
        rule = rules[0]
        if method == "nivel" and capture["nivel"] < rule["nivel_requerido"]:
            raise EvolutionError(400, "Este Pokémon aún no puede evolucionar por nivel")
        inventory = None
        if method == "item":
            cursor.execute("""
                SELECT id, cantidad FROM public.usuario_items
                WHERE usuario_id = %s AND item_id = %s FOR UPDATE NOWAIT
            """, (user_id, item_id))
            inventory = cursor.fetchone()
            if not inventory or int(inventory["cantidad"] or 0) <= 0:
                raise EvolutionError(400, "No tienes ese item")
        cursor.execute("SELECT p.* FROM public.pokemon p WHERE p.id = %s FOR SHARE", (rule["evolucion_id"],))
        base = cursor.fetchone()
        if not base:
            raise EvolutionError(409, "La evolución configurada no existe")
        if base["id"] == capture["pokemon_id"]:
            raise EvolutionError(409, "La regla de evolución no cambia la especie")
        stats = calculate_stats(
            base["hp"], base["ataque"], base["defensa"], base["velocidad"],
            capture["nivel"], bool(capture["es_shiny"]),
            capture.get("bonus_hp_renacer", 0), capture.get("bonus_ataque_renacer", 0),
            capture.get("bonus_defensa_renacer", 0), capture.get("bonus_velocidad_renacer", 0),
            base.get("ataque_especial", base["ataque"]), base.get("defensa_especial", base["defensa"]),
            capture.get("bonus_ataque_especial_renacer", 0), capture.get("bonus_defensa_especial_renacer", 0),
        )
        if inventory:
            cursor.execute("""
                UPDATE public.usuario_items SET cantidad = cantidad - 1
                WHERE id = %s AND usuario_id = %s AND item_id = %s AND cantidad > 0
                RETURNING cantidad
            """, (inventory["id"], user_id, item_id))
            if not cursor.fetchone():
                raise EvolutionError(409, "El inventario cambió; vuelve a comprobar la evolución")
        changes = {"pokemon_id": rule["evolucion_id"], "hp_actual": stats["hp_max"],
                   "hp_max": stats["hp_max"], "ataque": stats["ataque"],
                   "defensa": stats["defensa"], "velocidad": stats["velocidad"]}
        for field in ("ataque_especial", "defensa_especial"):
            if field in capture:
                changes[field] = stats[field]
        assignments = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in changes)
        cursor.execute(sql.SQL("""
            UPDATE public.usuario_pokemon SET {}
            WHERE id = %s AND usuario_id = %s AND pokemon_id = %s AND xmin::text = %s
            RETURNING id
        """).format(assignments), tuple(changes.values()) +
                       (capture_id, user_id, capture["pokemon_id"], capture["evolution_state"]))
        if not cursor.fetchone():
            raise EvolutionError(409, "El Pokémon cambió; vuelve a comprobar la evolución")
        sync_moves(cursor, capture_id, rule["evolucion_id"], capture["nivel"], origen="nivel", auto_equipar=True)
        record_activity(cursor, user_id, method, rule["evolucion_id"])
        return {"ok": True, "mensaje": "¡Tu Pokémon evolucionó con éxito!",
                "usuario_pokemon_id": capture_id, "pokemon_id": rule["evolucion_id"],
                "pokemon_nombre": base["nombre"]}
    except errors.LockNotAvailable as exc:
        raise EvolutionError(409, "Ya hay una evolución o cambio de inventario en curso; vuelve a comprobar") from exc

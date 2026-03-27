import json
from datetime import datetime, timedelta, timezone
from typing import Any




UTC = timezone.utc


def utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def serializar_datetime_api(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value_utc = value.replace(tzinfo=UTC)
    else:
        value_utc = value.astimezone(UTC)
    return value_utc.isoformat().replace("+00:00", "Z")


def existe_tabla_local(cursor, tabla: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) AS regclass", (f"public.{tabla}",))
    row = cursor.fetchone() or {}
    return bool(row.get("regclass"))


def columna_existe_local(cursor, tabla: str, columna: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (tabla, columna)
    )
    return cursor.fetchone() is not None


def asegurar_tablas_monetizacion(cursor):
    tablas = (
        "monetizacion_productos",
        "usuario_compras",
        "usuario_beneficios",
        "payment_webhook_eventos",
    )
    faltantes = [tabla for tabla in tablas if not existe_tabla_local(cursor, tabla)]
    if faltantes:
        raise RuntimeError(f"Faltan tablas de monetización: {', '.join(faltantes)}")


def _normalizar_metadata(metadata: Any) -> dict:
    if not metadata:
        return {}
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str):
        try:
            data = json.loads(metadata)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def limpiar_beneficios_expirados(cursor, usuario_id: int | None = None):
    if not existe_tabla_local(cursor, "usuario_beneficios"):
        return

    ahora_utc = utc_now_naive()

    if usuario_id is None:
        cursor.execute(
            """
            UPDATE usuario_beneficios
            SET estado = 'expirado', actualizado_en = NOW()
            WHERE estado = 'activo'
              AND expira_en IS NOT NULL
              AND expira_en <= %s
            """,
            (ahora_utc,)
        )
    else:
        cursor.execute(
            """
            UPDATE usuario_beneficios
            SET estado = 'expirado', actualizado_en = NOW()
            WHERE usuario_id = %s
              AND estado = 'activo'
              AND expira_en IS NOT NULL
              AND expira_en <= %s
            """,
            (int(usuario_id), ahora_utc)
        )


def obtener_beneficios_activos(cursor, usuario_id: int) -> list[dict]:
    if not existe_tabla_local(cursor, "usuario_beneficios"):
        return []

    limpiar_beneficios_expirados(cursor, usuario_id)
    ahora_utc = utc_now_naive()
    cursor.execute(
        """
        SELECT id, beneficio_codigo, estado, inicia_en, expira_en, usos_totales, usos_consumidos, metadata
        FROM usuario_beneficios
        WHERE usuario_id = %s
          AND estado = 'activo'
          AND (expira_en IS NULL OR expira_en > %s)
        ORDER BY id DESC
        """,
        (int(usuario_id), ahora_utc)
    )
    rows = cursor.fetchall() or []
    beneficios = []
    for row in rows:
        beneficios.append({
            "id": int(row["id"]),
            "beneficio_codigo": row["beneficio_codigo"],
            "estado": row["estado"],
            "inicia_en": serializar_datetime_api(row.get("inicia_en")),
            "expira_en": serializar_datetime_api(row.get("expira_en")),
            "usos_totales": int(row["usos_totales"] or 0) if row.get("usos_totales") is not None else None,
            "usos_consumidos": int(row["usos_consumidos"] or 0),
            "metadata": _normalizar_metadata(row.get("metadata")),
        })
    return beneficios


def usuario_tiene_beneficio_activo(cursor, usuario_id: int, beneficio_codigo: str) -> bool:
    if not existe_tabla_local(cursor, "usuario_beneficios"):
        return False
    limpiar_beneficios_expirados(cursor, usuario_id)
    ahora_utc = utc_now_naive()
    cursor.execute(
        """
        SELECT 1
        FROM usuario_beneficios
        WHERE usuario_id = %s
          AND beneficio_codigo = %s
          AND estado = 'activo'
          AND (expira_en IS NULL OR expira_en > %s)
        LIMIT 1
        """,
        (int(usuario_id), str(beneficio_codigo), ahora_utc)
    )
    return cursor.fetchone() is not None


def activar_o_extender_beneficio(
    cursor,
    usuario_id: int,
    beneficio_codigo: str,
    *,
    compra_id: int | None = None,
    origen_tipo: str = "compra",
    duracion_horas: int = 0,
    duracion_dias: int = 0,
    duracion_meses: int = 0,
    metadata: dict | None = None,
):
    asegurar_tablas_monetizacion(cursor)
    limpiar_beneficios_expirados(cursor, usuario_id)

    if duracion_horas < 0 or duracion_dias < 0 or duracion_meses < 0:
        raise ValueError("La duración del beneficio no puede ser negativa")

    extra_dias = int(duracion_dias) + (int(duracion_meses) * 30)
    delta = timedelta(hours=int(duracion_horas), days=extra_dias)
    ahora_utc = utc_now_naive()
    expira_en = None if delta.total_seconds() <= 0 else ahora_utc + delta

    cursor.execute(
        """
        SELECT id, expira_en, metadata
        FROM usuario_beneficios
        WHERE usuario_id = %s
          AND beneficio_codigo = %s
          AND estado = 'activo'
        ORDER BY id DESC
        LIMIT 1
        FOR UPDATE
        """,
        (int(usuario_id), str(beneficio_codigo))
    )
    actual = cursor.fetchone()
    metadata_json = json.dumps(metadata or {})

    if actual:
        base_expira = actual.get("expira_en") or ahora_utc
        if expira_en is None:
            nueva_expira = None
        else:
            base = base_expira if base_expira > ahora_utc else ahora_utc
            nueva_expira = base + delta

        cursor.execute(
            """
            UPDATE usuario_beneficios
            SET compra_id = COALESCE(%s, compra_id),
                origen_tipo = %s,
                expira_en = %s,
                metadata = %s::jsonb,
                actualizado_en = NOW()
            WHERE id = %s
            RETURNING *
            """,
            (compra_id, origen_tipo, nueva_expira, metadata_json, int(actual["id"]))
        )
        return cursor.fetchone()

    cursor.execute(
        """
        INSERT INTO usuario_beneficios
            (usuario_id, compra_id, beneficio_codigo, origen_tipo, estado, inicia_en, expira_en, metadata)
        VALUES
            (%s, %s, %s, %s, 'activo', %s, %s, %s::jsonb)
        RETURNING *
        """,
        (int(usuario_id), compra_id, str(beneficio_codigo), origen_tipo, ahora_utc, expira_en, metadata_json)
    )
    return cursor.fetchone()


def resolver_item_por_codigo(cursor, item_codigo: str):
    if not item_codigo:
        return None

    if columna_existe_local(cursor, "items", "codigo"):
        cursor.execute(
            """
            SELECT id, nombre, codigo, tipo, descripcion
            FROM items
            WHERE codigo = %s
            LIMIT 1
            """,
            (str(item_codigo),)
        )
        row = cursor.fetchone()
        if row:
            return row

    cursor.execute(
        """
        SELECT id, nombre, NULL::varchar AS codigo, tipo, descripcion
        FROM items
        WHERE lower(nombre) = lower(%s)
        LIMIT 1
        """,
        (str(item_codigo),)
    )
    return cursor.fetchone()


def agregar_item_usuario(cursor, usuario_id: int, item_id: int, cantidad: int):
    if int(cantidad or 0) <= 0:
        return
    cursor.execute(
        """
        INSERT INTO usuario_items (usuario_id, item_id, cantidad)
        VALUES (%s, %s, %s)
        ON CONFLICT (usuario_id, item_id)
        DO UPDATE SET cantidad = usuario_items.cantidad + EXCLUDED.cantidad
        """,
        (int(usuario_id), int(item_id), int(cantidad))
    )


def consumir_item_usuario_por_codigo(cursor, usuario_id: int, item_codigo: str, cantidad: int = 1):
    item = resolver_item_por_codigo(cursor, item_codigo)
    if not item:
        return None

    cursor.execute(
        """
        SELECT id, usuario_id, item_id, cantidad
        FROM usuario_items
        WHERE usuario_id = %s
          AND item_id = %s
        LIMIT 1
        FOR UPDATE
        """,
        (int(usuario_id), int(item["id"]))
    )
    row = cursor.fetchone()
    if not row or int(row.get("cantidad") or 0) < int(cantidad):
        return None

    nueva_cantidad = int(row["cantidad"]) - int(cantidad)
    if nueva_cantidad <= 0:
        cursor.execute("DELETE FROM usuario_items WHERE id = %s", (int(row["id"]),))
    else:
        cursor.execute("UPDATE usuario_items SET cantidad = %s WHERE id = %s", (nueva_cantidad, int(row["id"])))

    return {
        "inventario_id": int(row["id"]),
        "item_id": int(item["id"]),
        "item_codigo": item.get("codigo"),
        "item_nombre": item["nombre"],
        "cantidad_restante": max(0, nueva_cantidad),
    }


def obtener_multiplicadores_recompensa_batalla(cursor, usuario_id: int) -> dict:
    exp_multiplier = 1
    gold_multiplier = 1
    beneficios = []

    if usuario_tiene_beneficio_activo(cursor, usuario_id, "battle_exp_x2"):
        exp_multiplier *= 2
        beneficios.append("battle_exp_x2")

    if usuario_tiene_beneficio_activo(cursor, usuario_id, "battle_gold_x2"):
        gold_multiplier *= 2
        beneficios.append("battle_gold_x2")

    return {
        "exp_multiplier": exp_multiplier,
        "gold_multiplier": gold_multiplier,
        "beneficios_activos": beneficios,
    }


def aplicar_multiplicadores_recompensa_batalla(cursor, usuario_id: int, exp_base: int, pokedolares_base: int) -> dict:
    mult = obtener_multiplicadores_recompensa_batalla(cursor, usuario_id)
    exp_final = int(exp_base or 0) * int(mult["exp_multiplier"])
    gold_final = int(pokedolares_base or 0) * int(mult["gold_multiplier"])
    return {
        **mult,
        "exp_base": int(exp_base or 0),
        "pokedolares_base": int(pokedolares_base or 0),
        "exp_final": int(exp_final),
        "pokedolares_final": int(gold_final),
    }

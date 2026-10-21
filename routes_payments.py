import json
import os
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from auth import get_current_user
from routes_daily_shiny import PRODUCT_CODE as DAILY_PRODUCT_CODE, configured_ids, stable_choice
from server_time import server_now
from routes_pokemon import (calcular_stats, calcular_experiencia_total_base, sincronizar_movimientos_usuario_pokemon)
from database import get_connection, get_cursor, release_connection
from monetization_utils import (
    activar_o_extender_beneficio,
    agregar_item_usuario,
    asegurar_tablas_monetizacion,
    consumir_item_usuario_por_codigo,
    existe_tabla_local,
    obtener_beneficios_activos,
    resolver_item_por_codigo,
    serializar_datetime_api,
)

router_payments = APIRouter()

PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "").strip()
PAYPAL_SECRET = os.getenv("PAYPAL_SECRET", "").strip()
PAYPAL_BASE_URL = os.getenv("PAYPAL_BASE_URL", "https://api-m.sandbox.paypal.com").strip().rstrip("/")
PAYPAL_BRAND_NAME = os.getenv("PAYPAL_BRAND_NAME", "Mastersmon").strip() or "Mastersmon"
PAYPAL_RETURN_URL = os.getenv("PAYPAL_RETURN_URL", "https://mastersmon.com/payment-success.html").strip()
PAYPAL_CANCEL_URL = os.getenv("PAYPAL_CANCEL_URL", "https://mastersmon.com/payment-cancel.html").strip()
PAYPAL_WEBHOOK_ID = os.getenv("PAYPAL_WEBHOOK_ID", "").strip()

PRODUCT_TYPES_WITH_SINGLE_LIMIT = {"suscripcion", "battle_pass", "beneficio"}
BOOSTER_ITEM_TO_BENEFIT = {
    "booster_battle_exp_x2_24h": {"beneficio_codigo": "battle_exp_x2", "duracion_horas": 24},
    "booster_battle_gold_x2_24h": {"beneficio_codigo": "battle_gold_x2", "duracion_horas": 24},
}


# JL: Legacy recovery is intentionally restricted to the known, paid historical order.
LEGACY_RECOVERY_PURCHASE_ID = 22
LEGACY_RECOVERY_USER_ID = 1
LEGACY_RECOVERY_POKEMON_ID = 245  # Suicune, documented in the original checkout
LEGACY_RECOVERY_AMOUNT = Decimal("0.50")
LEGACY_RECOVERY_CONFIRMATION = "ENTREGAR SUICUNE COMPRA 22"


class AdminRecoveryConfirmation(BaseModel):
    confirmacion: str


class CreatePayPalOrderPayload(BaseModel):
    product_code: str
    quantity: int = 1
    confirmacion_aceptada: bool = False
    version_terminos: str = "v1"
    daily_pokemon_id: int | None = None
    daily_offer_date: str | None = None


class CapturePayPalOrderPayload(BaseModel):
    compra_id: int
    paypal_order_id: str


class UseBoosterPayload(BaseModel):
    item_code: str


def _paypal_enabled() -> bool:
    return bool(PAYPAL_CLIENT_ID and PAYPAL_SECRET)


def _ensure_paypal_enabled():
    if not _paypal_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PayPal no está configurado todavía en el backend"
        )


def _paypal_access_token() -> str:
    _ensure_paypal_enabled()
    response = requests.post(
        f"{PAYPAL_BASE_URL}/v1/oauth2/token",
        headers={"Accept": "application/json", "Accept-Language": "es_PE"},
        data={"grant_type": "client_credentials"},
        auth=(PAYPAL_CLIENT_ID, PAYPAL_SECRET),
        timeout=25,
    )
    if response.status_code >= 300:
        raise HTTPException(status_code=502, detail="No se pudo autenticar con PayPal")
    data = response.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(status_code=502, detail="PayPal no devolvió access_token")
    return token


def _paypal_headers(token: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _json_safe(value: Any) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            data = json.loads(value)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _obtener_producto_por_codigo(cursor, product_code: str):
    cursor.execute(
        """
        SELECT *
        FROM monetizacion_productos
        WHERE codigo = %s
          AND activo = TRUE
        LIMIT 1
        """,
        (str(product_code),)
    )
    return cursor.fetchone()


def _contar_compras_pagadas_producto(cursor, usuario_id: int, producto_id: int) -> int:
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM usuario_compras
        WHERE usuario_id = %s
          AND producto_id = %s
          AND estado = 'pagado'
        """,
        (int(usuario_id), int(producto_id))
    )
    row = cursor.fetchone() or {}
    return int(row.get("total") or 0)


def _validar_limite_producto(cursor, usuario_id: int, producto: dict):
    limite = producto.get("limite_por_cuenta")
    if limite is None:
        return
    total = _contar_compras_pagadas_producto(cursor, usuario_id, int(producto["id"]))
    if total >= int(limite):
        raise HTTPException(status_code=409, detail="Ya alcanzaste el límite de compra para este producto")


def _oferta_shiny_actual(cursor):
    """Fija la misma rotación que GET /payments/legendario-del-dia."""
    fecha = server_now().date()
    ids = configured_ids()
    cursor.execute("SELECT id, nombre FROM pokemon WHERE id = ANY(%s)", (list(ids),))
    encontrados = {int(x["id"]): x for x in (cursor.fetchall() or [])}
    disponibles = tuple(i for i in ids if i in encontrados)
    if not disponibles:
        raise HTTPException(status_code=503, detail="No hay legendarios Shiny disponibles")
    elegido = stable_choice(disponibles, fecha)
    return {"pokemon_id": elegido, "nombre": str(encontrados[elegido]["nombre"]),
            "fecha": fecha.isoformat(), "es_shiny": True}


def _verificar_captura_paypal(orden: dict, compra: dict, capture_id_esperado=None) -> str:
    """Sólo entrega tras una captura COMPLETED de la orden y del importe registrados."""
    if str(orden.get("status") or "").upper() != "COMPLETED":
        raise HTTPException(status_code=409, detail="La orden PayPal aún no está completada")
    unidades = orden.get("purchase_units") or []
    if len(unidades) != 1:
        raise HTTPException(status_code=409, detail="Cantidad de unidades PayPal inválida")
    unidad = unidades[0]
    cid = str(compra["id"])
    if str(unidad.get("custom_id") or "") != cid or str(unidad.get("invoice_id") or "") != f"MSM-{cid}":
        raise HTTPException(status_code=409, detail="La orden PayPal no corresponde a la compra")
    capturas = (unidad.get("payments") or {}).get("captures") or []
    correctas = [x for x in capturas if str(x.get("status") or "").upper() == "COMPLETED"]
    if len(correctas) != 1:
        raise HTTPException(status_code=409, detail="PayPal no confirmó una captura válida")
    captura = correctas[0]
    importe = captura.get("amount") or {}
    try:
        valor = Decimal(str(importe["value"]))
        esperado = Decimal(str(compra["monto"]))
    except (InvalidOperation, ValueError, TypeError, KeyError):
        raise HTTPException(status_code=409, detail="Importe PayPal inválido")
    if valor != esperado or str(importe.get("currency_code") or "") != str(compra["moneda"]):
        raise HTTPException(status_code=409, detail="Importe o moneda PayPal no coinciden")
    captura_id = str(captura.get("id") or "")
    if not captura_id or (capture_id_esperado and captura_id != str(capture_id_esperado)):
        raise HTTPException(status_code=409, detail="Identificador de captura incorrecto")
    return captura_id


def _entregar_shiny_reservado(cursor, compra: dict) -> dict:
    reserva = _json_safe(compra.get("metadata")).get("daily_shiny")
    if not isinstance(reserva, dict) or not reserva.get("pokemon_id"):
        # Las compras anteriores a esta versión no fijaban especie: no se adivina.
        raise HTTPException(status_code=409, detail="Compra anterior sin especie reservada: requiere recuperación supervisada")
    pokemon_id = int(reserva["pokemon_id"])
    if pokemon_id not in configured_ids() or reserva.get("es_shiny") is not True:
        raise HTTPException(status_code=409, detail="Pokémon reservado no autorizado")
    cursor.execute("""SELECT id, nombre, hp, ataque, defensa, velocidad, ataque_especial, defensa_especial
                      FROM pokemon WHERE id=%s LIMIT 1""", (pokemon_id,))
    pokemon = cursor.fetchone()
    if not pokemon:
        raise HTTPException(status_code=409, detail="Pokémon reservado no encontrado")
    nivel = 5
    stats = calcular_stats(pokemon["hp"], pokemon["ataque"], pokemon["defensa"],
                           pokemon["velocidad"], nivel, es_shiny=True,
                           base_ataque_especial=pokemon["ataque_especial"],
                           base_defensa_especial=pokemon["defensa_especial"])
    cursor.execute("""INSERT INTO usuario_pokemon
        (usuario_id,pokemon_id,nivel,experiencia,experiencia_total,es_shiny,
         hp_actual,hp_max,ataque,defensa,velocidad,ataque_especial,defensa_especial,
         bonus_hp_renacer,bonus_ataque_renacer,bonus_defensa_renacer,bonus_velocidad_renacer,victorias_total)
        VALUES (%s,%s,%s,0,%s,TRUE,%s,%s,%s,%s,%s,%s,%s,0,0,0,0,0) RETURNING id""",
        (int(compra["usuario_id"]), pokemon_id, nivel, calcular_experiencia_total_base(nivel),
         stats["hp_max"], stats["hp_max"], stats["ataque"], stats["defensa"],
         stats["velocidad"], stats["ataque_especial"], stats["defensa_especial"]))
    nuevo_id = int(cursor.fetchone()["id"])
    sincronizar_movimientos_usuario_pokemon(cursor, nuevo_id, pokemon_id, nivel,
                                             origen="compra", auto_equipar=True)
    return {"usuario_pokemon_id": nuevo_id, "pokemon_id": pokemon_id,
            "nombre": str(pokemon["nombre"]), "es_shiny": True, "nivel": nivel,
            "fecha_oferta": str(reserva.get("fecha") or "")}


def _crear_compra_pendiente(cursor, usuario: dict, producto: dict, payload: CreatePayPalOrderPayload, request: Request):
    monto = float(producto["precio_usd"])
    if int(payload.quantity or 1) != 1:
        raise HTTPException(status_code=400, detail="Por ahora quantity debe ser 1")

    consentimiento_ip = request.client.host if request.client else None
    consentimiento_ua = request.headers.get("user-agent", "")[:400]
    idempotency_key = str(uuid.uuid4())

    cursor.execute(
        """
        INSERT INTO usuario_compras
            (usuario_id, producto_id, estado, moneda, monto, proveedor_pago, metadata,
             consentimiento_version, consentimiento_aceptado_en, consentimiento_ip, consentimiento_user_agent, idempotency_key)
        VALUES
            (%s, %s, 'pendiente', 'USD', %s, 'paypal', %s::jsonb,
             %s, NOW(), %s, %s, %s)
        RETURNING *
        """,
        (
            int(usuario["id"]),
            int(producto["id"]),
            monto,
            json.dumps({"product_code": producto["codigo"], "quantity": 1}),
            payload.version_terminos,
            consentimiento_ip,
            consentimiento_ua,
            idempotency_key,
        )
    )
    return cursor.fetchone()


def _crear_orden_paypal(compra: dict, producto: dict):
    token = _paypal_access_token()
    monto = f"{Decimal(str(compra['monto'])):.2f}"
    body = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "reference_id": f"compra-{compra['id']}",
                "custom_id": str(compra["id"]),
                "invoice_id": f"MSM-{compra['id']}",
                "description": producto["nombre"],
                "amount": {
                    "currency_code": "USD",
                    "value": monto,
                },
            }
        ],
        "application_context": {
            "brand_name": PAYPAL_BRAND_NAME,
            "user_action": "PAY_NOW",
            "return_url": PAYPAL_RETURN_URL,
            "cancel_url": PAYPAL_CANCEL_URL,
        },
    }
    response = requests.post(
        f"{PAYPAL_BASE_URL}/v2/checkout/orders",
        headers={**_paypal_headers(token), "PayPal-Request-Id": str(compra["idempotency_key"])},
        json=body,
        timeout=30,
    )
    if response.status_code >= 300:
        # El detalle interno se registra en Render, nunca se expone el token al cliente.
        print("PayPal create HTTP:", response.status_code, response.text[:900])
        raise HTTPException(status_code=502, detail="PayPal rechazó la creación de la orden. Revisa el log del backend")
    data = response.json()
    return data


def _extraer_link_paypal(order_data: dict, rel: str):
    for link in order_data.get("links", []) or []:
        if link.get("rel") == rel:
            return link.get("href")
    return None


def _capturar_orden_paypal(order_id: str):
    token = _paypal_access_token()
    response = requests.post(
        f"{PAYPAL_BASE_URL}/v2/checkout/orders/{order_id}/capture",
        headers=_paypal_headers(token),
        json={},
        timeout=30,
    )
    if response.status_code >= 300:
        raise HTTPException(status_code=502, detail="No se pudo capturar la orden PayPal")
    return response.json()


def _obtener_orden_paypal(order_id: str):
    token = _paypal_access_token()
    response = requests.get(
        f"{PAYPAL_BASE_URL}/v2/checkout/orders/{order_id}",
        headers=_paypal_headers(token),
        timeout=30,
    )
    if response.status_code >= 300:
        raise HTTPException(status_code=502, detail="No se pudo consultar la orden PayPal")
    return response.json()


def _extraer_capture_id_paypal(order_data: dict) -> str | None:
    for pu in order_data.get("purchase_units", []) or []:
        payments = pu.get("payments") or {}
        captures = payments.get("captures") or []
        if captures:
            return captures[0].get("id")
    return None


def _verificar_webhook_paypal(headers: dict, body: dict) -> bool | None:
    if not PAYPAL_WEBHOOK_ID:
        return None
    token = _paypal_access_token()
    payload = {
        "auth_algo": headers.get("paypal-auth-algo"),
        "cert_url": headers.get("paypal-cert-url"),
        "transmission_id": headers.get("paypal-transmission-id"),
        "transmission_sig": headers.get("paypal-transmission-sig"),
        "transmission_time": headers.get("paypal-transmission-time"),
        "webhook_id": PAYPAL_WEBHOOK_ID,
        "webhook_event": body,
    }
    response = requests.post(
        f"{PAYPAL_BASE_URL}/v1/notifications/verify-webhook-signature",
        headers=_paypal_headers(token),
        json=payload,
        timeout=30,
    )
    if response.status_code >= 300:
        return False
    result = response.json()
    return str(result.get("verification_status", "")).upper() == "SUCCESS"


def _marcar_compra_pagada(cursor, compra_id: int, paypal_order_id: str | None, paypal_capture_id: str | None, raw_payload: dict | None = None):
    metadata_actualizar = json.dumps(raw_payload or {})
    cursor.execute(
        """
        UPDATE usuario_compras
        SET estado = 'pagado',
            pagado_en = COALESCE(pagado_en, NOW()),
            proveedor_pago = 'paypal',
            paypal_order_id = COALESCE(%s, paypal_order_id),
            paypal_capture_id = COALESCE(%s, paypal_capture_id),
            metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
            actualizado_en = NOW()
        WHERE id = %s
        RETURNING *
        """,
        (paypal_order_id, paypal_capture_id, metadata_actualizar, int(compra_id))
    )
    return cursor.fetchone()


def _entregar_producto_pagado(cursor, compra: dict, producto: dict):
    if compra.get("entregado_en") is not None and compra.get("grant_status") == "entregado":
        return {"ya_entregado": True, "grant_status": compra.get("grant_status")}

    metadata = _json_safe(producto.get("metadata"))
    beneficio_codigo = producto.get("beneficio_codigo")
    grant_status = "entregado"
    grant_payload: dict[str, Any] = {"product_code": producto["codigo"]}

    if producto["tipo"] in ("suscripcion", "battle_pass", "beneficio") and beneficio_codigo:
        beneficio = activar_o_extender_beneficio(
            cursor,
            int(compra["usuario_id"]),
            str(beneficio_codigo),
            compra_id=int(compra["id"]),
            origen_tipo="compra",
            duracion_horas=int(producto.get("duracion_horas") or 0),
            duracion_dias=int(producto.get("duracion_dias") or 0),
            duracion_meses=int(producto.get("duracion_meses") or 0),
            metadata={"producto_codigo": producto["codigo"], **metadata},
        )
        grant_payload["beneficio_codigo"] = beneficio_codigo
        grant_payload["expira_en"] = serializar_datetime_api(beneficio.get("expira_en"))

    elif producto["tipo"] == "pack_item":
        item_code = str(metadata.get("grant_item_codigo") or "").strip()
        cantidad = int(metadata.get("cantidad") or 0)
        if not item_code or cantidad <= 0:
            raise HTTPException(status_code=500, detail="El producto pack_item no tiene metadata válida")
        item = resolver_item_por_codigo(cursor, item_code)
        if not item:
            raise HTTPException(status_code=500, detail=f"No existe el item configurado para el pack: {item_code}")
        agregar_item_usuario(cursor, int(compra["usuario_id"]), int(item["id"]), cantidad)
        grant_payload["items_entregados"] = [{
            "item_id": int(item["id"]),
            "item_codigo": item.get("codigo"),
            "item_nombre": item["nombre"],
            "cantidad": cantidad,
        }]

    elif producto["tipo"] == "pokemon_exclusivo" and producto["codigo"] == DAILY_PRODUCT_CODE:
        grant_payload.update(_entregar_shiny_reservado(cursor, compra))
    elif producto["tipo"] == "pokemon_exclusivo":
        raise HTTPException(status_code=409, detail="El producto exclusivo no tiene entrega habilitada")

    else:
        grant_payload["message"] = "Compra registrada sin entrega automática específica"

    cursor.execute(
        """
        UPDATE usuario_compras
        SET grant_status = %s,
            grant_payload = %s::jsonb,
            entregado_en = CASE WHEN %s = 'entregado' THEN COALESCE(entregado_en, NOW()) ELSE entregado_en END,
            actualizado_en = NOW()
        WHERE id = %s
        RETURNING *
        """,
        (grant_status, json.dumps(grant_payload), grant_status, int(compra["id"]))
    )
    updated = cursor.fetchone()
    return {
        "ya_entregado": False,
        "grant_status": grant_status,
        "grant_payload": _json_safe(updated.get("grant_payload")),
    }


def _serializar_producto(row: dict) -> dict:
    return {
        "id": int(row["id"]),
        "codigo": row["codigo"],
        "nombre": row["nombre"],
        "tipo": row["tipo"],
        "precio_usd": float(row["precio_usd"]),
        "beneficio_codigo": row.get("beneficio_codigo"),
        "duracion_horas": int(row["duracion_horas"] or 0) if row.get("duracion_horas") is not None else None,
        "duracion_dias": int(row["duracion_dias"] or 0) if row.get("duracion_dias") is not None else None,
        "duracion_meses": int(row["duracion_meses"] or 0) if row.get("duracion_meses") is not None else None,
        "limite_por_cuenta": int(row["limite_por_cuenta"] or 0) if row.get("limite_por_cuenta") is not None else None,
        "metadata": _json_safe(row.get("metadata")),
    }


@router_payments.get("/payments/catalogo")
def obtener_catalogo_pagos(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)
        cursor.execute(
            """
            SELECT *
            FROM monetizacion_productos
            WHERE activo = TRUE
            ORDER BY sort_order ASC, id ASC
            """
        )
        productos = [_serializar_producto(row) for row in (cursor.fetchall() or [])]
        beneficios = obtener_beneficios_activos(cursor, int(usuario["id"]))
        return {"ok": True, "productos": productos, "beneficios_activos": beneficios}
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.get("/payments/beneficios/activos")
def obtener_beneficios_usuario(usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)
        beneficios = obtener_beneficios_activos(cursor, int(usuario["id"]))
        return {"ok": True, "beneficios": beneficios}
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.get("/payments/compras")
def obtener_mis_compras(limit: int = 20, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)
        limit = max(1, min(100, int(limit or 20)))
        cursor.execute(
            """
            SELECT uc.*, mp.codigo AS producto_codigo, mp.nombre AS producto_nombre
            FROM usuario_compras uc
            JOIN monetizacion_productos mp ON mp.id = uc.producto_id
            WHERE uc.usuario_id = %s
            ORDER BY uc.id DESC
            LIMIT %s
            """,
            (int(usuario["id"]), limit)
        )
        compras = []
        for row in cursor.fetchall() or []:
            compras.append({
                "id": int(row["id"]),
                "producto_codigo": row["producto_codigo"],
                "producto_nombre": row["producto_nombre"],
                "estado": row["estado"],
                "monto": float(row["monto"]),
                "moneda": row["moneda"],
                "grant_status": row.get("grant_status"),
                "paypal_order_id": row.get("paypal_order_id"),
                "paypal_capture_id": row.get("paypal_capture_id"),
                "pagado_en": serializar_datetime_api(row.get("pagado_en")),
                "entregado_en": serializar_datetime_api(row.get("entregado_en")),
                "grant_payload": _json_safe(row.get("grant_payload")),
            })
        return {"ok": True, "compras": compras}
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.post("/payments/paypal/order/create")
def crear_orden_paypal(payload: CreatePayPalOrderPayload, request: Request, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)

        if not payload.confirmacion_aceptada:
            raise HTTPException(status_code=400, detail="Debes confirmar las condiciones antes de pagar")

        producto = _obtener_producto_por_codigo(cursor, payload.product_code)
        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado o inactivo")

        _validar_limite_producto(cursor, int(usuario["id"]), producto)
        compra = _crear_compra_pendiente(cursor, usuario, producto, payload, request)
        if str(producto["codigo"]) == DAILY_PRODUCT_CODE:
            oferta = _oferta_shiny_actual(cursor)
            if (payload.daily_pokemon_id is not None and int(payload.daily_pokemon_id) != oferta["pokemon_id"]
                    or payload.daily_offer_date is not None and str(payload.daily_offer_date) != oferta["fecha"]):
                raise HTTPException(status_code=409, detail="La oferta Shiny cambió. Actualiza la tienda")
            cursor.execute("""UPDATE usuario_compras
                              SET metadata=COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                              WHERE id=%s RETURNING *""",
                           (json.dumps({"daily_shiny": oferta}), int(compra["id"])))
            compra = cursor.fetchone()
        orden = _crear_orden_paypal(compra, producto)

        cursor.execute(
            """
            UPDATE usuario_compras
            SET referencia_externa = %s,
                paypal_order_id = %s,
                metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                actualizado_en = NOW()
            WHERE id = %s
            """,
            (
                orden.get("id"),
                orden.get("id"),
                json.dumps({"paypal_order_status": orden.get("status")}),
                int(compra["id"]),
            )
        )
        conn.commit()

        return {
            "ok": True,
            "compra_id": int(compra["id"]),
            "producto": _serializar_producto(producto),
            "paypal_order_id": orden.get("id"),
            "paypal_status": orden.get("status"),
            "approval_url": _extraer_link_paypal(orden, "approve"),
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /payments/paypal/order/create:", repr(error))
        raise HTTPException(status_code=500, detail="No se pudo crear la orden de PayPal")
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.post("/payments/paypal/order/capture")
def capturar_orden_paypal(payload: CapturePayPalOrderPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)
        cursor.execute(
            """
            SELECT
                uc.id AS compra_id,
                uc.usuario_id AS compra_usuario_id,
                uc.producto_id AS compra_producto_id,
                uc.estado AS compra_estado,
                uc.moneda AS compra_moneda,
                uc.monto AS compra_monto,
                uc.proveedor_pago AS compra_proveedor_pago,
                uc.referencia_externa AS compra_referencia_externa,
                uc.paypal_order_id AS compra_paypal_order_id,
                uc.paypal_capture_id AS compra_paypal_capture_id,
                uc.grant_status AS compra_grant_status,
                uc.grant_payload AS compra_grant_payload,
                uc.metadata AS compra_metadata,
                uc.pagado_en AS compra_pagado_en,
                uc.entregado_en AS compra_entregado_en,

                mp.id AS producto_id,
                mp.codigo AS producto_codigo,
                mp.nombre AS producto_nombre,
                mp.tipo AS producto_tipo,
                mp.precio_usd AS producto_precio_usd,
                mp.beneficio_codigo AS producto_beneficio_codigo,
                mp.duracion_horas AS producto_duracion_horas,
                mp.duracion_dias AS producto_duracion_dias,
                mp.duracion_meses AS producto_duracion_meses,
                mp.limite_por_cuenta AS producto_limite_por_cuenta,
                mp.metadata AS producto_metadata
            FROM usuario_compras uc
            JOIN monetizacion_productos mp ON mp.id = uc.producto_id
            WHERE uc.id = %s
            AND uc.usuario_id = %s
            LIMIT 1
            FOR UPDATE
            """,
            (int(payload.compra_id), int(usuario["id"]))
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Compra no encontrada")

        row = dict(row)

        compra = {
            "id": row["compra_id"],
            "usuario_id": row["compra_usuario_id"],
            "producto_id": row["compra_producto_id"],
            "estado": row["compra_estado"],
            "moneda": row["compra_moneda"],
            "monto": row["compra_monto"],
            "proveedor_pago": row["compra_proveedor_pago"],
            "referencia_externa": row["compra_referencia_externa"],
            "paypal_order_id": row["compra_paypal_order_id"],
            "paypal_capture_id": row["compra_paypal_capture_id"],
            "grant_status": row["compra_grant_status"],
            "grant_payload": row["compra_grant_payload"],
            "metadata": row["compra_metadata"],
            "pagado_en": row["compra_pagado_en"],
            "entregado_en": row["compra_entregado_en"],
        }

        producto = {
            "id": row["producto_id"],
            "codigo": row["producto_codigo"],
            "nombre": row["producto_nombre"],
            "tipo": row["producto_tipo"],
            "precio_usd": row["producto_precio_usd"],
            "beneficio_codigo": row["producto_beneficio_codigo"],
            "duracion_horas": row["producto_duracion_horas"],
            "duracion_dias": row["producto_duracion_dias"],
            "duracion_meses": row["producto_duracion_meses"],
            "limite_por_cuenta": row["producto_limite_por_cuenta"],
            "metadata": row["producto_metadata"],
        }

        if compra.get("paypal_order_id") and compra["paypal_order_id"] != payload.paypal_order_id:
            raise HTTPException(status_code=409, detail="La orden PayPal no coincide con la compra")

        if compra.get("estado") == "pagado" and compra.get("grant_status") == "entregado":
            return {
                "ok": True,
                "compra_id": int(compra["id"]),
                "estado": "pagado",
                "grant_status": compra.get("grant_status"),
                "grant_payload": _json_safe(compra.get("grant_payload")),
                "paypal_order_id": compra.get("paypal_order_id"),
                "paypal_capture_id": compra.get("paypal_capture_id"),
                "synced": True,
            }

        # La orden puede estar pagada previamente (retorno duplicado o webhook primero).
        # GET es la fuente final de verificación; POST capture sólo si no está completada.
        estado_previo = _obtener_orden_paypal(payload.paypal_order_id)
        ya_capturada = str(estado_previo.get("status") or "").upper() == "COMPLETED"
        if not ya_capturada and compra.get("estado") != "pagado":
            try:
                _capturar_orden_paypal(payload.paypal_order_id)
            except HTTPException:
                # El servidor pudo haber capturado la orden y perdido la respuesta.
                pass
        captured = _obtener_orden_paypal(payload.paypal_order_id)
        status_paypal = str(captured.get("status") or "").upper()
        capture_id = _verificar_captura_paypal(captured, compra)
        if compra.get("paypal_capture_id") and str(compra["paypal_capture_id"]) != capture_id:
            raise HTTPException(status_code=409, detail="La compra registra otra captura PayPal")

        compra_actualizada = _marcar_compra_pagada(
            cursor, int(compra["id"]), payload.paypal_order_id, capture_id, captured)
        if (producto["codigo"] == DAILY_PRODUCT_CODE
                and not _json_safe(compra_actualizada.get("metadata")).get("daily_shiny")):
            # La compra 22 (y otras antiguas) queda pagada sin inventar qué especie se vendió.
            entrega = {"ya_entregado": False, "grant_status": "pendiente_seleccion",
                       "grant_payload": {"message": "Pago confirmado. Falta recuperar la especie histórica de esta orden."}}
        else:
            entrega = _entregar_producto_pagado(cursor, compra_actualizada, producto)
        conn.commit()

        return {
            "ok": True,
            "compra_id": int(compra["id"]),
            "estado": "pagado",
            "paypal_order_id": payload.paypal_order_id,
            "paypal_capture_id": capture_id,
            "paypal_status": status_paypal,
            "sync_source": "order_lookup" if ya_capturada else "capture_api",
            **entrega,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /payments/paypal/order/capture:", repr(error))
        raise HTTPException(status_code=500, detail="No se pudo capturar la orden de PayPal")
    finally:
        cursor.close()
        release_connection(conn)


# JL - Inicio - Reclamo individual sin nuevo cobro. Disponible solo para el dueño.
@router_payments.post("/payments/compras/{compra_id}/reclamar")
def reclamar_compra_pagada(compra_id: int, usuario=Depends(get_current_user)):
    """Reintenta solamente la entrega de una compra capturada por PayPal.

    PostgreSQL serializa los reintentos de la misma compra mediante FOR UPDATE.
    Incluye una unica excepcion historica estricta para la compra 22.
    """
    if compra_id <= 0:
        raise HTTPException(status_code=400, detail="Identificador de compra inválido")
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""SELECT * FROM public.usuario_compras
                          WHERE id=%s AND usuario_id=%s FOR UPDATE""",
                       (compra_id, int(usuario["id"])))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Compra no encontrada en tu cuenta")
        compra = dict(row)
        if compra.get("grant_status") == "entregado" and compra.get("entregado_en") is not None:
            conn.rollback()
            return {"ok": True, "estado": "pagado", "grant_status": "entregado",
                    "ya_entregado": True, "grant_payload": _json_safe(compra.get("grant_payload"))}
        if str(compra.get("estado")) != "pagado":
            raise HTTPException(status_code=409, detail="La compra aún no figura pagada")
        if compra.get("entregado_en") is not None:
            raise HTTPException(status_code=409, detail="La compra tiene una entrega que requiere revisión")
        cursor.execute("SELECT * FROM public.monetizacion_productos WHERE id=%s",
                       (int(compra["producto_id"]),))
        item = cursor.fetchone()
        if not item:
            raise HTTPException(status_code=409, detail="El producto original no existe")
        producto = dict(item)
        paypal_order_id = str(compra.get("paypal_order_id") or "")
        paypal_capture_id = str(compra.get("paypal_capture_id") or "")
        if not paypal_order_id or not paypal_capture_id:
            raise HTTPException(status_code=409, detail="Faltan referencias del pago confirmado")
        orden = _obtener_orden_paypal(paypal_order_id)
        if str(orden.get("id") or "") != paypal_order_id:
            raise HTTPException(status_code=409, detail="La referencia PayPal no coincide")
        _verificar_captura_paypal(orden, compra, paypal_capture_id)

        # La orden histórica 22 no guardó especie. Esta excepción está ligada
        # únicamente al usuario 1 y al pago de USD 0.50 documentado.
        metadata = _json_safe(compra.get("metadata"))
        if producto.get("codigo") == DAILY_PRODUCT_CODE and not metadata.get("daily_shiny"):
            if not (int(compra_id) == 22 and int(compra["usuario_id"]) == 1
                    and str(compra.get("grant_status")) == "pendiente_seleccion"
                    and str(compra.get("moneda")) == "USD"
                    and Decimal(str(compra.get("monto"))) == Decimal("0.50")):
                raise HTTPException(status_code=409,
                                    detail="Compra antigua sin Pokémon reservado: requiere revisión de soporte")
            cursor.execute("SELECT nombre FROM public.pokemon WHERE id=245")
            especie = cursor.fetchone()
            if not especie or "suicune" not in str(especie["nombre"]).lower():
                raise HTTPException(status_code=409, detail="Suicune no coincide con el catálogo")
            reserva = {"pokemon_id": 245, "nombre": str(especie["nombre"]),
                       "fecha": "2026-10-21", "es_shiny": True,
                       "recuperacion_historica": "compra_22_checkout_suicune"}
            cursor.execute("""UPDATE public.usuario_compras SET
                  metadata=COALESCE(metadata,'{}'::jsonb) || %s::jsonb,
                  actualizado_en=NOW() WHERE id=%s RETURNING *""",
                  (json.dumps({"daily_shiny": reserva}), compra_id))
            compra = dict(cursor.fetchone())
        entrega = _entregar_producto_pagado(cursor, compra, producto)
        if entrega.get("grant_status") != "entregado":
            raise HTTPException(status_code=409, detail="No se completó la entrega")
        conn.commit()
        return {"ok": True, "compra_id": int(compra_id), "estado": "pagado",
                "grant_status": "entregado", "cobro_adicional": False,
                "grant_payload": entrega.get("grant_payload", {})}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        print("Error al reclamar compra:", type(exc).__name__, str(exc))
        raise HTTPException(status_code=500, detail="No se pudo completar la entrega. Intenta más tarde")
    finally:
        cursor.close()
        release_connection(conn)
# JL - Fin - Reclamo individual seguro.


@router_payments.post("/payments/booster/usar")
def usar_booster(payload: UseBoosterPayload, usuario=Depends(get_current_user)):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        asegurar_tablas_monetizacion(cursor)
        item_code = str(payload.item_code or "").strip()
        config = BOOSTER_ITEM_TO_BENEFIT.get(item_code)
        if not config:
            raise HTTPException(status_code=400, detail="Este item no es un booster utilizable")

        consumo = consumir_item_usuario_por_codigo(cursor, int(usuario["id"]), item_code, 1)
        if not consumo:
            raise HTTPException(status_code=409, detail="No tienes unidades disponibles de ese booster")

        beneficio = activar_o_extender_beneficio(
            cursor,
            int(usuario["id"]),
            config["beneficio_codigo"],
            origen_tipo="item",
            duracion_horas=int(config["duracion_horas"]),
            metadata={"item_code": item_code, "source": "inventory_booster"},
        )
        conn.commit()

        return {
            "ok": True,
            "consumo": consumo,
            "beneficio": {
                "beneficio_codigo": beneficio["beneficio_codigo"],
                "inicia_en": serializar_datetime_api(beneficio.get("inicia_en")),
                "expira_en": serializar_datetime_api(beneficio.get("expira_en")),
            }
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        print("Error en /payments/booster/usar:", repr(error))
        raise HTTPException(status_code=500, detail="No se pudo activar el booster")
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.post("/payments/paypal/webhook")
async def paypal_webhook(request: Request):
    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes.decode("utf-8") or "{}")
    except Exception:
        payload = {}

    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if not existe_tabla_local(cursor, "payment_webhook_eventos"):
            return {"ok": True, "warning": "Tabla payment_webhook_eventos no existe todavía"}

        verificado = _verificar_webhook_paypal({k.lower(): v for k, v in request.headers.items()}, payload)
        event_id = str(payload.get("id") or "")
        event_type = str(payload.get("event_type") or "unknown")

        cursor.execute(
            """
            INSERT INTO payment_webhook_eventos
                (proveedor, event_id, event_type, verificado, payload)
            VALUES
                ('paypal', %s, %s, %s, %s::jsonb)
            ON CONFLICT (proveedor, event_id)
            DO NOTHING
            """,
            (event_id or str(uuid.uuid4()), event_type, verificado, json.dumps(payload or {}))
        )

        if verificado is not True:
            conn.commit()
            return {
                "ok": False,
                "event_type": event_type,
                "verified": verificado,
                "ignored": True,
            }

        order_id = None
        capture_id = None
        resource = payload.get("resource") or {}
        if event_type == "PAYMENT.CAPTURE.COMPLETED":
            capture_id = resource.get("id")
            related_ids = ((resource.get("supplementary_data") or {}).get("related_ids") or {})
            order_id = related_ids.get("order_id")
        elif event_type == "CHECKOUT.ORDER.APPROVED":
            order_id = resource.get("id")

        if order_id:
            cursor.execute(
                """
                SELECT
                    uc.id AS compra_id,
                    uc.usuario_id AS compra_usuario_id,
                    uc.producto_id AS compra_producto_id,
                    uc.estado AS compra_estado,
                    uc.moneda AS compra_moneda,
                    uc.monto AS compra_monto,
                    uc.proveedor_pago AS compra_proveedor_pago,
                    uc.referencia_externa AS compra_referencia_externa,
                    uc.paypal_order_id AS compra_paypal_order_id,
                    uc.paypal_capture_id AS compra_paypal_capture_id,
                    uc.grant_status AS compra_grant_status,
                    uc.grant_payload AS compra_grant_payload,
                    uc.metadata AS compra_metadata,
                    uc.pagado_en AS compra_pagado_en,
                    uc.entregado_en AS compra_entregado_en,

                    mp.id AS producto_id,
                    mp.codigo AS producto_codigo,
                    mp.nombre AS producto_nombre,
                    mp.tipo AS producto_tipo,
                    mp.precio_usd AS producto_precio_usd,
                    mp.beneficio_codigo AS producto_beneficio_codigo,
                    mp.duracion_horas AS producto_duracion_horas,
                    mp.duracion_dias AS producto_duracion_dias,
                    mp.duracion_meses AS producto_duracion_meses,
                    mp.limite_por_cuenta AS producto_limite_por_cuenta,
                    mp.metadata AS producto_metadata
                FROM usuario_compras uc
                JOIN monetizacion_productos mp ON mp.id = uc.producto_id
                WHERE uc.paypal_order_id = %s
                   OR uc.referencia_externa = %s
                ORDER BY uc.id DESC
                LIMIT 1
                FOR UPDATE
                """,
                (order_id, order_id)
            )

            row = cursor.fetchone()

            if row and event_type == "PAYMENT.CAPTURE.COMPLETED":
                row = dict(row)

                compra = {
                    "id": row["compra_id"],
                    "usuario_id": row["compra_usuario_id"],
                    "producto_id": row["compra_producto_id"],
                    "estado": row["compra_estado"],
                    "moneda": row["compra_moneda"],
                    "monto": row["compra_monto"],
                    "proveedor_pago": row["compra_proveedor_pago"],
                    "referencia_externa": row["compra_referencia_externa"],
                    "paypal_order_id": row["compra_paypal_order_id"],
                    "paypal_capture_id": row["compra_paypal_capture_id"],
                    "grant_status": row["compra_grant_status"],
                    "grant_payload": row["compra_grant_payload"],
                    "metadata": row["compra_metadata"],
                    "pagado_en": row["compra_pagado_en"],
                    "entregado_en": row["compra_entregado_en"],
                }

                producto = {
                    "id": row["producto_id"],
                    "codigo": row["producto_codigo"],
                    "nombre": row["producto_nombre"],
                    "tipo": row["producto_tipo"],
                    "precio_usd": row["producto_precio_usd"],
                    "beneficio_codigo": row["producto_beneficio_codigo"],
                    "duracion_horas": row["producto_duracion_horas"],
                    "duracion_dias": row["producto_duracion_dias"],
                    "duracion_meses": row["producto_duracion_meses"],
                    "limite_por_cuenta": row["producto_limite_por_cuenta"],
                    "metadata": row["producto_metadata"],
                }

                orden_validada = _obtener_orden_paypal(order_id)
                valid_capture_id = _verificar_captura_paypal(orden_validada, compra, capture_id)
                if compra.get("paypal_capture_id") and str(compra["paypal_capture_id"]) != valid_capture_id:
                    raise HTTPException(status_code=409, detail="Webhook con captura diferente")
                if compra.get("estado") != "pagado":
                    compra = _marcar_compra_pagada(
                        cursor, int(compra["id"]), order_id, valid_capture_id, orden_validada)
                if not (producto["codigo"] == DAILY_PRODUCT_CODE
                        and not _json_safe(compra.get("metadata")).get("daily_shiny")):
                    _entregar_producto_pagado(cursor, compra, producto)

        conn.commit()
        return {"ok": True, "event_type": event_type, "verified": verificado}
    except Exception as error:
        conn.rollback()
        print("Error en /payments/paypal/webhook:", repr(error))
        raise HTTPException(status_code=500, detail="No se pudo procesar el webhook PayPal")
    finally:
        cursor.close()
        release_connection(conn)


# JL - Inicio - Recovery of one verified legacy purchase; no public item-grant endpoint.
def _admin_developer(cursor, identity):
    """Always read the live DB role, never trust a role supplied by the browser/JWT."""
    uid = int(identity["id"])
    cursor.execute("SELECT id, rol FROM public.usuarios WHERE id = %s", (uid,))
    row = cursor.fetchone()
    if not row or str(row.get("rol") or "") != "desarrollador":
        raise HTTPException(status_code=403, detail="Acceso exclusivo para desarrollador")
    return uid


def _legacy_purchase_locked(cursor):
    cursor.execute("SELECT * FROM public.usuario_compras WHERE id=%s FOR UPDATE", (LEGACY_RECOVERY_PURCHASE_ID,))
    purchase = cursor.fetchone()
    if not purchase:
        raise HTTPException(status_code=404, detail="Compra histórica no encontrada")
    purchase = dict(purchase)
    if int(purchase["usuario_id"]) != LEGACY_RECOVERY_USER_ID:
        raise HTTPException(status_code=409, detail="La compra pertenece a otro usuario")
    cursor.execute("SELECT * FROM public.monetizacion_productos WHERE id=%s", (int(purchase["producto_id"]),))
    product = cursor.fetchone()
    if not product or str(product["codigo"]) != DAILY_PRODUCT_CODE:
        raise HTTPException(status_code=409, detail="El producto no coincide con el exclusivo original")
    return purchase, dict(product)


def _validate_legacy_payment(cursor, purchase):
    if str(purchase["estado"]) != "pagado":
        raise HTTPException(status_code=409, detail="La compra no consta pagada")
    if str(purchase["moneda"]) != "USD" or Decimal(str(purchase["monto"])) != LEGACY_RECOVERY_AMOUNT:
        raise HTTPException(status_code=409, detail="Importe histórico incorrecto")
    if purchase.get("grant_status") == "entregado" or purchase.get("entregado_en") is not None:
        raise HTTPException(status_code=409, detail="Esta compra ya registra una entrega")
    if str(purchase.get("grant_status") or "") != "pendiente_seleccion":
        raise HTTPException(status_code=409, detail="La compra no está pendiente de selección")
    metadata = _json_safe(purchase.get("metadata"))
    if metadata.get("daily_shiny"):
        raise HTTPException(status_code=409, detail="Ya existe un Pokémon reservado: requiere revisión")
    paypal_id = str(purchase.get("paypal_order_id") or "")
    capture_id = str(purchase.get("paypal_capture_id") or "")
    if not paypal_id or not capture_id:
        raise HTTPException(status_code=409, detail="Faltan referencias del pago original")
    order = _obtener_orden_paypal(paypal_id)
    if str(order.get("id") or "") != paypal_id:
        raise HTTPException(status_code=409, detail="PayPal devolvió otra orden")
    verified_id = _verificar_captura_paypal(order, purchase, capture_id)
    cursor.execute("SELECT id,nombre FROM public.pokemon WHERE id=%s", (LEGACY_RECOVERY_POKEMON_ID,))
    pokemon = cursor.fetchone()
    if not pokemon or "suicune" not in str(pokemon["nombre"]).lower():
        raise HTTPException(status_code=409, detail="Suicune no está disponible en el catálogo")
    return dict(pokemon), verified_id


@router_payments.get("/payments/admin/recuperacion/22")
def admin_preview_compra22(usuario=Depends(get_current_user)):
    """Read-only verification, no delivery. Restricted to current developer role."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        _admin_developer(cursor, usuario)
        purchase, _ = _legacy_purchase_locked(cursor)
        if purchase.get("grant_status") == "entregado" and purchase.get("entregado_en"):
            result = {"ok": True, "compra_id": 22, "estado": "entregado", "grant_payload": _json_safe(purchase.get("grant_payload"))}
        else:
            pokemon, _ = _validate_legacy_payment(cursor, purchase)
            result = {"ok": True, "compra_id": 22, "estado": "pendiente_seleccion",
                      "monto": "0.50", "moneda": "USD", "pokemon_id": LEGACY_RECOVERY_POKEMON_ID,
                      "pokemon_nombre": str(pokemon["nombre"]), "es_shiny": True,
                      "pago_verificado": True,
                      "advertencia": "Asociación histórica a Suicune basada en la captura del checkout; la orden antigua no guardó la especie."}
        conn.rollback()  # GET never changes DB state
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)


@router_payments.post("/payments/admin/recuperacion/22")
def admin_deliver_compra22(payload: AdminRecoveryConfirmation, usuario=Depends(get_current_user)):
    """Exactly-once grant under PostgreSQL row lock, including audit record."""
    if payload.confirmacion != LEGACY_RECOVERY_CONFIRMATION:
        raise HTTPException(status_code=400, detail="Frase de confirmación incorrecta")
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        actor_id = _admin_developer(cursor, usuario)
        purchase, product = _legacy_purchase_locked(cursor)
        pokemon, capture_id = _validate_legacy_payment(cursor, purchase)
        cursor.execute("""SELECT id FROM public.payment_admin_recoveries
                          WHERE compra_id=%s FOR UPDATE""", (LEGACY_RECOVERY_PURCHASE_ID,))
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Compra ya registrada en la auditoría")
        reservation = {"pokemon_id": LEGACY_RECOVERY_POKEMON_ID,
                       "nombre": str(pokemon["nombre"]), "fecha": "2026-10-21", "es_shiny": True,
                       "recuperacion_admin": True, "fuente": "checkout_historico_compra_22"}
        cursor.execute("""UPDATE public.usuario_compras
             SET metadata=COALESCE(metadata,'{}'::jsonb) || %s::jsonb,
                 actualizado_en=NOW() WHERE id=%s RETURNING *""",
             (json.dumps({"daily_shiny": reservation}), LEGACY_RECOVERY_PURCHASE_ID))
        updated = cursor.fetchone()
        result = _entregar_producto_pagado(cursor, dict(updated), product)
        grant = result.get("grant_payload") or {}
        if result.get("grant_status") != "entregado" or not grant.get("usuario_pokemon_id"):
            raise HTTPException(status_code=500, detail="La entrega no confirmó el Pokémon")
        cursor.execute("""INSERT INTO public.payment_admin_recoveries
            (compra_id, administrador_id, usuario_id, pokemon_id, usuario_pokemon_id,
             paypal_order_id, paypal_capture_id, monto, moneda, motivo)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (LEGACY_RECOVERY_PURCHASE_ID, actor_id, LEGACY_RECOVERY_USER_ID,
             LEGACY_RECOVERY_POKEMON_ID, int(grant["usuario_pokemon_id"]),
             str(purchase["paypal_order_id"]), capture_id,
             LEGACY_RECOVERY_AMOUNT, "USD", "Recuperación manual verificada - checkout Suicune Shiny 2026-10-21"))
        conn.commit()
        return {"ok": True, "compra_id": 22, "estado": "entregado", "grant_status": "entregado",
                "pokemon_id": LEGACY_RECOVERY_POKEMON_ID,
                "usuario_pokemon_id": int(grant["usuario_pokemon_id"]), "cobro_adicional": False}
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        # Do not expose database or payment details to the client.
        raise HTTPException(status_code=500, detail="No se pudo finalizar la recuperación. Revisa los logs.")
    finally:
        cursor.close()
        release_connection(conn)
# JL - Fin - Recover purchase 22.

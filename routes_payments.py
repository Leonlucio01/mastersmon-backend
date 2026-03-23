import json
import os
import uuid
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from auth import get_current_user
from database import get_connection, get_cursor, release_connection
from monetization_utils import (
    activar_o_extender_beneficio,
    agregar_item_usuario,
    asegurar_tablas_monetizacion,
    consumir_item_usuario_por_codigo,
    existe_tabla_local,
    obtener_beneficios_activos,
    resolver_item_por_codigo,
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


class CreatePayPalOrderPayload(BaseModel):
    product_code: str
    quantity: int = 1
    confirmacion_aceptada: bool = False
    version_terminos: str = "v1"


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
        raise HTTPException(status_code=502, detail=f"No se pudo autenticar con PayPal: {response.text[:300]}")
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
    monto = f"{float(producto['precio_usd']):.2f}"
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
        raise HTTPException(status_code=502, detail=f"No se pudo crear la orden PayPal: {response.text[:400]}")
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
        raise HTTPException(status_code=502, detail=f"No se pudo capturar la orden PayPal: {response.text[:400]}")
    return response.json()

def _obtener_orden_paypal(order_id: str):
    token = _paypal_access_token()
    response = requests.get(
        f"{PAYPAL_BASE_URL}/v2/checkout/orders/{order_id}",
        headers=_paypal_headers(token),
        timeout=30,
    )
    if response.status_code >= 300:
        raise HTTPException(status_code=502, detail=f"No se pudo consultar la orden PayPal: {response.text[:400]}")
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
        grant_payload["expira_en"] = beneficio["expira_en"].isoformat() if beneficio.get("expira_en") else None

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

    elif producto["tipo"] == "pokemon_exclusivo":
        grant_status = "pendiente_seleccion"
        grant_payload["requires_selection"] = True
        grant_payload["message"] = "La compra quedó pagada. Falta seleccionar el Pokémon exclusivo para entregarlo."

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
                "pagado_en": row["pagado_en"].isoformat() if row.get("pagado_en") else None,
                "entregado_en": row["entregado_en"].isoformat() if row.get("entregado_en") else None,
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
            SELECT uc.*, mp.*
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

        compra = dict(row)
        producto = dict(row)

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

        captured = None
        capture_error_detail = None

        try:
            captured = _capturar_orden_paypal(payload.paypal_order_id)
        except HTTPException as exc:
            detail_text = str(exc.detail or "")
            capture_error_detail = detail_text

            if "ORDER_ALREADY_CAPTURED" not in detail_text:
                raise

            # Ya fue capturada en PayPal; reconciliar consultando la orden
            captured = _obtener_orden_paypal(payload.paypal_order_id)

        status_paypal = str(captured.get("status") or "").upper()
        if status_paypal not in ("COMPLETED", "APPROVED"):
            raise HTTPException(
                status_code=409,
                detail=f"PayPal devolvió un estado no finalizable: {status_paypal}"
            )

        capture_id = _extraer_capture_id_paypal(captured)

        compra_actualizada = _marcar_compra_pagada(
            cursor,
            int(compra["id"]),
            payload.paypal_order_id,
            capture_id,
            captured,
        )

        entrega = _entregar_producto_pagado(cursor, compra_actualizada, producto)
        conn.commit()

        return {
            "ok": True,
            "compra_id": int(compra["id"]),
            "estado": "pagado",
            "paypal_order_id": payload.paypal_order_id,
            "paypal_capture_id": capture_id,
            "paypal_status": status_paypal,
            "sync_source": "order_lookup_after_already_captured" if capture_error_detail else "capture_api",
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
                "inicia_en": beneficio["inicia_en"].isoformat() if beneficio.get("inicia_en") else None,
                "expira_en": beneficio["expira_en"].isoformat() if beneficio.get("expira_en") else None,
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
                SELECT uc.*, mp.*
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
            if row and row.get("estado") != "pagado" and event_type == "PAYMENT.CAPTURE.COMPLETED":
                compra = _marcar_compra_pagada(cursor, int(row["id"]), order_id, capture_id, payload)
                _entregar_producto_pagado(cursor, compra, dict(row))

        conn.commit()
        return {"ok": True, "event_type": event_type, "verified": verificado}
    except Exception as error:
        conn.rollback()
        print("Error en /payments/paypal/webhook:", repr(error))
        raise HTTPException(status_code=500, detail="No se pudo procesar el webhook PayPal")
    finally:
        cursor.close()
        release_connection(conn)

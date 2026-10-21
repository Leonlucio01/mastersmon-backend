#!/usr/bin/env python3
"""Apply V2.14.2 to the EXISTING backend/routes_payments.py, preserving other routes.
Run: python patch_routes_payments.py /path/to/mastersmon-backend/routes_payments.py
Creates .v2142.bak. Stops if anchors do not match: never partially overwrites.
"""
from pathlib import Path
import sys
import ast

file = Path(sys.argv[1] if len(sys.argv) > 1 else 'routes_payments.py')
if not file.is_file():
    sys.exit(f'Not found: {file}')
s=file.read_text(encoding='utf-8')
if '# V2142 VERIFIED DAILY SHINY' in s:
    sys.exit('V2.14.2 already applied. No changes.')

def replace_once(old, new):
    global s
    n=s.count(old)
    if n != 1:
        raise RuntimeError(f'Expected one anchor, got {n}: {old[:105]!r}')
    s=s.replace(old,new,1)

try:
    replace_once('import uuid\n', 'import uuid\nfrom decimal import Decimal, InvalidOperation\n')
    replace_once('from auth import get_current_user\n',
'''from auth import get_current_user
from routes_daily_shiny import PRODUCT_CODE as DAILY_PRODUCT_CODE, configured_ids, stable_choice
from server_time import server_now
from routes_pokemon import calcular_stats, calcular_experiencia_total_base, sincronizar_movimientos_usuario_pokemon
''')
    replace_once('"description": producto["nombre"],',
                 '"description": (_json_safe(compra.get("metadata")).get("daily_shiny") or {}).get("nombre", producto["nombre"]),')
    replace_once('    version_terminos: str = "v1"\n',
'''    version_terminos: str = "v1"
    daily_pokemon_id: int | None = None
    daily_offer_date: str | None = None
''')
    replace_once('def _entregar_producto_pagado(cursor, compra: dict, producto: dict):',
'''# V2142 VERIFIED DAILY SHINY. Check PayPal captures before fulfillment.
def _daily_shiny_at_order(cursor):
    today = server_now().date()
    allowed = configured_ids()
    cursor.execute("SELECT id, nombre FROM pokemon WHERE id = ANY(%s)", (list(allowed),))
    found = {int(row["id"]): row for row in (cursor.fetchall() or [])}
    available = tuple(pid for pid in allowed if pid in found)
    if not available:
        raise HTTPException(status_code=503, detail="Sin legendarios diarios disponibles")
    pokemon_id = stable_choice(available, today)
    return {"pokemon_id": pokemon_id, "nombre": str(found[pokemon_id]["nombre"]), "fecha": today.isoformat(), "es_shiny": True}


def _require_completed_capture(paypal_order: dict, compra: dict, expected_capture_id=None):
    if str(paypal_order.get("status") or "").upper() != "COMPLETED":
        raise HTTPException(status_code=409, detail="La orden PayPal no está completada")
    units = paypal_order.get("purchase_units") or []
    if len(units) != 1 or str(units[0].get("custom_id") or "") != str(compra["id"]):
        raise HTTPException(status_code=409, detail="La orden PayPal no corresponde a esta compra")
    unit = units[0]
    if str(unit.get("invoice_id") or "") != f"MSM-{compra['id']}":
        raise HTTPException(status_code=409, detail="La factura PayPal no coincide")
    captures = (unit.get("payments") or {}).get("captures") or []
    captures = [c for c in captures if str(c.get("status") or "").upper() == "COMPLETED"]
    if len(captures) != 1:
        raise HTTPException(status_code=409, detail="No existe una captura PayPal completada única")
    capture = captures[0]
    amount = capture.get("amount") or {}
    if str(amount.get("currency_code") or "") != str(compra["moneda"]):
        raise HTTPException(status_code=409, detail="La moneda capturada no coincide")
    try:
        paid = Decimal(str(amount["value"]))
        expected = Decimal(str(compra["monto"]))
    except (InvalidOperation, KeyError, TypeError):
        raise HTTPException(status_code=409, detail="Monto de PayPal inválido")
    if paid != expected:
        raise HTTPException(status_code=409, detail="El importe capturado no coincide")
    capture_id = str(capture.get("id") or "")
    if not capture_id or (expected_capture_id and capture_id != str(expected_capture_id)):
        raise HTTPException(status_code=409, detail="Identificador de captura incorrecto")
    return capture_id


def _entregar_producto_pagado(cursor, compra: dict, producto: dict):''')
    replace_once('''    elif producto["tipo"] == "pokemon_exclusivo":
        grant_status = "pendiente_seleccion"
        grant_payload["requires_selection"] = True
        grant_payload["message"] = "La compra quedó pagada. Falta seleccionar el Pokémon exclusivo para entregarlo."
''','''    elif producto["tipo"] == "pokemon_exclusivo" and producto["codigo"] == DAILY_PRODUCT_CODE:
        selection = _json_safe(compra.get("metadata")).get("daily_shiny")
        if not isinstance(selection, dict) or not selection.get("pokemon_id"):
            raise HTTPException(status_code=409, detail="Compra sin legendario reservado: requiere revisión")
        pokemon_id = int(selection["pokemon_id"])
        if pokemon_id not in configured_ids() or selection.get("es_shiny") is not True:
            raise HTTPException(status_code=409, detail="Legendario reservado no permitido")
        cursor.execute("SELECT id, nombre, hp, ataque, defensa, velocidad, ataque_especial, defensa_especial FROM pokemon WHERE id=%s", (pokemon_id,))
        p = cursor.fetchone()
        if not p:
            raise HTTPException(status_code=409, detail="Legendario reservado no existe")
        level = 5
        stats = calcular_stats(int(p["hp"]), int(p["ataque"]), int(p["defensa"]), int(p["velocidad"]), level,
            base_ataque_especial=int(p["ataque_especial"]), base_defensa_especial=int(p["defensa_especial"]))
        cursor.execute(''' + "'''" + '''INSERT INTO usuario_pokemon
            (usuario_id,pokemon_id,nivel,experiencia,experiencia_total,es_shiny,
             hp_actual,hp_max,ataque,defensa,velocidad,ataque_especial,defensa_especial,
             bonus_hp_renacer,bonus_ataque_renacer,bonus_defensa_renacer,bonus_velocidad_renacer,victorias_total)
            VALUES (%s,%s,%s,0,%s,TRUE,%s,%s,%s,%s,%s,%s,%s,0,0,0,0,0) RETURNING id''' + "'''" + ''',
            (int(compra["usuario_id"]), pokemon_id, level, calcular_experiencia_total_base(level, 0),
             stats["hp_max"], stats["hp_max"], stats["ataque"], stats["defensa"], stats["velocidad"],
             stats["ataque_especial"], stats["defensa_especial"]))
        owned_id = int(cursor.fetchone()["id"])
        sincronizar_movimientos_usuario_pokemon(cursor, owned_id, pokemon_id, level, origen="compra", auto_equipar=True)
        grant_payload.update({"usuario_pokemon_id": owned_id, "pokemon_id": pokemon_id,
                              "nombre": p["nombre"], "es_shiny": True, "fecha_oferta": selection["fecha"]})
    elif producto["tipo"] == "pokemon_exclusivo":
        raise HTTPException(status_code=409, detail="Producto exclusivo sin entrega configurada")
''')
    replace_once('''        compra = _crear_compra_pendiente(cursor, usuario, producto, payload, request)
        orden = _crear_orden_paypal(compra, producto)
''','''        compra = _crear_compra_pendiente(cursor, usuario, producto, payload, request)
        if producto["codigo"] == DAILY_PRODUCT_CODE:
            selected = _daily_shiny_at_order(cursor)
            if (payload.daily_pokemon_id != selected["pokemon_id"]
                    or payload.daily_offer_date != selected["fecha"]):
                raise HTTPException(status_code=409, detail="La oferta Shiny ha cambiado; actualiza el catálogo")
            cursor.execute("UPDATE usuario_compras SET metadata=COALESCE(metadata, '{}'::jsonb) || %s::jsonb WHERE id=%s RETURNING *",
                           (json.dumps({"daily_shiny": selected}), int(compra["id"])))
            compra = cursor.fetchone()
        orden = _crear_orden_paypal(compra, producto)
''')
    start=s.index('        try:\n            captured = _capturar_orden_paypal(payload.paypal_order_id)',s.index('def capturar_orden_paypal'))
    end=s.index('        compra_actualizada = _marcar_compra_pagada(',start)
    block='''        try:
            captured = _capturar_orden_paypal(payload.paypal_order_id)
        except HTTPException:
            # If PayPal captured already, reconcile via the verified order lookup.
            captured = _obtener_orden_paypal(payload.paypal_order_id)
            capture_error_detail = "reconciled"
        status_paypal = str(captured.get("status") or "").upper()
        capture_id = _require_completed_capture(captured, compra)

'''
    s=s[:start]+block+s[end:]
    replace_once('''                if compra.get("estado") != "pagado":
                    compra = _marcar_compra_pagada(''','''                verified_order = _obtener_orden_paypal(order_id)
                verified_capture_id = _require_completed_capture(verified_order, compra, capture_id)
                if compra.get("estado") != "pagado":
                    compra = _marcar_compra_pagada(''')
    replace_once('''                        capture_id,
                        payload
                    )

                _entregar_producto_pagado(cursor, compra, producto)''','''                        verified_capture_id,
                        payload
                    )

                _entregar_producto_pagado(cursor, compra, producto)''')
    ast.parse(s)
except Exception as err:
    sys.exit(f'No files changed: {err}')
backup=file.with_suffix(file.suffix+'.v2142.bak')
if backup.exists():
    sys.exit(f'Refusing to replace existing backup {backup}')
backup.write_text(file.read_text(encoding='utf-8'),encoding='utf-8')
file.write_text(s,encoding='utf-8')
print(f'V2.14.2 applied to {file}; backup saved to {backup}')

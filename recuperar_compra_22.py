"""Mastersmon - recovery of verified, already-paid legacy purchase 22.
CLI only, NOT a FastAPI route. Dry-run default. Run inside the Render backend
environment with the existing PAYPAL_* and DATABASE_* configuration.
"""
import argparse
import json
import sys

from database import get_connection, get_cursor, release_connection
from routes_payments import (
    DAILY_PRODUCT_CODE, _entregar_producto_pagado, _json_safe,
    _obtener_orden_paypal, _verificar_captura_paypal,
)

PURCHASE_ID = 22
EXPECTED_USER_ID = 1
EXPECTED_POKEMON_ID = 245  # Suicune (buyer screenshot of checkout)
EXPECTED_AMOUNT = '0.50'
EXPECTED_CURRENCY = 'USD'
OFFER_DAY = '2026-10-21'


def recover(apply=False):
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        # Lock original paid purchase. Repeated invocations cannot grant twice.
        cursor.execute('SELECT * FROM usuario_compras WHERE id=%s FOR UPDATE', (PURCHASE_ID,))
        compra = cursor.fetchone()
        if not compra:
            raise RuntimeError('Compra 22 no encontrada')
        compra = dict(compra)
        if int(compra['usuario_id']) != EXPECTED_USER_ID:
            raise RuntimeError('Usuario distinto al de la compra confirmada')
        if str(compra['estado']) != 'pagado':
            raise RuntimeError('La compra no figura como pagada')
        if str(compra['moneda']) != EXPECTED_CURRENCY or float(compra['monto']) != float(EXPECTED_AMOUNT):
            raise RuntimeError('Importe o moneda original diferentes')
        if compra.get('grant_status') == 'entregado' or compra.get('entregado_en') is not None:
            print('Compra 22 ya entregada. No se agrego ningun Pokemon.')
            conn.rollback()
            return
        if compra.get('grant_status') != 'pendiente_seleccion':
            raise RuntimeError('Estado de entrega no esperado; detener para revision')
        cursor.execute('SELECT * FROM monetizacion_productos WHERE id=%s', (int(compra['producto_id']),))
        producto = cursor.fetchone()
        if not producto or producto['codigo'] != DAILY_PRODUCT_CODE:
            raise RuntimeError('Compra 22 no corresponde al producto esperado')
        producto = dict(producto)
        order_id = str(compra.get('paypal_order_id') or '').strip()
        original_capture = str(compra.get('paypal_capture_id') or '').strip()
        if not order_id or not original_capture:
            raise RuntimeError('Falta referencia PayPal original')
        # Independently verify with PayPal, not merely a database flag.
        paypal_order = _obtener_orden_paypal(order_id)
        if str(paypal_order.get('id') or '') != order_id:
            raise RuntimeError('PayPal devolvio una orden diferente')
        capture_id = _verificar_captura_paypal(paypal_order, compra, original_capture)
        cursor.execute('SELECT id, nombre FROM pokemon WHERE id=%s', (EXPECTED_POKEMON_ID,))
        pokemon = cursor.fetchone()
        if not pokemon or 'suicune' not in str(pokemon['nombre']).lower():
            raise RuntimeError('Pokemon 245 no corresponde a Suicune')
        metadata = _json_safe(compra.get('metadata'))
        if metadata.get('daily_shiny'):
            raise RuntimeError('Existe reserva previa en compra 22; requiere revision')
        print('Pago verificado: compra 22, USD 0.50, captura coincidente, Suicune Shiny.')
        print('La asociacion a Suicune se basa en la evidencia del checkout historico.')
        if not apply:
            print('SIMULACION: ningun registro modificado. Para ejecutar usa --entregar-compra-22-suicune.')
            conn.rollback()
            return
        offer = {
            'pokemon_id': EXPECTED_POKEMON_ID,
            'nombre': str(pokemon['nombre']),
            'fecha': OFFER_DAY,
            'es_shiny': True,
            'recuperacion_admin': True,
            'fuente': 'checkout_historico_compra_22',
        }
        cursor.execute("""UPDATE usuario_compras
            SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                actualizado_en = NOW()
            WHERE id = %s AND estado = 'pagado'
            RETURNING *""", (json.dumps({'daily_shiny': offer}), PURCHASE_ID))
        updated = cursor.fetchone()
        if not updated:
            raise RuntimeError('No se pudo reservar Suicune en la compra pagada')
        result = _entregar_producto_pagado(cursor, dict(updated), producto)
        if result.get('grant_status') != 'entregado' or not result.get('grant_payload', {}).get('usuario_pokemon_id'):
            raise RuntimeError('La entrega no retorno un Pokemon entregado')
        conn.commit()
        print('ENTREGADO: compra 22, usuario_pokemon_id =', result['grant_payload']['usuario_pokemon_id'])
        print('No se genero orden PayPal nueva ni se realizo cobro adicional.')
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Recuperar compra historica 22 con captura PayPal verificada')
    parser.add_argument('--entregar-compra-22-suicune', action='store_true', help='Confirmar y entregar (por defecto solo verifica)')
    args = parser.parse_args()
    try:
        recover(apply=args.entregar_compra_22_suicune)
    except Exception as exc:
        print('RECUPERACION NO EJECUTADA:', type(exc).__name__, str(exc), file=sys.stderr)
        sys.exit(1)

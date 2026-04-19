"""Compatibility facade for payment and monetization helpers."""
from monetization_utils import (
    asegurar_tablas_monetizacion,
    obtener_beneficios_activos,
    activar_o_extender_beneficio,
    agregar_item_usuario,
    consumir_item_usuario_por_codigo,
)
from routes_payments import router_payments

__all__ = [
    "asegurar_tablas_monetizacion",
    "obtener_beneficios_activos",
    "activar_o_extender_beneficio",
    "agregar_item_usuario",
    "consumir_item_usuario_por_codigo",
    "router_payments",
]

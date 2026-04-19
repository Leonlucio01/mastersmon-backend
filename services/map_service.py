"""Compatibility facade for map, presence, and encounter helpers."""
from routes_pokemon import (
    limpiar_presencia_expirada,
    registrar_presencia_usuario,
    eliminar_presencia_usuario,
    obtener_presencia_usuario_actual,
    validar_zona_existente,
    obtener_jugadores_presencia_zona_data,
    generar_encuentro,
)

__all__ = [
    "limpiar_presencia_expirada",
    "registrar_presencia_usuario",
    "eliminar_presencia_usuario",
    "obtener_presencia_usuario_actual",
    "validar_zona_existente",
    "obtener_jugadores_presencia_zona_data",
    "generar_encuentro",
]

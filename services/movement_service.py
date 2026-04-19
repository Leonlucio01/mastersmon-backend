"""Compatibility facade for move-system helpers."""
from routes_pokemon import (
    sistema_movimientos_habilitado,
    sincronizar_movimientos_usuario_pokemon,
    obtener_movimientos_usuario_pokemon,
    equipar_movimiento_usuario_pokemon,
)

__all__ = [
    "sistema_movimientos_habilitado",
    "sincronizar_movimientos_usuario_pokemon",
    "obtener_movimientos_usuario_pokemon",
    "equipar_movimiento_usuario_pokemon",
]

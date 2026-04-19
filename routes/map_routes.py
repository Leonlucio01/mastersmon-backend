"""Future home for map and encounter endpoints."""
from routes_pokemon import (
    obtener_zonas,
    obtener_presencia_maps,
    actualizar_presencia_maps,
    eliminar_presencia_maps,
    generar_encuentro,
)

__all__ = [
    "obtener_zonas",
    "obtener_presencia_maps",
    "actualizar_presencia_maps",
    "eliminar_presencia_maps",
    "generar_encuentro",
]

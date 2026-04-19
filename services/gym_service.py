"""Compatibility facade for gym domain logic."""
from routes_gyms import (
    obtener_catalogo_gyms,
    obtener_progreso_gyms,
    iniciar_gym,
    reclamar_recompensa_gym,
    cancelar_sesion_gym,
)

__all__ = [
    "obtener_catalogo_gyms",
    "obtener_progreso_gyms",
    "iniciar_gym",
    "reclamar_recompensa_gym",
    "cancelar_sesion_gym",
]

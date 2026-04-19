"""Compatibility facade for boss and idle domain logic."""
from routes_boss_idle import (
    obtener_estado_boss,
    iniciar_boss_diario,
    reclamar_recompensa_boss,
    obtener_ranking_boss,
    obtener_estado_idle,
    iniciar_idle,
    reclamar_idle,
    cancelar_idle,
)

__all__ = [
    "obtener_estado_boss",
    "iniciar_boss_diario",
    "reclamar_recompensa_boss",
    "obtener_ranking_boss",
    "obtener_estado_idle",
    "iniciar_idle",
    "reclamar_idle",
    "cancelar_idle",
]

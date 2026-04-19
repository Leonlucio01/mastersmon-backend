"""Compatibility facade for progression and rewards.

This centralizes the reward helpers that are currently spread across multiple routes.
"""
from monetization_utils import obtener_multiplicadores_recompensa_batalla, aplicar_multiplicadores_recompensa_batalla
from routes_boss_idle import aplicar_exp_a_equipo, sumar_pokedolares_usuario
from routes_gyms import aplicar_recompensas_gym

__all__ = [
    "obtener_multiplicadores_recompensa_batalla",
    "aplicar_multiplicadores_recompensa_batalla",
    "aplicar_exp_a_equipo",
    "sumar_pokedolares_usuario",
    "aplicar_recompensas_gym",
]

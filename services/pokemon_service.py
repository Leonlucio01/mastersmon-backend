"""Non-breaking service facade for pokemon-related legacy helpers."""
from routes_pokemon import (
    obtener_pokemon_usuario_data,
    obtener_items_usuario_data,
    obtener_pokemon_me,
    obtener_pokemon_usuario,
    capturar_pokemon,
    soltar_pokemon,
    ganar_experiencia,
)

__all__ = [
    "obtener_pokemon_usuario_data",
    "obtener_items_usuario_data",
    "obtener_pokemon_me",
    "obtener_pokemon_usuario",
    "capturar_pokemon",
    "soltar_pokemon",
    "ganar_experiencia",
]

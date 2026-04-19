"""Future home for player profile endpoints.

This module intentionally re-exports current handlers only.
"""
from routes_pokemon import obtener_usuario_me, actualizar_avatar_me, obtener_trainer_setup_me, actualizar_trainer_setup_me

__all__ = [
    "obtener_usuario_me",
    "actualizar_avatar_me",
    "obtener_trainer_setup_me",
    "actualizar_trainer_setup_me",
]

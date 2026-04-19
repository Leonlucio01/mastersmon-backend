"""Compatibility facade for battle session helpers."""
from routes_pokemon import (
    invalidar_sesiones_battle_activas,
    obtener_sesion_battle_por_token,
    validar_cooldown_recompensa_batalla,
    registrar_cooldown_recompensa_batalla,
    obtener_recompensa_batalla_permitida,
    obtener_recompensa_batalla_por_codigo,
    battle_booster_snapshot_habilitado,
    obtener_snapshot_boosters_batalla,
    aplicar_snapshot_multiplicadores_recompensa_batalla,
)

__all__ = [
    "invalidar_sesiones_battle_activas",
    "obtener_sesion_battle_por_token",
    "validar_cooldown_recompensa_batalla",
    "registrar_cooldown_recompensa_batalla",
    "obtener_recompensa_batalla_permitida",
    "obtener_recompensa_batalla_por_codigo",
    "battle_booster_snapshot_habilitado",
    "obtener_snapshot_boosters_batalla",
    "aplicar_snapshot_multiplicadores_recompensa_batalla",
]

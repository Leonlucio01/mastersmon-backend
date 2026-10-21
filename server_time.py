"""Zona horaria oficial del juego. Compartir entre eventos y tienda."""
from datetime import datetime
from zoneinfo import ZoneInfo

SERVER_TIMEZONE = ZoneInfo("America/Lima")

def server_now():
    return datetime.now(SERVER_TIMEZONE)

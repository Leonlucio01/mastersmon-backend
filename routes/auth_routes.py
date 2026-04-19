"""Future home for authentication endpoints.

Not mounted yet to avoid duplicating endpoints that still live in routes_pokemon.py.
"""
from routes_pokemon import google_login, auth_me

__all__ = ["google_login", "auth_me"]

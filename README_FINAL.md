# Mastersmon Backend Final Compacto

Este paquete mantiene el backend actual y agrega una carpeta `core/` minima para empezar la separacion de helpers sin tocar aun la logica principal.

## Incluye
- app.py
- auth.py
- database.py
- monetization_utils.py
- requirements.txt
- gmail.py
- routes_pokemon.py
- routes_gyms.py
- routes_boss_idle.py
- routes_payments.py
- core/
  - stats.py
  - tokens.py
  - db_checks.py
  - serializers.py
  - activity.py

## Importante
- No elimina funciones del backend actual.
- No activa aun la estructura grande de `routes/` y `services/`.
- La zona horaria actual sigue siendo la del proyecto, incluyendo `America/Lima` en boss/idle.
- La carpeta `core/` por ahora usa wrappers seguros para convivir con la logica existente.

## Siguiente fase recomendada
Migrar helpers compartidos desde `routes_pokemon.py` hacia `core/`, actualizando imports de forma gradual.

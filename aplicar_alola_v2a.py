#!/usr/bin/env python3
"""Apply Alola V2A on a CURRENT repository checkout, not an old frozen copy.
Fails closed if an expected anchor is not found exactly once.
"""
from pathlib import Path
import shutil
import sys

if len(sys.argv) != 2:
    sys.exit('Uso: python aplicar_alola_v2a.py RUTA_AL_BACKEND')
root = Path(sys.argv[1]).resolve()
route = root/'routes_pokemon.py'
if not route.is_file() or not (root/'app.py').is_file():
    sys.exit('No se encontro un checkout de mastersmon-backend (routes_pokemon.py y app.py)')
src = route.read_text(encoding='utf-8')
if 'ALOLA_V2A_ENCOUNTER_PATCH' in src:
    sys.exit('Alola V2A ya esta aplicado; no duplicar')


def once(old, new, label):
    global src
    found = src.count(old)
    if found != 1:
        raise RuntimeError(f'{label}: se esperaba una coincidencia, hay {found}. NO se altero ningun archivo')
    src = src.replace(old, new, 1)

once('from fastapi import APIRouter', 'from alola_v2_service import choose_vulpix_alola, capture_form_id, stat_bases\nfrom fastapi import APIRouter', 'import')

once('''        nivel_min = int(elegido["nivel_min"] or 1)
        nivel_max = int(elegido["nivel_max"] or nivel_min)
''', '''        # ALOLA_V2A_ENCOUNTER_PATCH: form decided on server, never by client.
        alola_form = choose_vulpix_alola(cursor, elegido["pokemon_id"])
        if alola_form:
            elegido = dict(elegido)
            elegido.update(stat_bases(alola_form))
            elegido["tipo"] = "Ice"  # Vulpix Alola, frontend translations may localize
            elegido["imagen"] = alola_form["sprite_normal"]
        nivel_min = int(elegido["nivel_min"] or 1)
        nivel_max = int(elegido["nivel_max"] or nivel_min)
''', 'regional encounter selection')

once('''                (token, usuario_id, zona_id, pokemon_id, nivel, es_shiny, hp_max, ataque, defensa, velocidad,
                 activo, creado_en, expira_en)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW() + (%s * INTERVAL '1 second'))
''', '''                (token, usuario_id, zona_id, pokemon_id, forma_id, nivel, es_shiny, hp_max, ataque, defensa, velocidad,
                 activo, creado_en, expira_en)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW() + (%s * INTERVAL '1 second'))
''', 'map encounter insert columns')

once('''                int(elegido["pokemon_id"]),
                int(nivel),
                bool(es_shiny),
''', '''                int(elegido["pokemon_id"]),
                int(alola_form["form_id"]) if alola_form else None,
                int(nivel),
                bool(es_shiny),
''', 'map encounter insert values')

once('''            "pokemon_id": int(elegido["pokemon_id"]),
            "nombre": elegido["nombre"],
''', '''            "pokemon_id": int(elegido["pokemon_id"]),
            "forma_id": int(alola_form["form_id"]) if alola_form else None,
            "region": "alola" if alola_form else None,
            "nombre": "Vulpix de Alola" if alola_form else elegido["nombre"],
''', 'response encounter identity')

once('''            "imagen": elegido["imagen"],
            "rareza": elegido["rareza"],
''', '''            "imagen": (alola_form["sprite_shiny"] if es_shiny else alola_form["sprite_normal"]) if alola_form else elegido["imagen"],
            "rareza": elegido["rareza"],
''', 'response sprite')

once('''            pokemon_db = cursor.fetchone()
            nombre_pokemon = pokemon_db["nombre"] if pokemon_db and pokemon_db.get("nombre") else f"Pokémon #{int(encuentro['pokemon_id'])}"
''', '''            pokemon_db = cursor.fetchone()
            forma_capturada = capture_form_id(encuentro)
            nombre_pokemon = ("Vulpix de Alola" if forma_capturada else
                (pokemon_db["nombre"] if pokemon_db and pokemon_db.get("nombre") else f"Pokémon #{int(encuentro['pokemon_id'])}"))
''', 'capture form validation')

once('''                    usuario_id, pokemon_id, nivel, experiencia, experiencia_total, es_shiny,
''', '''                    usuario_id, pokemon_id, forma_id, nivel, experiencia, experiencia_total, es_shiny,
''', 'capture insert columns')

once('''                VALUES (%s, %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0)
''', '''                VALUES (%s, %s, %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0)
''', 'capture insert placeholders')

once('''                    usuario["id"],
                    int(encuentro["pokemon_id"]),
                    int(encuentro["nivel"]),
''', '''                    usuario["id"],
                    int(encuentro["pokemon_id"]),
                    forma_capturada,
                    int(encuentro["nivel"]),
''', 'capture insert values')

once('''                "usuario_pokemon_id": usuario_pokemon_id,
                "item_id": int(payload.item_id),
''', '''                "usuario_pokemon_id": usuario_pokemon_id,
                "forma_id": forma_capturada,
                "item_id": int(payload.item_id),
''', 'capture response')

# Collection: LEFT JOIN only; do not overwrite legacy NULL captures.
needle='''                up.id,
                p.id AS pokemon_id,
                p.nombre,
                p.tipo,
                p.imagen,
'''
replacement='''                up.id,
                p.id AS pokemon_id,
                up.forma_id,
                CASE WHEN up.forma_id = 10205 THEN 'Vulpix de Alola' ELSE p.nombre END AS nombre,
                CASE WHEN up.forma_id = 10205 THEN 'Ice' ELSE p.tipo END AS tipo,
                CASE WHEN up.forma_id = 10205 THEN
                  CASE WHEN up.es_shiny THEN a.sprite_shiny ELSE a.sprite_normal END
                ELSE p.imagen END AS imagen,
'''
once(needle,replacement,'collection select regional')
# Join only within collection function rather than global replacing every table join.
start=src.index('def obtener_pokemon_usuario_data(')
stop=src.index('\ndef obtener_equipo_usuario_data(',start)
sub=src[start:stop]
old='''            FROM usuario_pokemon up
            JOIN pokemon p ON p.id = up.pokemon_id
            WHERE up.usuario_id = %s
'''
new='''            FROM usuario_pokemon up
            JOIN pokemon p ON p.id = up.pokemon_id
            LEFT JOIN public.mm30_p8_form_assets a ON a.form_id = up.forma_id
            WHERE up.usuario_id = %s
'''
if sub.count(old)!=1: raise RuntimeError('collection join: anchor missing')
sub=sub.replace(old,new)
old='''                "pokemon_id": row["pokemon_id"],
                "nombre": row["nombre"],
'''
new='''                "pokemon_id": row["pokemon_id"],
                "forma_id": row["forma_id"],
                "region": "alola" if row["forma_id"] == 10205 else None,
                "nombre": row["nombre"],
'''
if sub.count(old)!=1:raise RuntimeError('collection DTO: anchor missing')
sub=sub.replace(old,new)
src=src[:start]+sub+src[stop:]

# Prevent a snapshot with Alolan types being overwritten by the species base when inspecting teams.
# Equipo / battle remain out of scope; feature must stay off for real players until integrated.

shutil.copy2(route, route.with_suffix('.py.alola_v2a_backup'))
route.write_text(src, encoding='utf-8')
shutil.copy2(Path(__file__).with_name('alola_v2_service.py'), root/'alola_v2_service.py')
print('OK: routes_pokemon.py respaldado; servicio copiado. Reiniciar backend solo tras migracion SQL.')

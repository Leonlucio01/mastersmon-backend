"""MastersMon Alola Frontier pilot: server-decided identity limited to one map.

Only Frontier Snow Summit may yield form 10205, and only when explicitly enabled.
Never infer capture form from a browser-submitted field.
"""
import os
import random

ALOLA_VULPIX_SPECIES_ID = 37
ALOLA_VULPIX_FORM_ID = 10205
FRONTIER_SNOW_SUMMIT = 'frontier_snow_summit'


def feature_enabled():
    return os.getenv('MASTERSMON_ALOLA_V2_ENABLED', 'false').strip().lower() in ('1', 'true', 'yes')


def choose_vulpix_alola(cursor, species_id, zone_id):
    """Return the validated regional form only in Frontier Snow Summit.

    The probability is conditional on Vulpix being picked from zona_pokemon;
    it is not the probability of a random map encounter.
    """
    if not feature_enabled() or int(species_id) != ALOLA_VULPIX_SPECIES_ID:
        return None
    cursor.execute('''
        SELECT 1
          FROM public.zonas
         WHERE id = %s AND codigo = %s
         LIMIT 1
    ''', (int(zone_id), FRONTIER_SNOW_SUMMIT))
    if cursor.fetchone() is None:
        return None
    try:
        chance = float(os.getenv('MASTERSMON_ALOLA_SNOW_RATE', '1.0'))
    except (ValueError, TypeError):
        chance = 0.0
    if not 0 < chance <= 1 or random.random() >= chance:
        return None

    cursor.execute('''
        SELECT r.form_id, r.pokemon_entity_id, r.region,
               a.sprite_normal, a.sprite_shiny,
               COALESCE((SELECT jsonb_object_agg(s.identifier, fs.base_stat)
                   FROM public.mm30_p1_form_stats fs
                   JOIN public.mm30_p1_stats s ON s.id=fs.stat_id
                   WHERE fs.form_id=r.form_id), '{}'::jsonb) AS stats,
               COALESCE((SELECT string_agg(t.identifier, '/' ORDER BY ft.slot)
                   FROM public.mm30_p1_effective_form_types ft
                   JOIN public.mm30_p1_types t ON t.id=ft.type_id
                   WHERE ft.form_id=r.form_id), '') AS types
          FROM public.mm30_p6_form_registry r
          JOIN public.mm30_p8_form_assets a ON a.form_id=r.form_id
         WHERE r.species_id=%s AND r.form_id=%s
           AND r.region='alola' AND r.capturable=TRUE
         LIMIT 1
    ''', (ALOLA_VULPIX_SPECIES_ID, ALOLA_VULPIX_FORM_ID))
    row = cursor.fetchone()
    if not row:
        return None
    stats = row['stats'] or {}
    required = ('hp', 'attack', 'defense', 'speed', 'special-attack', 'special-defense')
    if any(int(stats.get(key) or 0) <= 0 for key in required):
        return None
    if not row.get('sprite_normal') or not row.get('sprite_shiny'):
        return None
    return dict(row)


def stat_bases(form):
    s = form['stats']
    return {
        'hp': int(s['hp']), 'ataque': int(s['attack']),
        'defensa': int(s['defense']), 'velocidad': int(s['speed']),
        'ataque_especial': int(s['special-attack']),
        'defensa_especial': int(s['special-defense']),
    }


def capture_form_id(encounter):
    """Capture identity comes exclusively from a persisted encounter token."""
    candidate = encounter.get('forma_id')
    if candidate is None:
        return None
    if int(encounter['pokemon_id']) != ALOLA_VULPIX_SPECIES_ID or int(candidate) != ALOLA_VULPIX_FORM_ID:
        raise ValueError('Unexpected encounter species/form pair')
    return ALOLA_VULPIX_FORM_ID

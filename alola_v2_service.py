"""MastersMon Alola V2A: gated, server-authoritative encounter identity.

Does not modify species, old captures, or evolution rules.
"""
import os
import random

ALOLA_VULPIX_SPECIES_ID = 37
ALOLA_VULPIX_FORM_ID = 10205


def feature_enabled():
    return os.getenv('MASTERSMON_ALOLA_V2_ENABLED', 'false').strip().lower() in ('1', 'true', 'yes')


def choose_vulpix_alola(cursor, species_id):
    """Only when explicitly enabled; region must be capturable in registry.

    One regional encounter among existing Vulpix encounters, never grants one.
    """
    if not feature_enabled() or int(species_id) != ALOLA_VULPIX_SPECIES_ID:
        return None
    try:
        rate = float(os.getenv('MASTERSMON_ALOLA_V2_RATE', '0.15'))
    except ValueError:
        rate = 0.0
    if not 0 < rate <= 1 or random.random() >= rate:
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
    expected = ('hp', 'attack', 'defense', 'speed', 'special-attack', 'special-defense')
    if any(int(stats.get(k) or 0) <= 0 for k in expected):
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
    """The client cannot influence identity. Only a persisted encounter can."""
    candidate = encounter.get('forma_id')
    if candidate is None:
        return None
    if int(encounter['pokemon_id']) != ALOLA_VULPIX_SPECIES_ID or int(candidate) != ALOLA_VULPIX_FORM_ID:
        raise ValueError('Unexpected encounter species/form pair')
    return ALOLA_VULPIX_FORM_ID

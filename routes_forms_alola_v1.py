"""MastersMon regional forms V1: read-only Alola pilot.

Vulpix/Ninetales only. No grants, evolution, capture or battle mutation.
Existing captures with forma_id NULL remain legacy base everywhere else.
"""
from fastapi import APIRouter, HTTPException, Query
from database import get_connection, get_cursor, release_connection

router_forms_alola_v1 = APIRouter(prefix="/pokemon", tags=["Pokemon regional forms - read only"])

PILOT_SPECIES = (37, 38)
PILOT_FORMS = {37: 10205, 38: 10206}


@router_forms_alola_v1.get("/formas-alola/{pokemon_id}")
def formas_alola_piloto(pokemon_id: int, forma_id: int | None = Query(None, ge=1)):
    """Read canonical form data. Does not make any form selectable or capturable."""
    if pokemon_id not in PILOT_SPECIES:
        raise HTTPException(status_code=404, detail="Especie fuera del piloto Alola V1")
    if forma_id is not None and forma_id != PILOT_FORMS[pokemon_id]:
        raise HTTPException(status_code=404, detail="Forma incompatible con la especie piloto")

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = get_cursor(conn)
        cur.execute("""
            SELECT
                p.id AS pokemon_id,
                p.nombre AS especie,
                r.form_id,
                r.pokemon_entity_id AS entidad_id,
                f.identifier AS forma_identificador,
                r.form_identifier,
                r.region,
                r.categoria,
                r.capturable,
                r.seleccionable,
                r.transformacion_habilitada,
                a.sprite_normal,
                a.sprite_shiny,
                a.estado AS estado_asset,
                COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'id_canonico', t.id,
                        'tipo', t.identifier,
                        'slot', et.slot
                    ) ORDER BY et.slot)
                    FROM public.mm30_p1_effective_form_types et
                    JOIN public.mm30_p1_types t ON t.id = et.type_id
                    WHERE et.form_id = r.form_id
                ), '[]'::jsonb) AS tipos,
                COALESCE((
                    SELECT jsonb_object_agg(st.identifier, fs.base_stat)
                    FROM public.mm30_p1_form_stats fs
                    JOIN public.mm30_p1_stats st ON st.id = fs.stat_id
                    WHERE fs.form_id = r.form_id
                ), '{}'::jsonb) AS stats_base_canonicos
            FROM public.mm30_p6_form_registry r
            JOIN public.mm30_p1_forms f ON f.id = r.form_id
            JOIN public.mm30_p1_entities e ON e.id = r.pokemon_entity_id
            JOIN public.pokemon p ON p.id = r.species_id
            LEFT JOIN public.mm30_p8_form_assets a ON a.form_id = r.form_id
            WHERE r.species_id = %s
              AND r.form_id = %s
              AND r.region = 'alola'
              AND f.pokemon_id = e.id
              AND e.species_id = p.id
            LIMIT 1
        """, (pokemon_id, PILOT_FORMS[pokemon_id]))
        result = cur.fetchone()
        if result is None:
            raise HTTPException(status_code=404, detail="Forma Alola no registrada de forma coherente")
        data = dict(result)
        return {
            "ok": True,
            "piloto": "alola_v1_lectura",
            "pokemon": data,
            "politica": {
                "solo_lectura": True,
                "habilitada_en_juego": False,
                "captura_activada_por_este_endpoint": False,
                "evolucion_activada_por_este_endpoint": False,
                "stats_operativos_legacy_sin_cambios": True,
                "sprite_shiny_no_se_sustituye_por_normal": True,
            },
        }
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            release_connection(conn)

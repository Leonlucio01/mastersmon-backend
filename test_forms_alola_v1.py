"""Unit tests without a database; run after installing FastAPI dependencies."""
import sys
from types import ModuleType
from unittest.mock import MagicMock

mock_database = ModuleType('database')
mock_database.get_connection = MagicMock()
mock_database.get_cursor = MagicMock()
mock_database.release_connection = MagicMock()
sys.modules['database'] = mock_database

from fastapi import HTTPException
import routes_forms_alola_v1 as api


def test_reject_wrong_species_and_form():
    for species, form in ((25, None), (37, 10206), (38, 10205)):
        try:
            api.formas_alola_piloto(species, form)
        except HTTPException as exc:
            assert exc.status_code == 404
        else:
            raise AssertionError(f'Must reject {species=}, {form=}')


def test_return_read_only_flags():
    mock_database.get_connection.return_value = MagicMock()
    mock_database.get_cursor.return_value = MagicMock()
    mock_database.get_cursor.return_value.fetchone.return_value = {
        'pokemon_id': 37, 'form_id': 10205, 'region': 'alola',
        'tipos': [{'tipo': 'ice'}], 'stats_base_canonicos': {'hp': 38},
        'capturable': False, 'seleccionable': False,
    }
    result = api.formas_alola_piloto(37, 10205)
    assert result['pokemon']['form_id'] == 10205
    assert result['politica']['solo_lectura']
    assert not result['politica']['habilitada_en_juego']
    assert mock_database.get_cursor.return_value.execute.call_args.args[1] == (37, 10205)
    assert mock_database.release_connection.called

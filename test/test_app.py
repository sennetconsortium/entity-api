import test
test.cwd_to_src()

from unittest.mock import Mock, patch

import pytest

import app as app_module
import test.entities as test_entities


@pytest.fixture()
def app():
    app = app_module.app
    app.config.update({'TESTING': True,})
    # other setup
    yield app
    # clean up

def test_index(app):
    """Test that the index page is working"""
    with app.test_client() as client:
        res = client.get('/')
        assert res.status_code == 200
        assert res.text == 'Hello! This is SenNet Entity API service :)'

@pytest.mark.parametrize('entity_type', [
    ('source'),
    ('sample'),
    ('dataset'),
])
def test_get_entity_by_id_success(app, entity_type):
    """Test that the get entity by id endpoint returns the correct entity"""
    entity = test_entities.get_entity(entity_type)
    entity_id = entity['uuid']
    sennet_ids = test_entities.get_sennet_ids(entity_id, entity_type)

    # Assume user is valid and in sennet read group
    app_module.auth_helper_instance.getUserInfo = Mock(return_value={'hmgroupids': 'testgroup'})
    app_module.auth_helper_instance.get_default_read_group_uuid = Mock(return_value='testgroup')

    with (app.test_client() as client,
          patch('app.schema_manager.get_sennet_ids', return_value=sennet_ids) as mock_get_sennet_ids,
          patch('app.app_neo4j_queries.get_entity', return_value=entity) as mock_get_entity,
          patch('app.schema_manager.get_complete_entity_result', return_value=entity) as mock_get_complete_entity_result):

        res = client.get(f'/entities/{entity_id}',
                         headers={'Authorization': 'Bearer testtoken1234'})

        mock_get_sennet_ids.assert_called_once_with(entity_id)
        mock_get_entity.assert_called_once()
        mock_get_complete_entity_result.assert_called_once()

        assert res.status_code == 200
        assert res.json == entity

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_entity_by_type_success(app, entity_type):
    """Test that the get entity by type endpoint calls neo4j and returns the 
       correct entities"""
    entities = test_entities.get_entities(entity_type)

    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type', return_value=entities) as mock_app_neo4j_queries):

        res = client.get(f'/{entity_type}/entities')

        mock_app_neo4j_queries.assert_called_once()

        assert res.status_code == 200
        assert res.json == entities

def test_create_entity_success(app):
    """Test that the create entity endpoint calls neo4j and returns the correct
        response"""
    with app.test_client() as client:
        expected_response = expected_responses.create_entity_source_response

        # Mock out the calls to the schema manager and neo4j queries
        app_module.schema_manager.create_sennet_ids = Mock(return_value=[{}])
        app_module.schema_manager.get_user_info = Mock(return_value={})
        app_module.schema_manager.generate_triggered_data = Mock(return_value={})
        app_module.app_neo4j_queries.create_entity = Mock(return_value=expected_response)

        res = client.post('/entities/source',
                          json=test_requests.create_entity_source_request,
                          headers={'Authorization': 'Bearer testtoken1234'})

        # Assert
        app_module.schema_manager.create_sennet_ids.assert_called_once()
        app_module.app_neo4j_queries.create_entity.assert_called_once()

        assert res.status_code == 200
        for key, value in expected_response.items():
            assert res.json[key] == value



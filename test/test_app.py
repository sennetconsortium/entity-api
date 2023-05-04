import test
test.cwd_to_src()

from unittest.mock import Mock

import pytest

import app as app_module
import test.requests as test_requests
import test.responses as expected_responses


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

@pytest.mark.parametrize('entity_id, expected_response', [
    ('8af152b82ea653a8e5189267a7e6f82a', expected_responses.get_entity_by_id_source_response),
    ('f8946c392b6bd14595ff5a6b2fdc8497', expected_responses.get_entity_by_id_sample_response),
    ('6a7be8e95c62c74545a29666111899d9', expected_responses.get_entity_by_id_dataset_response),
])
def test_get_entity_by_id(app, entity_id, expected_response):
    """Test that the get entity by id endpoint returns the correct entity"""
    # Assume user is valid and in sennet read group
    app_module.auth_helper_instance.getUserInfo = Mock(return_value={'hmgroupids': 'testgroup'})
    app_module.auth_helper_instance.get_default_read_group_uuid = Mock(return_value='testgroup')

    with app.test_client() as client:
        res = client.get(f'/entities/{entity_id}',
                         headers={'Authorization': 'Bearer testtoken1234'})

        assert res.status_code == 200
        for key, value in expected_response.items():
            assert res.json[key] == value

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_entity_by_type(app, entity_type):
    """Test that the get entity by type endpoint returns the correct entities"""
    with app.test_client() as client:
        res = client.get(f'/{entity_type}/entities')

        assert res.status_code == 200
        assert isinstance(res.json, list)
        assert len(res.json) > 0
        for entity in res.json:
            assert entity['entity_type'] == entity_type.title()

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

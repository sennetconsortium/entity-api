import test

test.cwd_to_src()

import json
import os
import random
import test.utils as test_utils
from unittest.mock import MagicMock, patch

import pytest
from flask import Response

import app as app_module

test_data_dir = os.path.join(os.path.dirname(__file__), 'data')


@pytest.fixture()
def app():
    app = app_module.app
    app.config.update({'TESTING': True})
    # other setup
    yield app
    # clean up


@pytest.fixture(scope="session", autouse=True)
def ontology_mock():
    """Automatically add ontology mock functions to all tests"""
    with (patch('atlas_consortia_commons.ubkg.ubkg_sdk.UbkgSDK', new=test_utils.MockOntology)):
        yield


@pytest.fixture(scope="session", autouse=True)
def auth_helper_mock():
    auth_mock = MagicMock()
    auth_mock.getUserTokenFromRequest.return_value = "test_token"
    auth_mock.getUserInfo.return_value = {
        "sub": "8cb9cda5-1930-493a-8cb9-df6742e0fb42",
        "email": "TESTUSER@example.com",
        "hmgroupids": ["60b692ac-8f6d-485f-b965-36886ecc5a26"],
    }

    # auth_helper_instance gets created (from 'import app') before fixture is called
    app_module.auth_helper_instance = auth_mock
    with (
        patch("hubmap_commons.hm_auth.AuthHelper.configured_instance", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.create", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.instance", return_value=auth_mock),
    ):
        yield


# Index

def test_index(app):
    """Test that the index page is working"""

    with app.test_client() as client:
        res = client.get('/')
        assert res.status_code == 200
        assert res.text == 'Hello! This is SenNet Entity API service :)'


# Get Entity by ID

@pytest.mark.parametrize('entity_type', [
    ('source'),
    ('sample'),
    ('dataset'),
])
def test_get_entity_by_id_success(app, entity_type):
    """Test that the get entity by id endpoint returns the correct entity"""

    with open(os.path.join(test_data_dir, f'get_entity_by_id_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']

    with (app.test_client() as client,
          patch('app.auth_helper_instance.getUserInfo', return_value=test_data['getUserInfo']),
          patch('app.auth_helper_instance.has_read_privs', return_value=test_data['has_read_privs']),
          patch('app.schema_manager.get_sennet_ids', return_value=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', return_value=test_data['get_entity']),
          patch('app.schema_triggers.set_dataset_sources', side_effect=test_data.get('get_associated_sources')),
          patch('app.schema_manager.get_complete_entity_result', return_value=test_data['get_complete_entity_result'])):

        res = client.get(f'/entities/{entity_id}',
                         headers=test_data['headers'])

        assert res.status_code == 200
        assert res.json == test_data['response']


@pytest.mark.parametrize('entity_type, query_key, query_value, status_code', [
    ('source', 'property', 'data_access_level', 200),
    ('source', 'property', 'status', 400),
    ('sample', 'property', 'data_access_level', 200),
    ('sample', 'property', 'status', 400),
    ('dataset', 'property', 'data_access_level', 200),
    ('dataset', 'property', 'status', 200),
    ('source', 'invalid_key', 'status', 400),
    ('source', 'property', 'invalid_value', 400),
])
def test_get_entity_by_id_query(app, entity_type, query_key, query_value, status_code):
    """Test that the get entity by id endpoint can handle specific query parameters"""

    with open(os.path.join(test_data_dir, f'get_entity_by_id_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']
    expected_response = test_data['response']

    with (app.test_client() as client,
          patch('app.auth_helper_instance.getUserInfo', return_value=test_data['getUserInfo']),
          patch('app.auth_helper_instance.has_read_privs', return_value=test_data['has_read_privs']),
          patch('app.schema_manager.get_sennet_ids', return_value=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', return_value=test_data['get_entity']),
          patch('app.schema_manager.get_complete_entity_result', return_value=test_data['get_complete_entity_result'])):

        res = client.get(f'/entities/{entity_id}?{query_key}={query_value}',
                         headers=test_data['headers'])

        assert res.status_code == status_code
        if status_code == 200:
            assert res.text == expected_response[query_value]


# Get Entity by Type

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_entities_by_type_success(app, entity_type):
    """Test that the get entity by type endpoint calls neo4j and returns the
       correct entities"""

    with open(os.path.join(test_data_dir, f'get_entity_by_type_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)

    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type', return_value=test_data['get_entities_by_type']),
          patch('app.schema_neo4j_queries.get_entity_creation_action_activity', side_effect=test_data.get('get_entity_creation_action_activity'))):

        res = client.get(f'/{entity_type}/entities')

        assert res.status_code == 200
        assert res.json == test_data['response']


@pytest.mark.parametrize('entity_type', [
    ('invalid_type'),
])
def test_get_entities_by_type_invalid_type(app, entity_type):
    """Test that the get entity by type endpoint returns a 400 for an invalid
       entity type"""

    with (app.test_client() as client):

        res = client.get(f'/{entity_type}/entities')

        assert res.status_code == 400


@pytest.mark.parametrize('entity_type, query_key, query_value, status_code', [
    ('source', 'property', 'uuid', 200),
    ('sample', 'property', 'uuid', 200),
    ('dataset', 'property', 'uuid', 200),
    ('source', 'invalid_key', 'status', 400),
    ('source', 'property', 'invalid_value', 400),
])
def test_get_entities_by_type_query(app, entity_type, query_key, query_value, status_code):
    """Test that the get entities by type endpoint can handle specific query parameters"""

    with open(os.path.join(test_data_dir, f'get_entity_by_type_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)

    expected_neo4j_query = test_data['get_entities_by_type']
    if status_code == 200:
        expected_neo4j_query = [entity[query_value] for entity in test_data['get_entities_by_type']]
        expected_response = [entity[query_value] for entity in test_data['response']]

    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type', return_value=expected_neo4j_query)):

        res = client.get(f'/{entity_type}/entities?{query_key}={query_value}')

        assert res.status_code == status_code
        if status_code == 200:
            assert res.json == expected_response


# Create Entity

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_create_entity_success(app, entity_type):
    """Test that the create entity endpoint calls neo4j and returns the correct
        response"""

    with open(os.path.join(test_data_dir, f'create_entity_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)

    with (app.test_client() as client,
          patch('app.schema_manager.create_sennet_ids', return_value=test_data['create_sennet_ids']),
          patch('app.schema_manager.get_user_info', return_value=test_data['get_user_info']),
          patch('app.schema_manager.generate_triggered_data', return_value=test_data['generate_triggered_data']),
          patch('app.app_neo4j_queries.create_entity', return_value=test_data['create_entity']),
          patch('app.schema_manager.get_sennet_ids', return_value=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', return_value=test_data['get_entity']),
          patch('app.app_neo4j_queries.get_source_organ_count', return_value=0),
          patch('requests.put', return_value=Response(status=202))):

        res = client.post(f'/entities/{entity_type}',
                          json=test_data['request'],
                          headers=test_data['headers'])

        assert res.status_code == 200
        assert res.json == test_data['response']


@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_create_entity_invalid(app, entity_type):
    """Test that the create entity endpoint returns a 400 for an invalid
       request schema"""

    # purposedly load the wrong entity data to use in the request body
    wrong_entity_type = random.choice([i for i in ['source', 'sample', 'dataset'] if i != entity_type])
    with open(os.path.join(test_data_dir, f'create_entity_success_{wrong_entity_type}.json'), 'r') as f:
        wrong_data = json.load(f)

    with open(os.path.join(test_data_dir, f'create_entity_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)

    with app.test_client() as client:

        res = client.post(f'/entities/{entity_type}',
                          json=wrong_data['request'],
                          headers=test_data['headers'])

        assert res.status_code == 400


# Update Entity

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_update_entity_success(app, entity_type):
    """Test that the update entity endpoint returns the correct entity"""

    with open(os.path.join(test_data_dir, f'update_entity_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']

    with (app.test_client() as client,
          patch('app.schema_manager.get_sennet_ids', side_effect=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', side_effect=test_data['get_entity']),

          patch('app.schema_manager.get_user_info', return_value=test_data['get_user_info']),
          patch('app.schema_manager.generate_triggered_data', side_effect=test_data['generate_triggered_data']),
          patch('app.app_neo4j_queries.update_entity', side_effect=test_data['update_entity']),
          patch('app.schema_manager.get_complete_entity_result', side_effect=test_data['get_complete_entity_result']),
          patch('app.app_neo4j_queries.get_activity_was_generated_by', return_value=test_data['get_activity_was_generated_by']),
          patch('app.app_neo4j_queries.get_activity', return_value=test_data['get_activity']),
          patch('app.app_neo4j_queries.get_source_organ_count', return_value=0),
          patch('app.schema_neo4j_queries.get_entity_creation_action_activity', return_value='lab process'),
          patch('requests.put', return_value=Response(status=202))):

        res = client.put(f'/entities/{entity_id}?return_dict=true',
                         json=test_data['request'],
                         headers=test_data['headers'])

        assert res.status_code == 200
        assert res.json == test_data['response']


@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_update_entity_invalid(app, entity_type):
    """Test that the update entity endpoint returns a 400 for an invalid
       request schema"""

    # purposedly load the wrong entity data to use in the request body
    wrong_entity_type = random.choice([i for i in ['source', 'sample', 'dataset'] if i != entity_type])
    with open(os.path.join(test_data_dir, f'create_entity_success_{wrong_entity_type}.json'), 'r') as f:
        wrong_data = json.load(f)

    with open(os.path.join(test_data_dir, f'update_entity_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']

    with (app.test_client() as client,
          patch('app.schema_manager.get_sennet_ids', side_effect=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', side_effect=test_data['get_entity'])):

        res = client.put(f'/entities/{entity_id}?return_dict=true',
                         json=wrong_data['request'],
                         headers=test_data['headers'])

        assert res.status_code == 400


# Get Ancestors

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_ancestors_success(app, entity_type):
    """Test that the get ancestors endpoint returns the correct entity"""

    with open(os.path.join(test_data_dir, f'get_ancestors_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']

    with (app.test_client() as client,
          patch('app.auth_helper_instance.getUserInfo', return_value=test_data['getUserInfo']),
          patch('app.auth_helper_instance.has_read_privs', return_value=test_data['has_read_privs']),
          patch('app.schema_manager.get_sennet_ids', return_value=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', return_value=test_data['get_entity']),
          patch('app.app_neo4j_queries.get_ancestors', return_value=test_data['get_ancestors'])):

        res = client.get(f'/ancestors/{entity_id}',
                         headers=test_data['headers'])

        assert res.status_code == 200
        assert res.json == test_data['response']


# Get Descendants

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_descendants_success(app, entity_type):
    """Test that the get descendants endpoint returns the correct entity"""

    with open(os.path.join(test_data_dir, f'get_descendants_success_{entity_type}.json'), 'r') as f:
        test_data = json.load(f)
    entity_id = test_data['uuid']

    with (app.test_client() as client,
          patch('app.auth_helper_instance.getUserInfo', return_value=test_data['getUserInfo']),
          patch('app.auth_helper_instance.has_read_privs', return_value=test_data['has_read_privs']),
          patch('app.schema_manager.get_sennet_ids', return_value=test_data['get_sennet_ids']),
          patch('app.app_neo4j_queries.get_entity', return_value=test_data['get_entity']),
          patch('app.app_neo4j_queries.get_descendants', return_value=test_data['get_descendants']),
          patch('app.schema_neo4j_queries.get_entity_creation_action_activity', side_effect=test_data.get('get_entity_creation_action_activity'))):

        res = client.get(f'/descendants/{entity_id}',
                         headers=test_data['headers'])

        assert res.status_code == 200
        assert res.json == test_data['response']


# Validate constraints

@pytest.mark.parametrize('test_name', [
    'source',
    'sample_organ',
    'sample_organ_blood',
    'sample_block',
    'sample_section',
    'sample_suspension',
    'dataset',
])
def test_validate_constraints_new(app, test_name):
    """Test that the validate constraints endpoint returns the correct constraints"""

    with open(os.path.join(test_data_dir, f'validate_constraints_{test_name}.json'), 'r') as f:
        test_data = json.load(f)

    def mock_func(func_name):
        data = test_data[func_name]
        if data and data.get('code'):
            # code being tested uses a StatusCode enum instead of an int
            data['code'] = app_module.StatusCodes(data['code'])
        return data

    with (app.test_client() as client,
          patch('app.get_constraints_by_ancestor', return_value=mock_func('get_constraints_by_ancestor')),
          patch('app.get_constraints_by_descendant', return_value=mock_func('get_constraints_by_descendant'))):

        res = client.post('/constraints' + test_data['query_string'],
                          headers={'Authorization': 'Bearer test_token'},
                          json=test_data['request'])

        assert res.status_code == 200
        assert res.json == test_data['response']

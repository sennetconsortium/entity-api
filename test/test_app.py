import random
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

### Index

def test_index(app):
    """Test that the index page is working"""
    with app.test_client() as client:
        res = client.get('/')
        assert res.status_code == 200
        assert res.text == 'Hello! This is SenNet Entity API service :)'

### Get Entity by ID

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
        mock_get_entity.call_args_list[0].args[1] == entity_id

        mock_get_complete_entity_result.assert_called_once()

        assert res.status_code == 200
        assert res.json == entity

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
    entity = test_entities.get_entity(entity_type)
    entity_id = entity['uuid']
    sennet_ids = test_entities.get_sennet_ids(entity_id, entity_type)

    # Assume user is valid and in sennet read group
    app_module.auth_helper_instance.getUserInfo = Mock(return_value={'hmgroupids': 'testgroup'})
    app_module.auth_helper_instance.get_default_read_group_uuid = Mock(return_value='testgroup')

    with (app.test_client() as client,
          patch('app.schema_manager.get_sennet_ids', return_value=sennet_ids),
          patch('app.app_neo4j_queries.get_entity', return_value=entity),
          patch('app.schema_manager.get_complete_entity_result', return_value=entity)):

        res = client.get(f'/entities/{entity_id}?{query_key}={query_value}',
                         headers={'Authorization': 'Bearer testtoken1234'})

        assert res.status_code == status_code
        if status_code == 200:
            assert res.text == entity[query_value]

def test_get_entity_by_id_unauthorized(app):
    """Test that the get entity by id endpoint returns 401 when no bearer token 
       is given"""
    with app.test_client() as client:

        res = client.get('/entities/8af152b82ea653a8e5189267a7e6f82a')

        assert res.status_code == 401
        assert res.json == {'error': '401 Unauthorized: No Authorization header'}

def test_get_entity_by_id_forbidden_read_group(app):
    """Test that the get entity by id endpoint returns 403 when user is not in
       the sennet read group"""
    with (app.test_client() as client,
          patch('app.auth_helper_instance.getUserInfo', return_value={'hmgroupids': ''}),
          patch('app.auth_helper_instance.get_default_read_group_uuid', return_value='testgroup')):

        res = client.get('/entities/8af152b82ea653a8e5189267a7e6f82a',
                         headers={'Authorization': 'Bearer testtoken1234'})

        assert res.status_code == 403
        assert res.json == {'error': '403 Forbidden: Access not granted'} 

### Get Entity by Type

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample',
    'dataset',
])
def test_get_entities_by_type_success(app, entity_type):
    """Test that the get entity by type endpoint calls neo4j and returns the 
       correct entities"""
    entities = test_entities.get_entities(entity_type)

    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type', return_value=entities) as mock_app_neo4j_queries):

        res = client.get(f'/{entity_type}/entities')

        mock_app_neo4j_queries.assert_called_once()

        assert res.status_code == 200
        assert res.json == entities

@pytest.mark.parametrize('entity_type', [
    ('collection'),
    ('invalid_type'),
])
def test_get_entities_by_type_invalid_type(app, entity_type):
    """Test that the get entity by type endpoint returns a 400 for an invalid 
       entity type"""
    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type') as mock_app_neo4j_queries):

        res = client.get(f'/{entity_type}/entities')

        mock_app_neo4j_queries.assert_not_called()

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
    entities = test_entities.get_entities(entity_type)
    if status_code == 200:
        entities = [entity[query_value] for entity in entities]

    with (app.test_client() as client,
          patch('app.app_neo4j_queries.get_entities_by_type', return_value=entities)):

        res = client.get(f'/{entity_type}/entities?{query_key}={query_value}')

        assert res.status_code == status_code
        if status_code == 200:
            assert res.json == entities

### Create Entity

@pytest.mark.parametrize('entity_type', [
    'source',
    'sample', 
    'dataset',
])
def test_create_entity_success(app, entity_type):
    """Test that the create entity endpoint calls neo4j and returns the correct
        response"""
    entity = test_entities.get_entity(entity_type)
    request = test_entities.request_templates[entity_type]
    # Only include the fields that are expected to be returned
    response = { k: v for k, v in entity.items() 
                 if k not in ['protocol_url', 'image_files', 'thumbnail_file']}

    with (app.test_client() as client,
          patch('app.schema_manager.create_sennet_ids', return_value=[{}]) as mock_create_sennet_ids,
          patch('app.schema_manager.get_user_info', return_value={}),
          patch('app.schema_manager.generate_triggered_data', return_value={}),
          patch('app.app_neo4j_queries.create_entity', return_value=response) as mock_create_entity):
        
        headers = {'Authorization': 'Bearer testtoken1234',}
        if entity_type == 'dataset':
            headers['X-Sennet-Application'] = 'ingest-api'

        res = client.post(f'/entities/{entity_type}',
                          json=request,
                          headers=headers)

        # Assert
        mock_create_sennet_ids.assert_called_once()
        assert mock_create_sennet_ids.call_args_list[0].args[0] == entity_type.title()
        mock_create_entity.assert_called_once()
        assert mock_create_entity.call_args_list[0].args[1] == entity_type.title()

        assert res.status_code == 200
        assert res.json == response
    
@pytest.mark.parametrize('entity_type', [
    'source',
    'sample', 
    'dataset',
])
def test_create_entity_invalid(app, entity_type):
    """Test that the create entity endpoint returns a 400 for an invalid 
       request schema"""
    # Purposely pick the wrong request template
    req_ent = [x for x in test_entities.request_templates if x != entity_type]
    request = test_entities.request_templates[random.choice(req_ent)]

    with app.test_client() as client:
        headers = {'Authorization': 'Bearer testtoken1234',}
        if entity_type == 'dataset':
            headers['X-Sennet-Application'] = 'ingest-api'

        res = client.post(f'/entities/{entity_type}',
                          json=request,
                          headers=headers)
                    
        assert res.status_code == 400

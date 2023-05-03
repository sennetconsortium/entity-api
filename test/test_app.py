import test
test.cwd_to_src()

from unittest.mock import Mock

import pytest

import app as app_module
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

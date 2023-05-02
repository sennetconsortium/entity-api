import test
test.cwd_to_src()

from unittest.mock import Mock

import pytest

import app as app_module

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

def test_get_entity_by_id_source(app):
    """Test that the get entity by id endpoint returns the correct source entity"""
    # Assume user is valid and in sennet read group
    app_module.auth_helper_instance.getUserInfo = Mock(return_value={'hmgroupids': 'testgroup'})
    app_module.auth_helper_instance.get_default_read_group_uuid = Mock(return_value='testgroup')

    with app.test_client() as client:
        res = client.get('/entities/8af152b82ea653a8e5189267a7e6f82a',
                         headers={'Authorization': 'Bearer testtoken1234'})

        assert res.status_code == 200
        assert res.json['created_by_user_displayname'] == 'Max Sibilla'
        assert res.json['created_by_user_email'] == 'MAS400@pitt.edu'
        assert res.json['created_by_user_sub'] == '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c'
        assert res.json['created_timestamp'] == 1681826894432
        assert res.json['data_access_level'] == 'consortium'
        assert res.json['description'] == 'This is a mouse source.'
        assert res.json['entity_type'] == 'Source'
        assert res.json['group_name'] == 'CODCC Testing Group'
        assert res.json['group_uuid'] == '57192604-18e0-11ed-b79b-972795fc9504'
        assert res.json['lab_source_id'] == 'Mouse Source 1'
        assert res.json['protocol_url'] == 'dx.doi.org/10.17504/protocols.io.3byl4j398lo5/v1'
        assert res.json['sennet_id'] == 'SNT986.NQMB.577'
        assert res.json['source_type'] == 'Mouse'
        assert res.json['uuid'] == '8af152b82ea653a8e5189267a7e6f82a'

def test_get_entity_by_id_sample(app):
    """Test that the get entity by id endpoint returns the correct sample entity"""
    # Assume user is valid and in sennet read group
    app_module.auth_helper_instance.getUserInfo = Mock(return_value={'hmgroupids': 'testgroup'})
    app_module.auth_helper_instance.get_default_read_group_uuid = Mock(return_value='testgroup')

    with app.test_client() as client:
        res = client.get('/entities/f8946c392b6bd14595ff5a6b2fdc8497',
                         headers={'Authorization': 'Bearer testtoken1234'})
        
        res.status_code == 200
        assert res.json['created_by_user_displayname'] == 'Max Sibilla'
        assert res.json['created_by_user_email'] == 'MAS400@pitt.edu'
        assert res.json['created_by_user_sub'] == '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c'
        assert res.json['created_timestamp'] == 1681837878549
        assert res.json['data_access_level'] == 'consortium'
        assert res.json['description'] == 'The ancestor is a Source'
        assert res.json['entity_type'] == 'Sample'
        assert res.json['group_name'] == 'Massachusetts General Hospital TDA'
        assert res.json['group_uuid'] == '39a276b3-ee73-11ec-87fd-31892bd489e1'
        assert res.json['lab_tissue_sample_id'] == 'Bulk register 1'
        assert res.json['organ'] == 'BD'
        assert res.json['protocol_url'] == 'https://dx.doi.org/10.17504/protocols.io.8nzhvf6'
        assert res.json['sample_category'] == 'Organ'
        assert res.json['sennet_id'] == 'SNT439.NHLL.499'
        assert res.json['uuid'] == 'f8946c392b6bd14595ff5a6b2fdc8497'

def test_get_entity_by_id_dataset(app):
    """Test that the get entity by id endpoint returns the correct dataset entity"""
    with app.test_client() as client:
        res = client.get('/entities/6a7be8e95c62c74545a29666111899d9',
                         headers={'Authorization': 'Bearer testtoken1234'})
        
        res.status_code == 200

        assert res.json['contains_human_genetic_sequences'] == True
        assert res.json['created_by_user_displayname'] == 'Lisa-Ann Bruney'
        assert res.json['created_by_user_email'] == 'LIB118@pitt.edu'
        assert res.json['created_by_user_sub'] == 'cd17bfa7-24fd-49ca-82ec-2d456ba53730'
        assert res.json['created_timestamp'] == 1681831855041
        assert res.json['data_access_level'] == 'protected'
        assert res.json['data_types'] == ['CODEX']
        assert res.json['entity_type'] == 'Dataset'
        assert res.json['group_name'] == 'University of Pittsburgh TMC'
        assert res.json['group_uuid'] == '28db7a2b-ed8a-11ec-8b0a-9fe9b51132b1'
        assert res.json['lab_dataset_id'] == '897-Dataset'
        assert res.json['local_directory_rel_path'] == 'protected/University of Pittsburgh TMC/6a7be8e95c62c74545a29666111899d9/'
        assert res.json['sennet_id'] == 'SNT554.XLGX.327'
        assert res.json['status'] == 'New'
        assert res.json['title'] == 'CODEX data from the None of a source of unknown age, race and sex'
        assert res.json['uuid'] == '6a7be8e95c62c74545a29666111899d9'

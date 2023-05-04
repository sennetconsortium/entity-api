"""Expected responses for unit tests."""
import test.requests as test_requests

create_entity_source_response = {
    'created_by_user_displayname': 'Test User',
    'created_by_user_email': 'testuser@example.com',
    'created_by_user_sub': '79b0358f-4dfc-4095-99f7-5ad5591717bb',
    'created_timestamp': 1683037611741,
    'data_access_level':'consortium',
    'description': test_requests.create_entity_source_request['description'],
    'entity_type':'Source',
    'group_name':'CODCC Testing Group',
    'group_uuid': test_requests.create_entity_source_request['group_uuid'],
    'lab_source_id': test_requests.create_entity_source_request['lab_source_id'],
    'last_modified_timestamp':1683037611741,
    'last_modified_user_displayname':'Test User',
    'last_modified_user_email':'testuser@example.com',
    'last_modified_user_sub':'79b0358f-4dfc-4095-99f7-5ad5591717bb',
    'sennet_id':'SNT478.WDVP.446',
    'source_type': test_requests.create_entity_source_request['source_type'],
    'uuid':'7db6b8f9738a01a6a9c3d55e6604b88b'
}

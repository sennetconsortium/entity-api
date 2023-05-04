from unittest.mock import Mock
from uuid import uuid4


entity_templates = {
    # source
    'source': {
        'created_by_user_displayname': 'Test User',
        'created_by_user_email': 'TESTUSER@example.com',
        'created_by_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
        'created_timestamp': 1681826847388,
        'data_access_level': 'consortium',
        'description': 'This is a human source. This is an edit.',
        'entity_type': 'Source',
        'group_name': 'CODCC Testing Group',
        'group_uuid': '57192604-18e0-11ed-b79b-972795fc9504',
        'lab_source_id': 'Human Source 1',
        'last_modified_timestamp': 1681844922032,
        'last_modified_user_displayname': 'Test User',
        'last_modified_user_email': 'TESTUSER@example.com',
        'last_modified_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
        'protocol_url': 'dx.doi.org/10.17504/protocols.io.3byl4j398lo5/v1',
        'sennet_id': 'SNT522.GDLF.724',
        'source_type': 'Human',
        'uuid': '325e13d30a43386d97fb3d046677b568'
    },
    # sample
    'sample': {
        'created_by_user_displayname': 'Test User',
        'created_by_user_email': 'TESTUSER@example.com',
        'created_by_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
        'created_timestamp': 1681828388360,
        'data_access_level': 'consortium',
        'entity_type': 'Sample',
        'group_name': 'CODCC Testing Group',
        'group_uuid': '57192604-18e0-11ed-b79b-972795fc9504',
        'image_files': [
            {
                'description': 'Test image',
                'file_uuid': 'ffff1b46e377b91565ed53464cc8d859',
                'filename': 'a4fc82ba0010139e33c6209b917ac9c487172222.png'
            }
        ],
        'lab_tissue_sample_id': 'Human Brain',
        'last_modified_timestamp': 1681828388360,
        'last_modified_user_displayname': 'Test User',
        'last_modified_user_email': 'TESTUSER@example.com',
        'last_modified_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
        'organ': 'BR',
        'protocol_url': 'dx.doi.org/10.17504/protocols.io.3byl4j398lo5/v1',
        'sample_category': 'Organ',
        'sennet_id': 'SNT834.LVJG.639',
        'thumbnail_file': {
            'file_uuid': 'ffffb2c9be7816087e13580e244855c5',
            'filename': 'image_handler.jpg'
        },
        'uuid': '3c4fc147a08429f58856779fcde96f42'
    },
    # dataset
    'dataset': {
        'contains_human_genetic_sequences': True,
        'created_by_user_displayname': 'Test User',
        'created_by_user_email': 'TESTUSER@example.com',
        'created_by_user_sub': 'cd17bfa7-24fd-49ca-82ec-2d456ba53730',
        'created_timestamp': 1681831855041,
        'data_access_level': 'protected',
        'data_types': ['CODEX'],
        'entity_type': 'Dataset',
        'group_name': 'University of Pittsburgh TMC',
        'group_uuid': '28db7a2b-ed8a-11ec-8b0a-9fe9b51132b1',
        'lab_dataset_id': '897-Dataset',
        'last_modified_timestamp': 1681831855041,
        'last_modified_user_displayname': 'Test User',
        'last_modified_user_email': 'TESTUSER@example.com',
        'last_modified_user_sub': 'cd17bfa7-24fd-49ca-82ec-2d456ba53730',
        'local_directory_rel_path': 'protected/University of Pittsburgh TMC/6a7be8e95c62c74545a29666111899d9/',
        'sennet_id': 'SNT554.XLGX.327',
        'status': 'New',
        'uuid': '6a7be8e95c62c74545a29666111899d9'
    }
}

def get_entity(entity_type: str) -> dict:
    entity = entity_templates[entity_type]
    entity['uuid'] = uuid4().hex
    return entity

def get_sennet_ids(uuid: str, entity_type: str) -> Mock:
    return {
        'ancestor_id': '0a5ff4a9c6c1d23932f2f1fbfef8355c',
        'ancestor_ids': ['0a5ff4a9c6c1d23932f2f1fbfef8355c'],
        'email': 'TESTUSER@example.com',
        'hm_uuid': '8af152b82ea653a8e5189267a7e6f82a',
        'sennet_id': 'SNT986.NQMB.577',
        'time_generated': '2023-04-18 14:08:14',
        'type': entity_type.upper(),
        'user_id': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
        'uuid': uuid
    }

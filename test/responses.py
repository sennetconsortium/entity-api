"""Expected responses for unit tests."""


get_entity_by_id_source_response = {
    'created_by_user_displayname': 'Max Sibilla',
    'created_by_user_email': 'MAS400@pitt.edu',
    'created_by_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
    'created_timestamp': 1681826894432,
    'data_access_level': 'consortium',
    'description': 'This is a mouse source.',
    'entity_type': 'Source',
    'group_name': 'CODCC Testing Group',
    'group_uuid': '57192604-18e0-11ed-b79b-972795fc9504',
    'lab_source_id': 'Mouse Source 1',
    'protocol_url': 'dx.doi.org/10.17504/protocols.io.3byl4j398lo5/v1',
    'sennet_id': 'SNT986.NQMB.577',
    'source_type': 'Mouse',
    'uuid': '8af152b82ea653a8e5189267a7e6f82a'
}

get_entity_by_id_sample_response = {
    'created_by_user_displayname': 'Max Sibilla',
    'created_by_user_email': 'MAS400@pitt.edu',
    'created_by_user_sub': '1b8f1792-0ee8-473f-9249-2dc5aa4ce19c',
    'created_timestamp': 1681837878549,
    'data_access_level': 'consortium',
    'description': 'The ancestor is a Source',
    'entity_type': 'Sample',
    'group_name': 'Massachusetts General Hospital TDA',
    'group_uuid': '39a276b3-ee73-11ec-87fd-31892bd489e1',
    'lab_tissue_sample_id': 'Bulk register 1',
    'organ': 'BD',
    'protocol_url': 'https://dx.doi.org/10.17504/protocols.io.8nzhvf6',
    'sample_category': 'Organ',
    'sennet_id': 'SNT439.NHLL.499',
    'uuid': 'f8946c392b6bd14595ff5a6b2fdc8497'
} 

get_entity_by_id_dataset_response = {
    'contains_human_genetic_sequences': True,
    'created_by_user_displayname': 'Lisa-Ann Bruney',
    'created_by_user_email': 'LIB118@pitt.edu',
    'created_by_user_sub': 'cd17bfa7-24fd-49ca-82ec-2d456ba53730',
    'created_timestamp': 1681831855041,
    'data_access_level': 'protected',
    'data_types': ['CODEX'],
    'entity_type': 'Dataset',
    'group_name': 'University of Pittsburgh TMC',
    'group_uuid': '28db7a2b-ed8a-11ec-8b0a-9fe9b51132b1',
    'lab_dataset_id': '897-Dataset',
    'local_directory_rel_path': 'protected/University of Pittsburgh TMC/6a7be8e95c62c74545a29666111899d9/',
    'sennet_id': 'SNT554.XLGX.327',
    'status': 'New',
    'title': 'CODEX data from the None of a source of unknown age, race and sex',
    'uuid': '6a7be8e95c62c74545a29666111899d9'
}

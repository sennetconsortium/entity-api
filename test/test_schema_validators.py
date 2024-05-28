import test

test.cwd_to_src()

import pytest

from schema import schema_validators


@pytest.mark.parametrize('creation_action, succeeds', [
    ('Central Process', True),
    ('central process', True),
    ('Lab Process', True),
    ('lab process', True),
    (None, True),
    ('Multi-Assay Split', False),
    ('multi-assay split', False),
    ('', False),
    ('bad_creation_action', False),
])
def test_validate_single_creation_action(creation_action, succeeds):
    """Test that validate creation action raises a ValueError when creation
       action is invalid"""

    property_key = 'creation_action'
    normalized_entity_type = 'Dataset'
    request = {}
    existing_data_dict = {}
    new_data_dict = {}
    if creation_action is not None:
        new_data_dict['creation_action'] = creation_action

    if succeeds:
        # Test valid creation action
        schema_validators.validate_creation_action(
            property_key, normalized_entity_type, request,
            existing_data_dict, new_data_dict
        )
    else:
        # Test invalid creation action
        with pytest.raises(ValueError):
            schema_validators.validate_creation_action(
                property_key, normalized_entity_type, request,
                existing_data_dict, new_data_dict
            )

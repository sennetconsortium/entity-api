import pytest

GROUP_ID = "7bce0f62-851c-4fdf-afee-5e20581957ba"

GROUP = {
    "name": "EntityAPI-Testing-Group",
    "uuid": GROUP_ID,
    "displayname": "EntityAPI Testing Group",
    "generateuuid": False,
    "data_provider": True,
    "description": "EntityAPI-Testing-Group",
    "tmc_prefix": "TST",
}

USER = {
    "username": "testuser@example.com",
    "name": "Test User",
    "email": "TESTUSER@example.com",
    "sub": "8cb9cda5-1930-493a-8cb9-df6742e0fb42",
    "hmgroupids": [GROUP_ID],
    "group_membership_ids": [GROUP_ID],
}


@pytest.fixture()
def app(auth):
    import app as app_module

    app_module.app.config.update({"TESTING": True})
    app_module.auth_helper_instance = auth
    app_module.schema_manager._auth_helper = auth
    # other setup
    yield app_module.app
    # clean up

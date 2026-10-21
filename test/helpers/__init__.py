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
def app(auth, database):
    import app as app_module

    driver, _ = database

    app_module.app.config.update({"TESTING": True})
    app_module.auth_helper_instance = auth
    app_module.schema_manager._auth_helper = auth

    app_module.neo4j_driver_instance = driver
    app_module.neo4j_driver._driver = driver
    app_module.schema_manager._neo4j_driver = driver

    # other setup
    yield app_module.app
    # cleanup


@pytest.fixture(scope="session", autouse=True)
def clean_up_after_tests():
    """
    Runs once per pytest session and ensures neo4j driver resources are closed
    after all tests finish.
    """
    yield
    try:
        import app as app_module

        driver = getattr(app_module, "neo4j_driver_instance", None)
        if driver:
            driver.close()
    except Exception:
        pass

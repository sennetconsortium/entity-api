from test.helpers import GROUP
from test.helpers.auth import USER
from test.helpers.database import create_provenance
from test.helpers.response import mock_response

import pytest


@pytest.fixture()
def app(auth):
    import app as app_module

    app_module.app.config.update({"TESTING": True})
    app_module.auth_helper_instance = auth
    app_module.schema_manager._auth_helper = auth
    # other setup
    yield app_module.app
    # clean up


# Get Entity Tests


@pytest.mark.usefixtures("lab")
def test_get_source_by_uuid(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source"])
    test_source = test_entities["source"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_source['uuid']}",
        "get",
        mock_response(200, {k: test_source[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_source['uuid']}",
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_source["uuid"]
        assert res.json["sennet_id"] == test_source["sennet_id"]
        assert res.json["description"] == test_source["description"]
        assert res.json["lab_source_id"] == test_source["lab_source_id"]
        assert res.json["source_type"] == test_source["source_type"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_get_source_by_sennet_id(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source"])
    test_source = test_entities["source"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_source['sennet_id']}",
        "get",
        mock_response(200, {k: test_source[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_source['sennet_id']}",
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_source["uuid"]
        assert res.json["sennet_id"] == test_source["sennet_id"]
        assert res.json["description"] == test_source["description"]
        assert res.json["lab_source_id"] == test_source["lab_source_id"]
        assert res.json["source_type"] == test_source["source_type"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_get_organ_sample_by_uuid(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ"])
    test_organ = test_entities["organ"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_organ['uuid']}",
        "get",
        mock_response(200, {k: test_organ[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_organ['uuid']}",
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_organ["uuid"]
        assert res.json["sennet_id"] == test_organ["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_organ["sample_category"]
        assert res.json["organ"] == test_organ["organ"]
        assert res.json["lab_tissue_sample_id"] == test_organ["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["organ_hierarchy"] == "Large Intestine"
        assert res.json["source"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_get_organ_sample_by_sennet_id(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ"])
    test_organ = test_entities["organ"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_organ['sennet_id']}",
        "get",
        mock_response(200, {k: test_organ[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_organ['sennet_id']}",
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_organ["uuid"]
        assert res.json["sennet_id"] == test_organ["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_organ["sample_category"]
        assert res.json["organ"] == test_organ["organ"]
        assert res.json["lab_tissue_sample_id"] == test_organ["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["organ_hierarchy"] == "Large Intestine"
        assert res.json["source"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"

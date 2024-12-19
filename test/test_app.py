from test.helpers import GROUP
from test.helpers.auth import USER
from test.helpers.database import create_provenance, generate_entity
from unittest.mock import MagicMock, patch

import pytest


def mock_response(status_code=200, json_data=None):
    res = MagicMock()
    res.status_code = status_code
    if json_data:
        res.json.return_value = json_data
    return res


@pytest.fixture()
def app(auth):
    import app as app_module

    app_module.app.config.update({"TESTING": True})
    app_module.auth_helper_instance = auth
    app_module.schema_manager._auth_helper = auth
    # other setup
    yield app_module.app
    # clean up


def test_index(app):
    """Test that the index page is working"""

    with app.test_client() as client:
        res = client.get("/")
        assert res.status_code == 200
        assert res.text == "Hello! This is SenNet Entity API service :)"


@pytest.mark.usefixtures("lab")
def test_create_source(app):
    entities = [
        generate_entity(),  # source
        generate_entity(),  # activity
    ]

    # UUID api mock responses
    post_uuid_res = [mock_response(200, [u]) for u in entities]
    put_search_res = mock_response(202)

    with (
        app.test_client() as client,
        patch("requests.post", side_effect=post_uuid_res),
        patch("requests.put", return_value=put_search_res),
    ):
        data = {
            "description": "Testing lab notes",
            "group_uuid": GROUP["uuid"],
            "lab_source_id": "test_lab_source_id",
            "protocol_url": "dx.doi.org/10.17504/protocols.io.3byl4j398lo5/v1",
            "source_type": "Human",
        }

        res = client.post(
            "/entities/source?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == entities[0]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["description"] == data["description"]
        assert res.json["lab_source_id"] == data["lab_source_id"]
        assert res.json["source_type"] == data["source_type"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_create_organ_sample(db_session, app):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source"])
    entities = [
        generate_entity(),  # organ
        generate_entity(),  # activity
        {k: test_entities["source"][k] for k in ["uuid", "sennet_id", "base_id"]},  # source
    ]

    # UUID api mock responses
    get_uuid_res = mock_response(200, entities[2])
    post_uuid_res = [mock_response(200, [u]) for u in entities[:2]]
    put_search_res = mock_response(202)

    with (
        app.test_client() as client,
        patch("requests.get", return_value=get_uuid_res),
        patch("requests.post", side_effect=post_uuid_res),
        patch("requests.put", return_value=put_search_res),
    ):
        data = {
            "sample_category": "Organ",
            "organ": "LV",
            "lab_tissue_sample_id": "test_lab_tissue_organ_id",
            "direct_ancestor_uuid": test_entities["source"]["uuid"],  # source to link to
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == entities[0]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["organ"] == data["organ"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["organ_hierarchy"] == "Liver"
        assert res.json["source"]["uuid"] == test_entities["source"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_create_block_sample(db_session, app):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ"])
    entities = [
        generate_entity(),  # block
        generate_entity(),  # activity
        {k: test_entities["organ"][k] for k in ["uuid", "sennet_id", "base_id"]},  # organ
    ]

    # UUID api mock responses
    get_uuid_res = mock_response(200, entities[2])
    post_uuid_res = [mock_response(200, [u]) for u in entities[:2]]
    put_search_res = mock_response(202)

    with (
        app.test_client() as client,
        patch("requests.get", return_value=get_uuid_res),
        patch("requests.post", side_effect=post_uuid_res),
        patch("requests.put", return_value=put_search_res),
    ):
        data = {
            "sample_category": "Block",
            "lab_tissue_sample_id": "test_lab_tissue_block_id",
            "direct_ancestor_uuid": test_entities["organ"]["uuid"],  # organ to link to
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == entities[0]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == test_entities["organ"]["uuid"]

        assert res.json["source"]["uuid"] == test_entities["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == test_entities["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_create_section_sample(db_session, app):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block"])

    entities = [
        generate_entity(),  # section
        generate_entity(),  # activity
        {k: test_entities["block"][k] for k in ["uuid", "sennet_id", "base_id"]},  # block
    ]

    # UUID api mock responses
    get_uuid_res = mock_response(200, entities[2])
    post_uuid_res = [mock_response(200, [u]) for u in entities[:2]]
    put_search_res = mock_response(202)

    with (
        app.test_client() as client,
        patch("requests.get", return_value=get_uuid_res),
        patch("requests.post", side_effect=post_uuid_res),
        patch("requests.put", return_value=put_search_res),
    ):
        data = {
            "sample_category": "Section",
            "lab_tissue_sample_id": "test_lab_tissue_section_id",
            "direct_ancestor_uuid": test_entities["block"]["uuid"],  # block to link to
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == entities[0]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == test_entities["block"]["uuid"]

        assert res.json["source"]["uuid"] == test_entities["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == test_entities["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


@pytest.mark.usefixtures("lab")
def test_create_dataset(db_session, app):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])

    entities = [
        generate_entity(),  # dataset
        generate_entity(),  # activity
        {k: test_entities["section"][k] for k in ["uuid", "sennet_id", "base_id"]},  # section
    ]

    # UUID api mock responses
    get_uuid_res = mock_response(200, entities[2])
    post_uuid_res = [mock_response(200, [u]) for u in entities[:2]]
    put_search_res = mock_response(202)

    with (
        app.test_client() as client,
        patch("requests.get", return_value=get_uuid_res),
        patch("requests.post", side_effect=post_uuid_res),
        patch("requests.put", return_value=put_search_res),
    ):
        data = {
            "contains_human_genetic_sequences": False,
            "dataset_type": "RNAseq",
            "direct_ancestor_uuids": [test_entities["section"]["uuid"]],  # section to link to
        }

        res = client.post(
            "/entities/dataset?return_all_properties=true",
            json=data,
            headers={
                "Authorization": "Bearer test_token",
                "X-SenNet-Application": "portal-ui",
            },
        )

        assert res.status_code == 200
        assert res.json["uuid"] == entities[0]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["entity_type"] == "Dataset"
        assert res.json["status"] == "New"

        assert res.json["contains_human_genetic_sequences"] == data["contains_human_genetic_sequences"]
        assert res.json["dataset_type"] == data["dataset_type"]
        assert len(res.json["direct_ancestors"]) == 1
        assert res.json["direct_ancestors"][0]["uuid"] == test_entities["section"]["uuid"]

        assert len(res.json["sources"]) == 1
        assert res.json["sources"][0]["uuid"] == test_entities["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == test_entities["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"

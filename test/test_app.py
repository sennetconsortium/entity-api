from test.helpers import GROUP
from test.helpers.auth import USER
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


TEST_ENTITIES = {
    "source": {
        "uuid": "ec55f7bcbb7343f199dcf50666c5e8a6",
        "sennet_id": "SNT123.ABCD.451",
        "base_id": "123ABCD451",
    },
    "organ": {
        "uuid": "bb889cd3edad4d65b6190b6529eeab89",
        "sennet_id": "SNT123.ABCD.453",
        "base_id": "123ABCD453",
    },
    "block": {
        "uuid": "ab8d641cd27e4fce8c52df13376082dd",
        "sennet_id": "SNT123.ABCD.455",
        "base_id": "123ABCD455",
    },
    "section": {
        "uuid": "b34d99d9a4c34cba993b55a17925559e",
        "sennet_id": "SNT123.ABCD.457",
        "base_id": "123ABCD457",
    },
    "dataset": {
        "uuid": "6c1e4cef787849c3a228fe6882d5926d",
        "sennet_id": "SNT123.ABCD.459",
        "base_id": "123ABCD459",
    },
}


def test_index(app):
    """Test that the index page is working"""

    with app.test_client() as client:
        res = client.get("/")
        assert res.status_code == 200
        assert res.text == "Hello! This is SenNet Entity API service :)"


@pytest.mark.usefixtures("lab")
def test_create_source(app):
    entities = [
        TEST_ENTITIES["source"],
        {
            "uuid": "014cf93c2f7c41b080a3d3c59eb71cdc",  # activity
            "sennet_id": "SNT123.ABCD.450",
            "base_id": "123ABCD450",
        },
    ]
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
        assert res.json["uuid"] == TEST_ENTITIES["source"]["uuid"]
        assert res.json["sennet_id"] == TEST_ENTITIES["source"]["sennet_id"]
        assert res.json["description"] == data["description"]
        assert res.json["lab_source_id"] == data["lab_source_id"]
        assert res.json["source_type"] == data["source_type"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


def test_create_organ_sample(app):
    entities = [
        TEST_ENTITIES["organ"],
        {
            "uuid": "f3976f0da50c4b6286cccd6d4f1d9835",  # activity
            "sennet_id": "SNT123.ABCD.452",
            "base_id": "123ABCD452",
        },
        TEST_ENTITIES["source"],
    ]
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
            "direct_ancestor_uuid": TEST_ENTITIES["source"]["uuid"],  # source from previous test
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == TEST_ENTITIES["organ"]["uuid"]
        assert res.json["sennet_id"] == TEST_ENTITIES["organ"]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["organ"] == data["organ"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == TEST_ENTITIES["source"]["uuid"]

        assert res.json["organ_hierarchy"] == "Liver"
        assert res.json["source"]["uuid"] == TEST_ENTITIES["source"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


def test_create_block_sample(app):
    entities = [
        TEST_ENTITIES["block"],
        {
            "uuid": "cd0fb0bf0ceb4463be63fb60c9e0bf97",  # activity
            "sennet_id": "SNT123.ABCD.454",
            "base_id": "123ABCD454",
        },
        TEST_ENTITIES["organ"],
    ]
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
            "direct_ancestor_uuid": TEST_ENTITIES["organ"]["uuid"],  # organ from previous test
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == TEST_ENTITIES["block"]["uuid"]
        assert res.json["sennet_id"] == TEST_ENTITIES["block"]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == TEST_ENTITIES["organ"]["uuid"]

        assert res.json["source"]["uuid"] == TEST_ENTITIES["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == TEST_ENTITIES["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


def test_create_section_sample(app):
    entities = [
        TEST_ENTITIES["section"],
        {
            "uuid": "5a3e7ac21849416894f018b58d428a64",  # activity
            "sennet_id": "SNT123.ABCD.456",
            "base_id": "123ABCD456",
        },
        TEST_ENTITIES["block"],
    ]
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
            "direct_ancestor_uuid": TEST_ENTITIES["block"]["uuid"],  # block from previous test
        }

        res = client.post(
            "/entities/sample?return_all_properties=true",
            json=data,
            headers={"Authorization": "Bearer test_token"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == TEST_ENTITIES["section"]["uuid"]
        assert res.json["sennet_id"] == entities[0]["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == data["sample_category"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]
        assert res.json["direct_ancestor"]["uuid"] == TEST_ENTITIES["block"]["uuid"]

        assert res.json["source"]["uuid"] == TEST_ENTITIES["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == TEST_ENTITIES["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"


def test_create_dataset(app):
    entities = [
        TEST_ENTITIES["dataset"],
        {
            "uuid": "130d460fa6104d8b99c32bea689704b7",  # activity
            "sennet_id": "SNT123.ABCD.458",
            "base_id": "123ABCD458",
        },
        TEST_ENTITIES["section"],
    ]
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
            "direct_ancestor_uuids": [
                TEST_ENTITIES["section"]["uuid"]  # section from previous test
            ],
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
        assert res.json["uuid"] == TEST_ENTITIES["dataset"]["uuid"]
        assert res.json["sennet_id"] == TEST_ENTITIES["dataset"]["sennet_id"]
        assert res.json["entity_type"] == "Dataset"
        assert res.json["status"] == "New"

        assert res.json["contains_human_genetic_sequences"] == data["contains_human_genetic_sequences"]
        assert res.json["dataset_type"] == data["dataset_type"]
        assert len(res.json["direct_ancestors"]) == 1
        assert res.json["direct_ancestors"][0]["uuid"] == TEST_ENTITIES["section"]["uuid"]

        assert len(res.json["sources"]) == 1
        assert res.json["sources"][0]["uuid"] == TEST_ENTITIES["source"]["uuid"]
        assert len(res.json["origin_samples"]) == 1
        assert res.json["origin_samples"][0]["uuid"] == TEST_ENTITIES["organ"]["uuid"]

        assert res.json["group_uuid"] == GROUP["uuid"]
        assert res.json["group_name"] == GROUP["displayname"]
        assert res.json["created_by_user_displayname"] == USER["name"]
        assert res.json["created_by_user_email"] == USER["email"]
        assert res.json["created_by_user_sub"] == USER["sub"]
        assert res.json["data_access_level"] == "consortium"

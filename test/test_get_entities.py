from test.helpers import GROUP, USER
from test.helpers.auth import AUTH_TOKEN
from test.helpers.database import create_provenance
from test.helpers.response import mock_response


# Get Entity Tests


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
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
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
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
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


def test_get_source_by_uuid_no_auth(db_session, app, requests):
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
        )

        assert res.status_code == 403


def test_get_source_by_uuid_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"}
    ])
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
        )

        assert res.status_code == 200
        assert res.json["data_access_level"] == "public"
        assert not any(k.startswith("lab_") for k in res.json.keys())  # no lab id fields


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
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
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
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
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


def test_get_organ_sample_by_uuid_no_auth(db_session, app, requests):
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
        )

        assert res.status_code == 403


def test_get_organ_sample_by_uuid_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"}
    ])
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
        )

        assert res.status_code == 200
        assert res.json["data_access_level"] == "public"
        assert not any(k.startswith("lab_") for k in res.json.keys())  # no lab id fields


def test_get_block_sample_by_uuid(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block"])
    test_block = test_entities["block"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_block['uuid']}",
        "get",
        mock_response(200, {k: test_block[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_block['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_block["uuid"]
        assert res.json["sennet_id"] == test_block["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_block["sample_category"]
        assert res.json["lab_tissue_sample_id"] == test_block["lab_tissue_sample_id"]
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


def test_get_block_sample_by_sennet_id(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block"])
    test_block = test_entities["block"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_block['sennet_id']}",
        "get",
        mock_response(200, {k: test_block[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_block['sennet_id']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_block["uuid"]
        assert res.json["sennet_id"] == test_block["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_block["sample_category"]
        assert res.json["lab_tissue_sample_id"] == test_block["lab_tissue_sample_id"]
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


def test_get_block_sample_by_uuid_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block"])
    test_block = test_entities["block"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_block['uuid']}",
        "get",
        mock_response(200, {k: test_block[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_block['uuid']}",
        )

        assert res.status_code == 403


def test_get_block_sample_by_uuid_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"}
    ])
    test_block = test_entities["block"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_block['uuid']}",
        "get",
        mock_response(200, {k: test_block[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_block['uuid']}",
        )

        assert res.status_code == 200
        assert res.json["data_access_level"] == "public"
        assert not any(k.startswith("lab_") for k in res.json.keys())  # no lab id fields


def test_get_section_sample_by_uuid(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])
    test_section = test_entities["section"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['uuid']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_section['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_section["uuid"]
        assert res.json["sennet_id"] == test_section["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_section["sample_category"]
        assert res.json["lab_tissue_sample_id"] == test_section["lab_tissue_sample_id"]
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


def test_get_section_sample_by_sennet_id(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])
    test_section = test_entities["section"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['sennet_id']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_section['sennet_id']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_section["uuid"]
        assert res.json["sennet_id"] == test_section["sennet_id"]
        assert res.json["entity_type"] == "Sample"

        assert res.json["sample_category"] == test_section["sample_category"]
        assert res.json["lab_tissue_sample_id"] == test_section["lab_tissue_sample_id"]
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


def test_get_section_sample_by_uuid_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])
    test_section = test_entities["section"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['uuid']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_section['uuid']}",
        )

        assert res.status_code == 403


def test_get_section_sample_by_uuid_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"}
    ])
    test_section = test_entities["section"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['uuid']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_section['uuid']}",
        )

        assert res.status_code == 200
        assert res.json["data_access_level"] == "public"
        assert not any(k.startswith("lab_") for k in res.json.keys())  # no lab id fields


def test_get_dataset_by_uuid(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
    test_dataset = test_entities["dataset"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_dataset['uuid']}",
        "get",
        mock_response(200, {k: test_dataset[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_dataset['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_dataset["uuid"]
        assert res.json["sennet_id"] == test_dataset["sennet_id"]
        assert res.json["entity_type"] == "Dataset"
        assert res.json["status"] == "New"

        assert res.json["contains_human_genetic_sequences"] == test_dataset["contains_human_genetic_sequences"]
        assert res.json["dataset_type"] == test_dataset["dataset_type"]
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


def test_get_dataset_by_sennet_id(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
    test_dataset = test_entities["dataset"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_dataset['sennet_id']}",
        "get",
        mock_response(200, {k: test_dataset[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_dataset['sennet_id']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_dataset["uuid"]
        assert res.json["sennet_id"] == test_dataset["sennet_id"]
        assert res.json["entity_type"] == "Dataset"
        assert res.json["status"] == "New"

        assert res.json["contains_human_genetic_sequences"] == test_dataset["contains_human_genetic_sequences"]
        assert res.json["dataset_type"] == test_dataset["dataset_type"]
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


def test_get_dataset_by_uuid_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])
    test_section = test_entities["section"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['uuid']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_section['uuid']}",
        )

        assert res.status_code == 403


def test_get_dataset_by_uuid_published_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"},
        {"type": "dataset", "data_access_level": "public", "status": "Published"}
    ])
    test_dataset = test_entities["dataset"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_dataset['uuid']}",
        "get",
        mock_response(200, {k: test_dataset[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.get(
            f"/entities/{test_dataset['uuid']}",
        )

        assert res.status_code == 200
        assert res.json["data_access_level"] == "public"
        assert res.json["status"] == "Published"
        assert not any(k.startswith("lab_") for k in res.json.keys())  # no lab id fields

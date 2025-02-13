from test.helpers.auth import AUTH_TOKEN
from test.helpers.database import create_provenance, get_entity
from test.helpers.response import mock_response


# Update Entity Tests


def test_update_source(app, requests, db_session):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source"])
    test_source = test_entities["source"]

    # uuid and search api mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    search_api_url = app.config["SEARCH_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_source['uuid']}",
        "get",
        mock_response(200, {k: test_source[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )
    requests.add_response(f"{search_api_url}/reindex/{test_source['uuid']}", "put", mock_response(202))

    with app.test_client() as client:
        data = {
            "description": "New Testing lab notes",
            "lab_source_id": "new_test_lab_source_id",
        }

        res = client.put(
            f"/entities/{test_source['uuid']}?return_all_properties=true",
            json=data,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_source["uuid"]
        assert res.json["sennet_id"] == test_source["sennet_id"]

        assert res.json["description"] == data["description"]
        assert res.json["lab_source_id"] == data["lab_source_id"]

        # check database
        db_entity = get_entity(test_source["uuid"], db_session)
        assert db_entity["description"] == data["description"]
        assert db_entity["lab_source_id"] == data["lab_source_id"]


def test_update_organ_sample(app, requests, db_session):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ"])
    test_organ = test_entities["organ"]

    # uuid and search api mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    search_api_url = app.config["SEARCH_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_organ['uuid']}",
        "get",
        mock_response(200, {k: test_organ[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )
    requests.add_response(f"{search_api_url}/reindex/{test_organ['uuid']}", "put", mock_response(202))

    with app.test_client() as client:
        data = {
            "description": "New Testing lab notes",
            "lab_tissue_sample_id": "new_test_lab_tissue_organ_id",
        }

        res = client.put(
            f"/entities/{test_organ['uuid']}?return_all_properties=true",
            json=data,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_organ["uuid"]
        assert res.json["sennet_id"] == test_organ["sennet_id"]

        assert res.json["description"] == data["description"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]

        # check database
        db_entity = get_entity(test_organ["uuid"], db_session)
        assert db_entity["description"] == data["description"]
        assert db_entity["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]


def test_update_block_sample(app, requests, db_session):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block"])
    test_block = test_entities["block"]

    # uuid and search api mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    search_api_url = app.config["SEARCH_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_block['uuid']}",
        "get",
        mock_response(200, {k: test_block[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )
    requests.add_response(f"{search_api_url}/reindex/{test_block['uuid']}", "put", mock_response(202))

    with app.test_client() as client:
        data = {
            "description": "New Testing lab notes",
            "lab_tissue_sample_id": "new_test_lab_tissue_block_id",
        }

        res = client.put(
            f"/entities/{test_block['uuid']}?return_all_properties=true",
            json=data,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_block["uuid"]
        assert res.json["sennet_id"] == test_block["sennet_id"]

        assert res.json["description"] == data["description"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]

        # check database
        db_entity = get_entity(test_block["uuid"], db_session)
        assert db_entity["description"] == data["description"]
        assert db_entity["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]


def test_update_section_sample(app, requests, db_session):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section"])
    test_section = test_entities["section"]

    # uuid and search api mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    search_api_url = app.config["SEARCH_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_section['uuid']}",
        "get",
        mock_response(200, {k: test_section[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )
    requests.add_response(f"{search_api_url}/reindex/{test_section['uuid']}", "put", mock_response(202))

    with app.test_client() as client:
        data = {
            "description": "New Testing lab notes",
            "lab_tissue_sample_id": "new_test_lab_tissue_section_id",
        }

        res = client.put(
            f"/entities/{test_section['uuid']}?return_all_properties=true",
            json=data,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_section["uuid"]
        assert res.json["sennet_id"] == test_section["sennet_id"]

        assert res.json["description"] == data["description"]
        assert res.json["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]

        # check database
        db_entity = get_entity(test_section["uuid"], db_session)
        assert db_entity["description"] == data["description"]
        assert db_entity["lab_tissue_sample_id"] == data["lab_tissue_sample_id"]


def test_update_dataset(app, requests, db_session):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
    test_dataset = test_entities["dataset"]

    # uuid and search api mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    search_api_url = app.config["SEARCH_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_dataset['uuid']}",
        "get",
        mock_response(200, {k: test_dataset[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )
    requests.add_response(f"{search_api_url}/reindex/{test_dataset['uuid']}", "put", mock_response(202))

    with app.test_client() as client:
        data = {
            "description": "New Testing lab notes",
            "contributors": [
                {
                    "affiliation": "Test Laboratory",
                    "display_name": "Teßt '$PI&*\" Üser",
                    "email": "Teßt.Üser@jax.org",
                    "first_name": "Teßt",
                    "is_contact": "Yes",
                    "is_operator": "Yes",
                    "is_principal_investigator": "Yes",
                    "last_name": "Üser",
                    "metadata_schema_id": "94dae6f8-0756-4ab0-a47b-138e446a9501",
                    "middle_name_or_initial": "'$PI&*\"",
                    "orcid": "0000-0000-0000-0000"
                },
            ],
        }

        res = client.put(
            f"/entities/{test_dataset['uuid']}?return_all_properties=true",
            json=data,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json["uuid"] == test_dataset["uuid"]
        assert res.json["sennet_id"] == test_dataset["sennet_id"]

        assert res.json["description"] == data["description"]
        assert res.json["contributors"] == data["contributors"]

        # check database
        db_entity = get_entity(test_dataset["uuid"], db_session)
        assert db_entity["description"] == data["description"]

from test.helpers.auth import AUTH_TOKEN
from test.helpers.database import create_provenance
from test.helpers.response import mock_response


# Get Descendants Tests


def test_get_source_descendants(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
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
            f"/descendants/{test_source['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 4

        ancestor_uuids = [ancestor["uuid"] for ancestor in res.json]
        assert test_entities["organ"]["uuid"] in ancestor_uuids
        assert test_entities["block"]["uuid"] in ancestor_uuids
        assert test_entities["section"]["uuid"] in ancestor_uuids
        assert test_entities["dataset"]["uuid"] in ancestor_uuids


def test_get_organ_sample_descendants(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
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
            f"/descendants/{test_organ['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 3

        ancestor_uuids = [ancestor["uuid"] for ancestor in res.json]
        assert test_entities["block"]["uuid"] in ancestor_uuids
        assert test_entities["section"]["uuid"] in ancestor_uuids
        assert test_entities["dataset"]["uuid"] in ancestor_uuids


def test_get_block_sample_descendants(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
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
            f"/descendants/{test_block['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 2

        ancestor_uuids = [ancestor["uuid"] for ancestor in res.json]
        assert test_entities["section"]["uuid"] in ancestor_uuids
        assert test_entities["dataset"]["uuid"] in ancestor_uuids


def test_get_section_sample_descendants(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ", "block", "section", "dataset"])
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
            f"/descendants/{test_section['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 1

        ancestor_uuids = [ancestor["uuid"] for ancestor in res.json]
        assert test_entities["dataset"]["uuid"] in ancestor_uuids


def test_get_dataset_descendants(db_session, app, requests):
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
            f"/descendants/{test_dataset['uuid']}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json == []

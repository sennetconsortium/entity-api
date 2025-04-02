from test.helpers.auth import AUTH_TOKEN
from test.helpers.database import create_provenance
from test.helpers.response import mock_response

filters = {
    "filter_properties": [
        "lab_source_id",
        "lab_tissue_sample_id",
        "lab_dataset_id",
        "origin_samples",
        "organ_hierarchy",
        "creation_action",
        "files",
        "metadata",
        "ingest_metadata",
        "cedar_mapped_metadata",
        "source_mapped_metadata"
    ],
    "is_include": True
}

filters_uuid = {
    "filter_properties": [
        "uuid"
    ],
    "is_include": True
}


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


def test_get_source_descendants_with_filters(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_source['uuid']}",
            json=filters,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 4

        organ = next(a for a in res.json if a["uuid"] == test_entities["organ"]["uuid"])
        origin_samples = organ.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert organ == {
            "creation_action": "Create Organ Activity",
            "data_access_level": test_entities["organ"]["data_access_level"],
            "entity_type": test_entities["organ"]["entity_type"],
            "group_name": test_entities["organ"]["group_name"],
            "group_uuid": test_entities["organ"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["organ"]["lab_tissue_sample_id"],
            "organ": test_entities["organ"]["organ"],
            "organ_hierarchy": "Large Intestine",
            "sample_category": test_entities["organ"]["sample_category"],
            "sennet_id": test_entities["organ"]["sennet_id"],
            "uuid": test_entities["organ"]["uuid"],
        }

        block = next(a for a in res.json if a["uuid"] == test_entities["block"]["uuid"])
        origin_samples = block.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert block == {
            "creation_action": "Create Block Activity",
            "data_access_level": test_entities["block"]["data_access_level"],
            "entity_type": test_entities["block"]["entity_type"],
            "group_name": test_entities["block"]["group_name"],
            "group_uuid": test_entities["block"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["block"]["lab_tissue_sample_id"],
            "sample_category": test_entities["block"]["sample_category"],
            "sennet_id": test_entities["block"]["sennet_id"],
            "uuid": test_entities["block"]["uuid"],
        }

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert section == {
            "creation_action": "Create Section Activity",
            "data_access_level": test_entities["section"]["data_access_level"],
            "entity_type": test_entities["section"]["entity_type"],
            "group_name": test_entities["section"]["group_name"],
            "group_uuid": test_entities["section"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["section"]["lab_tissue_sample_id"],
            "sample_category": test_entities["section"]["sample_category"],
            "sennet_id": test_entities["section"]["sennet_id"],
            "uuid": test_entities["section"]["uuid"],
        }

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert dataset == {
            "contains_human_genetic_sequences": test_entities["dataset"]["contains_human_genetic_sequences"],
            "creation_action": "Create Dataset Activity",
            "data_access_level": test_entities["dataset"]["data_access_level"],
            "dataset_type": test_entities["dataset"]["dataset_type"],
            "entity_type": test_entities["dataset"]["entity_type"],
            "group_name": test_entities["dataset"]["group_name"],
            "group_uuid": test_entities["dataset"]["group_uuid"],
            "lab_dataset_id": test_entities["dataset"]["lab_dataset_id"],
            "sennet_id": test_entities["dataset"]["sennet_id"],
            "status": test_entities["dataset"]["status"],
            "uuid": test_entities["dataset"]["uuid"],
        }


def test_get_source_descendants_with_filters_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"},
        {"type": "dataset", "data_access_level": "public", "status": "Published"}
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
        res = client.post(
            f"/descendants/{test_source['uuid']}",
            json=filters,
        )

        assert res.status_code == 200
        assert len(res.json) == 4

        organ = next(a for a in res.json if a["uuid"] == test_entities["organ"]["uuid"])
        origin_samples = organ.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in organ.keys())  # no lab id fields

        block = next(a for a in res.json if a["uuid"] == test_entities["block"]["uuid"])
        origin_samples = block.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in block.keys())  # no lab id fields

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in section.keys())  # no lab id fields

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in dataset.keys())  # no lab id fields


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


def test_get_organ_sample_descendants_with_filters(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_organ['uuid']}",
            json=filters,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 3

        block = next(a for a in res.json if a["uuid"] == test_entities["block"]["uuid"])
        origin_samples = block.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert block == {
            "creation_action": "Create Block Activity",
            "data_access_level": test_entities["block"]["data_access_level"],
            "entity_type": test_entities["block"]["entity_type"],
            "group_name": test_entities["block"]["group_name"],
            "group_uuid": test_entities["block"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["block"]["lab_tissue_sample_id"],
            "sample_category": test_entities["block"]["sample_category"],
            "sennet_id": test_entities["block"]["sennet_id"],
            "uuid": test_entities["block"]["uuid"],
        }

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert section == {
            "creation_action": "Create Section Activity",
            "data_access_level": test_entities["section"]["data_access_level"],
            "entity_type": test_entities["section"]["entity_type"],
            "group_name": test_entities["section"]["group_name"],
            "group_uuid": test_entities["section"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["section"]["lab_tissue_sample_id"],
            "sample_category": test_entities["section"]["sample_category"],
            "sennet_id": test_entities["section"]["sennet_id"],
            "uuid": test_entities["section"]["uuid"],
        }

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert dataset == {
            "contains_human_genetic_sequences": test_entities["dataset"]["contains_human_genetic_sequences"],
            "creation_action": "Create Dataset Activity",
            "data_access_level": test_entities["dataset"]["data_access_level"],
            "dataset_type": test_entities["dataset"]["dataset_type"],
            "entity_type": test_entities["dataset"]["entity_type"],
            "group_name": test_entities["dataset"]["group_name"],
            "group_uuid": test_entities["dataset"]["group_uuid"],
            "lab_dataset_id": test_entities["dataset"]["lab_dataset_id"],
            "sennet_id": test_entities["dataset"]["sennet_id"],
            "status": test_entities["dataset"]["status"],
            "uuid": test_entities["dataset"]["uuid"],
        }


def test_get_organ_sample_descendants_with_filters_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"},
        {"type": "dataset", "data_access_level": "public", "status": "Published"}
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
        res = client.post(
            f"/descendants/{test_organ['uuid']}",
            json=filters,
        )

        assert res.status_code == 200
        assert len(res.json) == 3

        block = next(a for a in res.json if a["uuid"] == test_entities["block"]["uuid"])
        origin_samples = block.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in block.keys())  # no lab id fields

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in section.keys())  # no lab id fields

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in dataset.keys())  # no lab id fields


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


def test_get_block_sample_descendants_with_filters(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_block['uuid']}",
            json=filters,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 2

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert section == {
            "creation_action": "Create Section Activity",
            "data_access_level": test_entities["section"]["data_access_level"],
            "entity_type": test_entities["section"]["entity_type"],
            "group_name": test_entities["section"]["group_name"],
            "group_uuid": test_entities["section"]["group_uuid"],
            "lab_tissue_sample_id": test_entities["section"]["lab_tissue_sample_id"],
            "sample_category": test_entities["section"]["sample_category"],
            "sennet_id": test_entities["section"]["sennet_id"],
            "uuid": test_entities["section"]["uuid"],
        }

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert dataset == {
            "contains_human_genetic_sequences": test_entities["dataset"]["contains_human_genetic_sequences"],
            "creation_action": "Create Dataset Activity",
            "data_access_level": test_entities["dataset"]["data_access_level"],
            "dataset_type": test_entities["dataset"]["dataset_type"],
            "entity_type": test_entities["dataset"]["entity_type"],
            "group_name": test_entities["dataset"]["group_name"],
            "group_uuid": test_entities["dataset"]["group_uuid"],
            "lab_dataset_id": test_entities["dataset"]["lab_dataset_id"],
            "sennet_id": test_entities["dataset"]["sennet_id"],
            "status": test_entities["dataset"]["status"],
            "uuid": test_entities["dataset"]["uuid"],
        }


def test_get_block_sample_descendants_with_filters_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"},
        {"type": "dataset", "data_access_level": "public", "status": "Published"}
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
        res = client.post(
            f"/descendants/{test_block['uuid']}",
            json=filters,
        )

        assert res.status_code == 200
        assert len(res.json) == 2

        section = next(a for a in res.json if a["uuid"] == test_entities["section"]["uuid"])
        origin_samples = section.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in section.keys())  # no lab id fields

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in dataset.keys())  # no lab id fields


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


def test_get_section_sample_descendants_with_filters(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_section['uuid']}",
            json=filters,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert len(res.json) == 1

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert origin_samples[0]["uuid"] == test_entities["organ"]["uuid"]
        assert dataset == {
            "contains_human_genetic_sequences": test_entities["dataset"]["contains_human_genetic_sequences"],
            "creation_action": "Create Dataset Activity",
            "data_access_level": test_entities["dataset"]["data_access_level"],
            "dataset_type": test_entities["dataset"]["dataset_type"],
            "entity_type": test_entities["dataset"]["entity_type"],
            "group_name": test_entities["dataset"]["group_name"],
            "group_uuid": test_entities["dataset"]["group_uuid"],
            "lab_dataset_id": test_entities["dataset"]["lab_dataset_id"],
            "sennet_id": test_entities["dataset"]["sennet_id"],
            "status": test_entities["dataset"]["status"],
            "uuid": test_entities["dataset"]["uuid"],
        }

def test_get_block_sample_descendants_uuids_with_filters_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, ["source", "organ",
                                                   {"type": "block", "data_access_level": "public"},
                                                   {"type": "section", "data_access_level": "public"},
                                                   {"type": "dataset", "data_access_level": "public", "status": "Published"},
                                                    ])
    test_entity = test_entities["block"]

    # uuid mock responses
    uuid_api_url = app.config["UUID_API_URL"]
    requests.add_response(
        f"{uuid_api_url}/uuid/{test_entity['uuid']}",
        "get",
        mock_response(200, {k: test_entity[k] for k in ["uuid", "sennet_id", "base_id"]}),
    )

    with app.test_client() as client:
        res = client.post(
            f"/descendants/{test_entity['uuid']}",
            json=filters_uuid,
        )

        assert res.status_code == 200
        assert len(res.json) == 2

        strs = [a for a in res.json if isinstance(a, str)]
        assert len(strs) == 2
        dataset_uuid = next(a for a in res.json if a == test_entities["dataset"]["uuid"])
        assert dataset_uuid == test_entities["dataset"]["uuid"]


def test_get_section_sample_descendants_with_filters_public_no_auth(db_session, app, requests):
    # Create provenance in test database
    test_entities = create_provenance(db_session, [
        {"type": "source", "data_access_level": "public"},
        {"type": "organ", "data_access_level": "public"},
        {"type": "block", "data_access_level": "public"},
        {"type": "section", "data_access_level": "public"},
        {"type": "dataset", "data_access_level": "public", "status": "Published"}
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
        res = client.post(
            f"/descendants/{test_section['uuid']}",
            json=filters,
        )

        assert res.status_code == 200
        assert len(res.json) == 1

        dataset = next(a for a in res.json if a["uuid"] == test_entities["dataset"]["uuid"])
        origin_samples = dataset.pop("origin_samples")
        assert len(origin_samples) == 1
        assert not any(k.startswith("lab_") for k in origin_samples[0].keys())  # no lab id fields
        assert not any(k.startswith("lab_") for k in dataset.keys())  # no lab id fields


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


def test_get_datasets_descendants_with_filters(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_dataset['uuid']}",
            json=filters,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )

        assert res.status_code == 200
        assert res.json == []


def test_get_datasets_descendants_with_filters_public_no_auth(db_session, app, requests):
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
        res = client.post(
            f"/descendants/{test_dataset['uuid']}",
            json=filters,
        )

        assert res.status_code == 200
        assert res.json == []

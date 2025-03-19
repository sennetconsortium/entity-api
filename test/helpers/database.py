import random
import string
import time
import uuid
from test.helpers import GROUP, USER

import pytest
from neo4j import GraphDatabase


def wait_for_neo4j(uri, user, password, timeout=60):
    """Wait for Neo4j to be ready

    Parameters
    ----------
    uri : str
        URI of the Neo4j server
    user : str
        Username to connect to the Neo4j server
    password : str
        Password to connect to the Neo4j server
    timeout : int
        Timeout in seconds (default is 60 seconds)

    Returns
    -------
    driver : neo4j.GraphDatabase.driver
        Neo4j driver object
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    start_time = time.time()
    while True:
        try:
            with driver.session() as session:
                session.run("RETURN 1")
            print("Neo4j is up and running")
            break
        except Exception as e:
            if time.time() - start_time > timeout:
                print("Timeout waiting for Neo4j to be ready")
                raise e
            print("Waiting for Neo4j to be ready...")
            time.sleep(1)

    return driver


@pytest.fixture(scope="session")
def db_session():
    """Test fixture to create a Neo4j session

    Returns
    -------
    session : neo4j.Session
        Neo4j session object. Fixture takes care of closing the session.
    """
    neo4j_uri = "bolt://localhost:7687"
    neo4j_username = "neo4j"
    neo4j_password = None

    driver = wait_for_neo4j(neo4j_uri, neo4j_username, neo4j_password)
    session = driver.session()
    create_lab(session)
    yield session
    session.close()
    driver.close()


def create_lab(db_session):
    """Create a lab node in the Neo4j database

    Parameters
    ----------
    db_session : neo4j.Session
        Neo4j session object from the `db_session` fixture

    Returns
    -------
    lab : dict
        Lab node properties
    """
    lab = {
        "entity_type": "Lab",
        "last_modified_timestamp": 1661717122681,
        "displayname": GROUP["displayname"],
        "created_timestamp": 1661717122681,
        "label": GROUP["name"],
        "uuid": GROUP["uuid"],
    }

    # Create a lab node if it doesn't exist
    query = "MATCH (l:Lab {uuid: $uuid}) RETURN l"
    result = db_session.run(query, uuid=lab["uuid"])
    if result.single() is None:
        query = """
        CREATE (l:Lab {
            entity_type: $entity_type,
            last_modified_timestamp: $last_modified_timestamp,
            displayname: $displayname,
            created_timestamp: $created_timestamp,
            label: $label,
            uuid: $uuid
        })
        """
        db_session.run(query, **lab)

    yield lab


def generate_entity():
    snt_first = random.randint(100, 999)
    snt_second = "".join(random.choices(string.ascii_uppercase, k=4))
    snt_third = random.randint(100, 999)
    sennet_id = f"SNT{snt_first}.{snt_second}.{snt_third}"

    return {
        "uuid": str(uuid.uuid4()).replace("-", ""),
        "sennet_id": sennet_id,
        "base_id": sennet_id.replace("SNT", "").replace(".", ""),
    }


def get_entity(uuid, db_session):
    query = "MATCH (e:Entity {uuid: $uuid}) RETURN e"
    result = db_session.run(query, uuid=uuid)
    return result.single()["e"]


def get_activity_for_entity(uuid, db_session):
    query = "MATCH (:Entity {uuid: $uuid})-[:WAS_GENERATED_BY]->(a:Activity) RETURN a"
    result = db_session.run(query, uuid=uuid)
    return result.single()["a"]


def create_provenance(db_session, provenance):
    created_entities = {}

    previous_uuid = None
    timestamp = int(time.time() * 1000)

    def gen_activity(_act):
        return {
            "uuid": _act["uuid"],
            "sennet_id": _act["sennet_id"],
            "created_by_user_displayname": USER["name"],
            "created_by_user_email": USER["email"],
            "created_by_user_sub": USER["sub"],
            "created_timestamp": timestamp,
            "creation_action": f"Create {entity_type.title()} Activity",
            "ended_at_time": timestamp,
            "protocol_url": "dx.doi.org/tests",
            "started_at_time": timestamp,
        }

    for item in provenance:
        if isinstance(item, dict):
            if "entity_type" in item or "sample_category" in item:
                raise ValueError("entity_type and sample_category are not allowed in provenance items. Use type instead.")
            entity_type = item.pop("type")
        elif isinstance(item, str):
            entity_type = item
        else:
            raise ValueError("Invalid provenance item")

        entity_type = entity_type.lower()
        entity = generate_entity()
        data = {
            "uuid": entity["uuid"],
            "sennet_id": entity["sennet_id"],
            "created_by_user_displayname": USER["name"],
            "created_by_user_email": USER["email"],
            "created_by_user_sub": USER["sub"],
            "created_timestamp": timestamp,
            "data_access_level": "consortium",
            "group_uuid": GROUP["uuid"],
            "group_name": GROUP["displayname"],
            "last_modified_timestamp": timestamp,
            "last_modified_user_displayname": USER["name"],
            "last_modified_user_email": USER["email"],
            "last_modified_user_sub": USER["sub"],
        }

        if entity_type == "source":
            data.update(
                {
                    "description": "Test source description.",
                    "entity_type": "Source",
                    "lab_source_id": "test_label_source_id",
                    "source_type": "Human",
                }
            )
        elif entity_type == "organ":
            data.update(
                {
                    "description": "Test organ description.",
                    "entity_type": "Sample",
                    "lab_tissue_sample_id": "test_label_organ_sample_id",
                    "organ": "LI",
                    "sample_category": "Organ",
                }
            )
        elif entity_type == "block":
            data.update(
                {
                    "description": "Test block description.",
                    "entity_type": "Sample",
                    "lab_tissue_sample_id": "test_label_block_sample_id",
                    "sample_category": "Block",
                }
            )
        elif entity_type == "section":
            data.update(
                {
                    "description": "Test sample description.",
                    "entity_type": "Sample",
                    "lab_tissue_sample_id": "test_label_section_sample_id",
                    "sample_category": "Section",
                }
            )
        elif entity_type == "dataset":
            data.update(
                {
                    "contains_human_genetic_sequences": False,
                    "data_types": "['Visium']",
                    "dataset_type": "Visium (no probes)",
                    "entity_type": "Dataset",
                    "lab_dataset_id": "test_lab_dataset_id",
                    "method": "Test dataset method.",
                    "purpose": "Test dataset purpose.",
                    "result": "Test dataset result.",
                    "status": "New",
                }
            )
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")

        if isinstance(item, dict):
            data.update(item)

        gen_by_activity = generate_entity()
        gen_by_activity_data = gen_activity(gen_by_activity)

        # Create the activity
        db_session.run(
            f"CREATE (:Activity {{ {', '.join(f'{k}: ${k}' for k in gen_by_activity_data)} }})",
            **gen_by_activity_data,
        )

        if previous_uuid is None:
            # Create the source
            db_session.run(
                f"CREATE (:Entity:{data['entity_type']} {{ {', '.join(f'{k}: ${k}' for k in data)} }})",
                **data,
            )
            # Connect directly to lab, this is a source
            db_session.run(
                "MATCH (l:Lab {uuid: $lab_uuid}), (e:Source {uuid: $source_uuid}) MERGE (l)<-[:WAS_ATTRIBUTED_TO]-(e)",
                lab_uuid=GROUP["uuid"],
                source_uuid=entity["uuid"],
            )
            # Connect the activity to the source
            db_session.run(
                "MATCH (a:Activity {uuid: $activity_uuid}), (e:Entity {uuid: $entity_uuid}) MERGE (a)<-[:WAS_GENERATED_BY]-(e)",
                activity_uuid=gen_by_activity["uuid"],
                entity_uuid=entity["uuid"],
            )

        else:
            # Create the entity
            db_session.run(
                f"CREATE (:Entity:{data['entity_type']} {{ {', '.join(f'{k}: ${k}' for k in data)} }})",
                **data,
            )
            # Connect the new entity and activity
            db_session.run(
                "MATCH (a:Activity {uuid: $activity_uuid}), (e:Entity {uuid: $entity_uuid}) MERGE (a)<-[:WAS_GENERATED_BY]-(e)",
                activity_uuid=gen_by_activity["uuid"],
                entity_uuid=entity["uuid"],
            )
            # Connect the previous entity and the activity
            db_session.run(
                "MATCH (a:Activity {uuid: $activity_uuid}), (e:Entity {uuid: $previous_uuid}) MERGE (e)<-[:USED]-(a)",
                activity_uuid=gen_by_activity["uuid"],
                previous_uuid=previous_uuid,
            )

        previous_uuid = entity["uuid"]

        entity_data = {**data, "base_id": entity["base_id"]}
        if entity_type in created_entities:
            if isinstance(created_entities[entity_type], list):
                created_entities[entity_type].append(entity_data)
            else:
                created_entities[entity_type] = [created_entities[entity_type], entity_data]
        else:
            created_entities[entity_type] = entity_data

    return created_entities

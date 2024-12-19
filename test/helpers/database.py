import time

import pytest
from neo4j import GraphDatabase

from . import GROUP


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
    yield session
    session.close()
    driver.close()


@pytest.fixture(scope="session")
def lab(db_session):
    """Test fixture to create a lab node in the Neo4j database

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

import logging

from atlas_consortia_commons.object import enum_val
from atlas_consortia_commons.string import equals
from neo4j.exceptions import TransactionError

from lib.ontology import Ontology
from schema import schema_neo4j_queries

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Directly called by app.py
####################################################################################################

"""
Check neo4j connectivity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool

Returns
-------
bool
    True if is connected, otherwise error
"""


def check_connection(neo4j_driver):
    query = (f"RETURN 1 AS {record_field_name}")

    # Sessions will often be created and destroyed using a with block context
    with neo4j_driver.session() as session:
        # Returned type is a Record object
        record = session.read_transaction(_execute_readonly_tx, query)

        # When record[record_field_name] is not None (namely the cypher result is not null)
        # and the value equals 1
        if record and record[record_field_name] and (record[record_field_name] == 1):
            logger.info("Neo4j is connected :)")
            return True

    logger.info("Neo4j is NOT connected :(")

    return False


"""
Get the activity connected to the given entity by the relationship WAS_GENERATED_BY

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of the entity connected to the requested actitivty

Returns
-------
dict
    A dictionary of activity details returned from the Cypher query
"""


def get_activity_was_generated_by(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)"
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN a AS {record_field_name}")

    logger.debug("======get_activity() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result


"""
Get target activity dict

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target activity

Returns
-------
dict
    A dictionary of activity details returned from the Cypher query
"""


def get_activity(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (e:Activity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN e AS {record_field_name}")

    logger.debug("======get_activity() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result


"""
Get the protocol_url from a related activity node

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of entity that connects to the activity that contains the protocol_url

Returns
-------
str
    The protocol_url property
"""


def get_activity_protocol(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)"
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN a.protocol_url AS protocol_url")

    logger.debug("======get_activity() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record is not None:
            if record[0] is not None:
                if record:
                    result = record[0]

    return result


"""
Get target entity dict

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity

Returns
-------
dict
    A dictionary of entity details returned from the Cypher query
"""


def get_entity(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN e AS {record_field_name}")

    logger.info("======get_entity() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

            protocol_url = get_activity_protocol(neo4j_driver, result['uuid'])
            if protocol_url != {}:
                result['protocol_url'] = protocol_url

    return result


def get_entity_by_id(neo4j_driver, uuid, property_keys=None):
    """
    Get target entity dict given the entity uuid

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    property_keys : Union[List[str], None]
        Properies to return in the result. Use None to return all properties. Default is None.

    Returns
    -------
    Union[dict, None]
        A dictionary of entity details returned from the Cypher query. None if the entity does not exist.
    """
    return_statement = 'e'
    if property_keys is not None:
        joined_props = ', '.join([f'{key}: e.{key}' for key in property_keys])
        return_statement = f'{{ {joined_props} }}'

    query = f"MATCH (e:Entity) WHERE e.uuid=$uuid RETURN {return_statement} AS {record_field_name}"

    logger.info("======get_entity_by_id() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            return dict(record[record_field_name])

    return None


"""
Get all the entity nodes for the given entity type

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of entity dicts of the given type returned from the Cypher query
"""


def get_entities_by_type(neo4j_driver, entity_type, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:{entity_type}) "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:{entity_type}) "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_entities_by_type() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    if not property_key:
        for result in results:
            protocol_url = get_activity_protocol(neo4j_driver, result['uuid'])
            if protocol_url != {}:
                result['protocol_url'] = protocol_url

    return results


"""
Get specific fields for Data Sharing Portal job dashboard for provided entities

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuids : list
    A list of entity UUIDs
entity_type : str
    One of the normalized entity types: Sample or Source

Returns
-------
list
    A dictionary of UUID to list of the following properties: sennet_id, uuid, lab_tissue_sample_id, source_type,
    sample_category, organ
"""


def get_entities_for_dashboard(neo4j_driver, entity_uuids, entity_type):
    results = []
    query = ''

    if entity_type.upper() == Ontology.ops().entities().SAMPLE.upper():
        query = (f"Match (e:Sample) "
                 f"WHERE e.uuid in {entity_uuids} "
                 f"OPTIONAL MATCH (e:Sample)-[*]->(o:Sample {{sample_category: 'Organ'}}) "
                 f"return apoc.coll.toSet(COLLECT({{sennet_id: e.sennet_id, uuid: e.uuid, "
                 f"lab_tissue_sample_id: e.lab_tissue_sample_id, sample_category: e.sample_category,"
                 f"organ_type: COALESCE(o.organ, e.organ), group_name: e.group_name}})) as {record_field_name}")

    if entity_type.upper() == Ontology.ops().entities().SOURCE.upper():
        query = (f"Match (e:Source) "
                 f"WHERE e.uuid in {entity_uuids} "
                 f"return apoc.coll.toSet(COLLECT({{sennet_id: e.sennet_id, uuid: e.uuid, "
                 f"lab_source_id: e.lab_source_id, source_type: e.source_type, group_name: e.group_name}})) as {record_field_name}")

    logger.info("======get_entities_for_dashboard() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)
        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def dataset_has_component_children(neo4j_driver, dataset_uuid):
    """
    Determine if given dataset has component children

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    dataset_uuid : str
        The uuid of the given dataset

    Returns
    -------
    bool
    """
    query = ("MATCH p=(ds1:Dataset)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(ds2:Dataset) "
             "WHERE ds2.uuid = $dataset_uuid AND a.creation_action = 'Multi-Assay Split' "
             "RETURN (COUNT(p) > 0)")
    with neo4j_driver.session() as session:
        value = session.run(query, dataset_uuid=dataset_uuid).value()
    return value[0]


"""
Retrieve the ancestor organ(s) of a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity

Returns
-------
list
    A list of organs that are ancestors of the given entity returned from the Cypher query
"""


def get_ancestor_organs(neo4j_driver, entity_uuid):
    results = []

    query = (f"MATCH (e:Entity {{uuid:'{entity_uuid}'}})-[*]->(organ:Sample {{sample_category:'{Ontology.ops().specimen_categories().ORGAN}'}}) "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"RETURN apoc.coll.toSet(COLLECT(organ)) AS {record_field_name}")

    logger.info("======get_ancestor_organs() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Create a new entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
entity_data_dict : dict
    The target Entity node to be created
superclass : str
    The normalized entity superclass type if defined, None by default

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""


def create_entity(neo4j_driver, entity_type, entity_data_dict, superclass=None):
    # Always define the Entity label in addition to the target `entity_type` label
    labels = f':Entity:{entity_type}'

    if superclass is not None:
        labels = f':Entity:{entity_type}:{superclass}'

    node_properties_map = _build_properties_map(entity_data_dict)

    query = (
        f"CREATE (e{labels}) "
        f"SET e = {node_properties_map} "
        f"RETURN e AS {record_field_name}")

    logger.info("======create_entity() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            entity_dict = _node_to_dict(entity_node)

            # logger.info("======create_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            tx.commit()

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit create_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Create multiple sample nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
samples_dict_list : list
    A list of dicts containing the generated data of each sample to be created
activity_dict : dict
    The dict containing generated activity data
direct_ancestor_uuid : str
    The uuid of the direct ancestor to be linked to
"""


def create_multiple_samples(neo4j_driver, samples_dict_list, activity_data_dict, direct_ancestor_uuid):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            activity_uuid = activity_data_dict['uuid']

            # Step 1: create the Activity node
            _create_activity_tx(tx, activity_data_dict)

            # Step 2: create relationship from source entity node to this Activity node
            _create_relationship_tx(tx, direct_ancestor_uuid, activity_uuid, 'USED', '<-')

            # Step 3: create each new sample node and link to the Activity node at the same time
            for sample_dict in samples_dict_list:
                node_properties_map = _build_properties_map(sample_dict)

                query = (f"MATCH (a:Activity) "
                         f"WHERE a.uuid = '{activity_uuid}' "
                         # Always define the Entity label in addition to the target `entity_type` label
                         f"CREATE (e:Entity:Sample {node_properties_map} ) "
                         f"CREATE (e)-[:WAS_GENERATED_BY]->(a)")

                logger.info("======create_multiple_samples() individual query======")
                logger.info(query)

                tx.run(query)

            # Then
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling create_multiple_samples(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit create_multiple_samples() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
entity_data_dict : dict
    The target entity with properties to be updated
uuid : str
    The uuid of target entity

Returns
-------
dict
    A dictionary of updated entity details returned from the Cypher query
"""


def update_entity(neo4j_driver, entity_type, entity_data_dict, uuid):
    node_properties_map = _build_properties_map(entity_data_dict)

    query = (f"MATCH (e:{entity_type}) "
             f"WHERE e.uuid = '{uuid}' "
             f"SET e += {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======update_entity() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            tx.commit()

            entity_dict = _node_to_dict(entity_node)

            # logger.info("======update_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit update_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


def get_ancestors(neo4j_driver, uuid, data_access_level=None, property_key=None, properties = [], is_include_action = False):
    """Get all ancestors by uuid.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    data_access_level : Optional[str]
        The data access level of the ancestor entities (public or consortium). None returns all ancestors.
    property_key : str
        A target property key for result filtering

    Returns
    -------
    list
        A list of unique ancestor dictionaries returned from the Cypher query
    """
    results = []

    predicate = ''
    if data_access_level:
        predicate = f"AND ancestor.data_access_level = '{data_access_level}' "

    if len(properties) > 0:
        action = 'NOT'
        if is_include_action is True:
            action = ''
        query = (f"MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(a:Entity) "
                 f"WHERE e.uuid = '{uuid}' AND a.entity_type <> 'Lab' {predicate} "
                 "WITH keys(a) as k1, a unwind k1 as k2 "
                 f"WITH a, k2 where {action} k2 IN {properties} "
                 f"WITH a, apoc.map.fromPairs([[k2, a[k2]], ['uuid', a.uuid]]) AS dict "
                 f"WITH collect(dict) as list with apoc.map.groupByMulti(list, 'uuid') AS groups "
                 f"WITH groups unwind keys(groups) as uuids "
                 f"WITH apoc.map.mergeList(groups[uuids]) AS list "
                 f"RETURN collect(list) AS {record_field_name}")

        # query = (f"MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(ancestor:Entity) "
        #          # Filter out the Lab entities
        #          f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' {predicate}"
        #          # COLLECT() returns a list
        #          # apoc.coll.toSet() reruns a set containing unique nodes
        #          f"RETURN apoc.coll.toSet(COLLECT(ancestor.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(ancestor:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' {predicate}"
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

    logger.info("======get_ancestors() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            if len(properties) > 0:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

                for result in results:
                    protocol_url = get_activity_protocol(neo4j_driver, result['uuid'])
                    if protocol_url != {}:
                        result['protocol_url'] = protocol_url

    return results


def get_descendants(neo4j_driver, uuid, data_access_level=None, property_key=None, entity_type=None):
    """ Get all descendants by uuid

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    data_access_level : Optional[str]
        The data access level of the descendant entities (public or consortium). None returns all descendants.
    property_key : str
        A target property key for result filtering

    Returns
    -------
    dict
        A list of unique desendant dictionaries returned from the Cypher query
    """
    results = []

    predicate = ''
    if data_access_level:
        predicate = f"AND descendant.data_access_level = '{data_access_level}' "

    if property_key:
        query = (f"MATCH (e:Entity)<-[:USED|WAS_GENERATED_BY*]-(descendant:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid=$uuid AND e.entity_type <> 'Lab' {predicate}"
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:USED|WAS_GENERATED_BY*]-(descendant:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid=$uuid AND e.entity_type <> 'Lab' {predicate}"
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

    logger.info("======get_descendants() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

                # If asked for the descendants of a Dataset then sort by last_modified_timestamp and place the published dataset at the top
                if equals(entity_type,  Ontology.ops().entities().DATASET):
                    results = sorted(results, key=lambda d: d['last_modified_timestamp'], reverse=True)

                    published_processed_dataset_location = next(
                        (i for i, item in enumerate(results) if item["status"] == "Published"), None)
                    if published_processed_dataset_location and published_processed_dataset_location != 0:
                        published_processed_dataset = results.pop(published_processed_dataset_location)
                        results.insert(0, published_processed_dataset)

                for result in results:
                    protocol_url = get_activity_protocol(neo4j_driver, result['uuid'])
                    if protocol_url != {}:
                        result['protocol_url'] = protocol_url

    return results


def get_descendant_datasets(neo4j_driver, uuid, property_key=None):
    """Get all descendant datasets for a given entity.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    property_key : str
        A target property key for result filtering

    Returns
    -------
    dict
        A list of unique descendant datasets returned from the Cypher query
    """
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:USED|WAS_GENERATED_BY*]-(descendant:Dataset) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid=$uuid AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:USED|WAS_GENERATED_BY*]-(descendant:Dataset) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid=$uuid AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

    logger.info("======get_descendant_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

                for result in results:
                    protocol_url = get_activity_protocol(neo4j_driver, result['uuid'])
                    if protocol_url != {}:
                        result['protocol_url'] = protocol_url

    return results


def get_ancestors_by_type(neo4j_driver, uuid, ancestor_type, property_keys=None):
    """Get all ancestors of a specific type for a given entity.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    ancestor_type : str
        The target entity type or sample category (Samples). This should be a value of one of the following:
        - Ontology.ops().entities()
        - Ontology.ops().specimen_categories()
    property_key : str
        Properies to return in the result. Use None to return all properties. Default is None.

    Returns
    -------
    list[dict]
        The list of ancestor entities as a dictionary
    """
    if ancestor_type in Ontology.ops(as_arr=True, cb=enum_val).entities():
        predicate = f"a.entity_type='{ancestor_type}'"
    elif ancestor_type in Ontology.ops(as_arr=True, cb=enum_val).specimen_categories():
        predicate = f"a.sample_category='{ancestor_type}'"
    else:
        raise ValueError(f'Unsupported entity type: {ancestor_type}')

    return_statement = 'COLLECT(a)'
    if property_keys is not None:
        joined_props = ', '.join([f'{key}: a.{key}' for key in property_keys])
        return_statement = f'COLLECT({{ {joined_props} }})'

    query = ("MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(a:Entity) "
             f"WHERE e.uuid=$uuid AND {predicate} "
             f"RETURN {return_statement} AS {record_field_name}")

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            return list(record[record_field_name])

    return []


def get_descendants_by_type(neo4j_driver, uuid, descendant_type, property_keys=None):
    """Get all descendants of a specific type for a given entity.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    descendant_type : str
        The target entity type or sample category (Samples). This should be a value of one of the following:
        - Ontology.ops().entities()
        - Ontology.ops().specimen_categories()
    property_key : str
        Properies to return in the result. Use None to return all properties. Default is None.

    Returns
    -------
    list[dict]
        The list of descendant entities as a dictionary
    """
    if descendant_type in Ontology.ops(as_arr=True, cb=enum_val).entities():
        predicate = f"AND d.entity_type='{descendant_type}'"
    elif descendant_type in Ontology.ops(as_arr=True, cb=enum_val).specimen_categories():
        predicate = f"AND d.sample_category='{descendant_type}'"
    else:
        raise ValueError(f'Unsupported entity type: {descendant_type}')

    return_statement = 'COLLECT(d)'
    if property_keys is not None:
        joined_props = ', '.join([f'{key}: d.{key}' for key in property_keys])
        return_statement = f'COLLECT({{ {joined_props} }})'

    query = ("MATCH (e:Entity)<-[:USED|WAS_GENERATED_BY*]-(d:Entity) "
             f"WHERE e.uuid=$uuid {predicate} "
             f"RETURN {return_statement} AS {record_field_name}")

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            return list(record[record_field_name])

    return []


def get_source_samples(neo4j_driver, uuid, property_keys=None):
    """Get all samples immediately connected to a given dataset.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    property_key : str
        Properies to return in the result. Use None to return all properties. Default is None.

    Returns
    -------
    list[dict]
        The list of source samples as a dictionary
    """
    return_statement = 'COLLECT(s)'
    if property_keys is not None:
        joined_props = ', '.join([f'{key}: s.{key}' for key in property_keys])
        return_statement = f'COLLECT({{ {joined_props} }})'

    query = (f"MATCH (d:Dataset)-[:WAS_GENERATED_BY|USED*]->(s:Sample) "
             f"WHERE d.uuid='{uuid}' "
             f"RETURN {return_statement} AS {record_field_name}")

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            return list(record[record_field_name])

    return []


"""
Get all parents by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique parent dictionaries returned from the Cypher query
"""


def get_parents(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(parent.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(parent)) AS {record_field_name}")

    logger.info("======get_parents() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get all children by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique child dictionaries returned from the Cypher query
"""


def get_children(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(child:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(child:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")

    logger.info("======get_children() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


def get_source_organ_count(neo4j_driver, uuid: str, organ: str, case_uuid: str = None):
    """
    Count the amount of a certain organ is attached to a particular Source.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    organ : str
        The organ to match against
    case_uuid : str
        An additional uuid to exclude from the count. Useful during updates.

    Returns
    -------
    int
        The result count
    """
    match_case = ''
    if case_uuid is not None:
        match_case = f"AND s.uuid <> '{case_uuid}' "

    query = f"MATCH (s:Sample)-[:WAS_GENERATED_BY]->(a)-[:USED]->(sr:Source) where sr.uuid='{uuid}' " \
            f"and s.organ='{organ}' {match_case}return count(s) AS {record_field_name}"

    logger.info("======get_source_organ_count() query======")
    logger.info(query)
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            return record[record_field_name]
        else:
            return 0


"""
Get all revisions for a given dataset uuid and sort them in descending order based on their creation time

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity

Returns
-------
dict
    A list of all the unique revision datasets in DESC order
"""


def get_sorted_revisions(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (prev:Dataset)<-[:REVISION_OF *0..]-(e:Dataset)<-[:REVISION_OF *0..]-(next:Dataset) "
             f"WHERE e.uuid='{uuid}' "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"WITH apoc.coll.toSet(COLLECT(next) + COLLECT(e) + COLLECT(prev)) AS collection "
             f"UNWIND collection as node "
             f"WITH node ORDER BY node.created_timestamp DESC "
             f"RETURN COLLECT(node) AS {record_field_name}")

    logger.info("======get_sorted_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the list of nodes to a list of dicts
            results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get all revisions for a given dataset uuid and sort them in descending order based on their creation time

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
fetch_all : bool
    Whether to fetch all Datasets or only include Published
property_key : str
    Return only a particular property from the cypher query, None for return all

Returns
-------
dict
    A multi-dimensional list [prev_revisions<list<list>>, next_revisions<list<list>>]
"""


def get_sorted_multi_revisions(neo4j_driver, uuid, fetch_all=True, property_key=False):
    results = []
    match_case = '' if fetch_all is True else 'AND prev.status = "Published" AND next.status = "Published" '
    collect_prop = f".{property_key}" if property_key else ''

    query = (
        "MATCH (e:Dataset), (next:Dataset), (prev:Dataset),"
        f"p = (e)-[:REVISION_OF *0..]->(prev),"
        f"n = (e)<-[:REVISION_OF *0..]-(next) "
        f"WHERE e.uuid='{uuid}' {match_case}"
        "WITH length(p) AS p_len, prev, length(n) AS n_len, next "
        "ORDER BY prev.created_timestamp, next.created_timestamp DESC "
        f"WITH p_len, collect(distinct prev{collect_prop}) AS prev_revisions, n_len, collect(distinct next{collect_prop}) AS next_revisions "
        f"RETURN [collect(distinct next_revisions), collect(distinct prev_revisions)] AS {record_field_name}"
    )

    logger.info("======get_sorted_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name] and len(record[record_field_name]) > 0:
            record[record_field_name][0].pop()  # the target will appear twice, pop it from the next list
            if property_key:
                return record[record_field_name]
            else:
                for collection in record[record_field_name]:  # two collections: next, prev
                    revs = []
                    for rev in collection:  # each collection list contains revision lists, so 2 dimensional array
                        # Convert the list of nodes to a list of dicts
                        nodes_to_dicts = _nodes_to_dicts(rev)
                        revs.append(nodes_to_dicts)

                    results.append(revs)

    return results


"""
Returns all of the Sample information associated with a Dataset, back to each Source. Returns a dictionary
containing all of the provenance info for a given dataset. Each Sample is in its own dictionary, converted
from its neo4j node and placed into a list.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : string
    the uuid of the desired dataset
"""
def get_all_dataset_samples(neo4j_driver, dataset_uuid):
    query = f"MATCH p = (ds:Dataset {{uuid: '{dataset_uuid}'}})-[*]->(s:Source) return p"
    logger.info("======get_all_dataset_samples() query======")
    logger.info(query)

    # Dictionary of Dictionaries, keyed by UUID, containing each Sample returned in the Neo4j Path
    dataset_sample_list = {}
    with neo4j_driver.session() as session:
        result = session.run(query)
        if result.peek() is None:
            return
        for record in result:
            for item in record:
                for node in item.nodes:
                    if node["entity_type"] == 'Sample':
                        if not node["uuid"] in dataset_sample_list:
                            dataset_sample_list[node["uuid"]] = {'sample_category': node["sample_category"]}
    return dataset_sample_list


"""
Get all previous revisions of the target entity by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A 2 dimensional list of unique previous revisions dictionaries returned from the Cypher query
"""


def get_previous_multi_revisions(neo4j_driver, uuid, property_key=None):
    results = []

    collect_prop = f".{property_key}" if property_key else ''
    query = (f"MATCH p=(e:Entity)-[:REVISION_OF*]->(prev:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"WITH length(p) as p_len, collect(distinct prev{collect_prop}) as prev_revisions "
             f"RETURN collect(distinct prev_revisions) AS {record_field_name}")

    logger.info("======get_previous_multi_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                for rev in record[record_field_name]:
                    results.append(_nodes_to_dicts(rev))

    return results


"""
Get all next revisions of the target entity by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A 2 dimensional list of unique next revisions dictionaries returned from the Cypher query
"""


def get_next_multi_revisions(neo4j_driver, uuid, property_key=None):
    results = []

    collect_prop = f".{property_key}" if property_key else ''
    query = (f"MATCH n=(e:Entity)<-[:REVISION_OF*]-(next:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"WITH length(n) as n_len, collect(distinct next{collect_prop}) as next_revisions "
             f"RETURN collect(distinct next_revisions) AS {record_field_name}")

    logger.info("======get_next_multi_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                for rev in record[record_field_name]:
                    results.append(_nodes_to_dicts(rev))

    return results


"""
Get all previous revisions of the target entity by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique previous revisions dictionaries returned from the Cypher query
"""


def get_previous_revisions(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:REVISION_OF*]->(prev:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(prev.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:REVISION_OF*]->(prev:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(prev)) AS {record_field_name}")

    logger.info("======get_previous_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get all next revisions of the target entity by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique next revisions dictionaries returned from the Cypher query
"""


def get_next_revisions(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:REVISION_OF*]-(next:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(next.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:REVISION_OF*]-(next:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(next)) AS {record_field_name}")

    logger.info("======get_next_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Link the entities to the target collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of target collection
entitiy_uuids_list : list
    A list of entity uuids to be linked to collection
"""


def add_entities_to_collection(neo4j_driver, collection_uuid, entitiy_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in entitiy_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    entitiy_uuids_list = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Collection and the given Entities")

            query = (f"MATCH (c:Collection), (e:Entity) "
                     f"WHERE c.uuid = '{collection_uuid}' AND e.uuid IN {entitiy_uuids_list} "
                     # Use MERGE instead of CREATE to avoid creating the relationship multiple times
                     # MERGE creates the relationship only if there is no existing relationship
                     f"MERGE (c)<-[r:IN_COLLECTION]-(e)")

            logger.info("======add_entities_to_collection() query======")
            logger.info(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling add_entities_to_collection(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit add_entities_to_collection() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Retrive the full tree above the given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity: Source/Sample/Dataset, not Collection
depth : int
    The maximum number of hops in the traversal
"""


def get_provenance(neo4j_driver, uuid, depth, return_descendants=None, query_filter=None):
    # max_level_str is the string used to put a limit on the number of levels to traverse
    max_level_str = ''
    if depth is not None and len(str(depth)) > 0:
        max_level_str = f"maxLevel: {depth}, "

    relationship_filter = 'USED>|WAS_GENERATED_BY>'
    if return_descendants:
        relationship_filter = '<USED|<WAS_GENERATED_BY'

    label_filter = ''
    if query_filter is not None and len(query_filter) > 0:
        label_filter = f", labelFilter:'{query_filter}'"

    # More info on apoc.path.subgraphAll() procedure: https://neo4j.com/labs/apoc/4.0/graph-querying/expand-subgraph/
    query = (f"MATCH (n:Entity) "
             f"WHERE n.uuid = '{uuid}' "
             f"CALL apoc.path.subgraphAll(n, {{ {max_level_str} relationshipFilter:'{relationship_filter}' {label_filter} }}) "
             f"YIELD nodes, relationships "
             f"WITH [node in nodes | node {{ .*, label:labels(node)[0] }} ] as nodes, "
             f"[rel in relationships | rel {{ .*, fromNode: {{ label:labels(startNode(rel))[0], uuid:startNode(rel).uuid }}, toNode: {{ label:labels(endNode(rel))[0], uuid:endNode(rel).uuid }}, rel_data: {{ type: type(rel) }} }} ] as rels "
             f"WITH {{ nodes:nodes, relationships:rels }} as json "
             f"RETURN json")

    logger.info("======get_provenance() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        return session.read_transaction(_execute_readonly_tx, query)


"""
Retrive the latest revision dataset of the given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target dataset
public : bool
    If get back the latest public revision dataset or the real one
"""


def get_dataset_latest_revision(neo4j_driver, uuid, public=False):
    # Defaut the latest revision to this entity itself
    result = get_entity(neo4j_driver, uuid)

    if public:
        # Don't use [r:REVISION_OF] because
        # Binding a variable length relationship pattern to a variable ('r') is deprecated
        query = (f"MATCH (e:Dataset)<-[:REVISION_OF*]-(next:Dataset) "
                 f"WHERE e.uuid='{uuid}' AND next.status='Published' "
                 f"WITH LAST(COLLECT(next)) as latest "
                 f"RETURN latest AS {record_field_name}")
    else:
        query = (f"MATCH (e:Dataset)<-[:REVISION_OF*]-(next:Dataset) "
                 f"WHERE e.uuid='{uuid}' "
                 f"WITH LAST(COLLECT(next)) as latest "
                 f"RETURN latest AS {record_field_name}")

    logger.info("======get_dataset_latest_revision() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        # Only convert when record[record_field_name] is not None (namely the cypher result is not null)
        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result


"""
Retrive the calculated revision number of the given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target dataset
"""


def get_dataset_revision_number(neo4j_driver, uuid):
    revision_number = 1

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Dataset)-[:REVISION_OF*]->(prev:Dataset) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN COUNT(prev) AS {record_field_name}")

    logger.info("======get_dataset_revision_number() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # The revision number is the count of previous revisions plus 1
            revision_number = record[record_field_name] + 1

    return revision_number


"""
Retrieve the list of uuids for organs associated with a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of the target entity: Dataset
"""


def get_associated_organs_from_dataset(neo4j_driver, dataset_uuid):
    results = []
    query = (f"MATCH (ds:Dataset)-[*]->(organ:Sample {{sample_category:'{Ontology.ops().specimen_categories().ORGAN}'}}) "
             f"WHERE ds.uuid='{dataset_uuid}'"
             f"RETURN apoc.coll.toSet(COLLECT(organ)) AS {record_field_name}")

    logger.info("======get_associated_organs_from_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = _nodes_to_dicts(record[record_field_name])

    return results


""" Retrieve the list of samples associated with a given dataset

Args:
    neo4j_driver (neo4j.Driver):
        The neo4j database connection pool
    dataset_uuid (str):
        The uuid of the target entity: Dataset

Returns:
    list: A list of dictionaries containing the sample information
"""


def get_associated_samples_from_dataset(neo4j_driver, dataset_uuid):
    results = []

    # specimen_type -> sample_category 12/15/2022
    query = (f"MATCH (ds:Dataset)-[*]->(sample:Sample) "
             f"WHERE ds.uuid='{dataset_uuid}' AND NOT sample.sample_category = 'organ' "
             f"RETURN apoc.coll.toSet(COLLECT(sample)) AS {record_field_name}")

    logger.info("======get_associated_samples_from_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(schema_neo4j_queries.execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = schema_neo4j_queries.nodes_to_dicts(record[record_field_name])

    return results


""" Retrieve the list of sources associated with a given dataset

Args:
    neo4j_driver (neo4j.Driver):
        The neo4j database connection pool
    dataset_uuid (str):
        The uuid of the target entity: Dataset

Returns:
    list: A list of dictionaries containing the source information
"""


def get_associated_sources_from_dataset(neo4j_driver, dataset_uuid):
    results = []

    # specimen_type -> sample_category 12/15/2022
    query = (f"MATCH (ds:Dataset)-[*]->(source:Source) "
             f"WHERE ds.uuid='{dataset_uuid}'"
             f"RETURN apoc.coll.toSet(COLLECT(source)) AS {record_field_name}")

    logger.info("======get_associated_sources_from_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(schema_neo4j_queries.execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = schema_neo4j_queries.nodes_to_dicts(record[record_field_name])

    return results


"""
Retrieve all the provenance information about each dataset. Each dataset's prov-info is given by a dictionary.
Certain fields such as first sample where there can be multiple nearest datasets in the provenance above a given
dataset, that field is a list inside of its given dictionary. Results can be filtered with certain parameters:
has_rui_info (true or false), organ (organ type), group_uuid, and dataset_status. These are passed in as a dictionary if
they are present.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
param_dict : dictionary
    Dictionary containing any parameters desired to filter for certain results
published_only : boolean
    If a user does not have a token with SenNet-Read Group access, published_only is set to true. This will cause only
    datasets with status = 'Published' to be included in the result.
"""


def get_prov_info(neo4j_driver, param_dict, published_only):
    group_uuid_query_string = ''
    organ_query_string = 'OPTIONAL MATCH'
    organ_where_clause = ""
    rui_info_query_string = 'OPTIONAL MATCH (ds)-[*]->(ruiSample:Sample)'
    rui_info_where_clause = "WHERE NOT ruiSample.rui_location IS NULL AND NOT trim(ruiSample.rui_location) = '' "
    dataset_status_query_string = ''
    published_only_query_string = ''
    if 'group_uuid' in param_dict:
        group_uuid_query_string = f" AND toUpper(ds.group_uuid) = '{param_dict['group_uuid'].upper()}'"
    if Ontology.ops().specimen_categories().ORGAN in param_dict:
        organ_query_string = 'MATCH'
        # organ_where_clause = f", organ: '{param_dict['organ'].upper()}'"
        organ_where_clause = f" WHERE toUpper(organ.organ) = '{param_dict[Ontology.ops().specimen_categories().ORGAN].upper()}'"
    if 'has_rui_info' in param_dict:
        rui_info_query_string = 'MATCH (ds)-[*]->(ruiSample:Sample)'
        if param_dict['has_rui_info'].lower() == 'false':
            rui_info_query_string = 'MATCH (ds:Dataset)'
            rui_info_where_clause = "WHERE NOT EXISTS {MATCH (ds)-[*]->(ruiSample:Sample) WHERE NOT ruiSample.rui_location IS NULL AND NOT TRIM(ruiSample.rui_location) = ''} MATCH (ds)-[*]->(ruiSample:Sample)"
    if 'dataset_status' in param_dict:
        dataset_status_query_string = f" AND toUpper(ds.status) = '{param_dict['dataset_status'].upper()}'"
    if published_only:
        published_only_query_string = " AND toUpper(ds.status) = 'PUBLISHED'"
    query = (f"MATCH (ds:Dataset)-[:WAS_GENERATED_BY]->(a)-[:USED]->(firstSample:Sample)-[*]->(source:Source)"
             f"WHERE not (ds)<-[:REVISION_OF]-(:Dataset)"
             f" AND NOT toLower(a.creation_action) ENDS WITH 'process'"
             f"{group_uuid_query_string}"
             f"{dataset_status_query_string}"
             f"{published_only_query_string}"
             f" WITH ds, COLLECT(distinct source) AS SOURCE, COLLECT(distinct firstSample) AS FIRSTSAMPLE"
             f" OPTIONAL MATCH (ds)-[:REVISION_OF]->(rev:Dataset)"
             f" WITH ds, SOURCE, FIRSTSAMPLE, COLLECT(rev.sennet_id) as REVISIONS"
             f" OPTIONAL MATCH (ds)-[*]->(metaSample:Sample)"
             f" WHERE NOT metaSample.metadata IS NULL AND NOT TRIM(metaSample.metadata) = ''"
             f" WITH ds, FIRSTSAMPLE, SOURCE, REVISIONS, collect(distinct metaSample) as METASAMPLE"
             f" {rui_info_query_string}"
             f" {rui_info_where_clause}"
             f" WITH ds, FIRSTSAMPLE, SOURCE, REVISIONS, METASAMPLE, collect(distinct ruiSample) as RUISAMPLE"
             f" {organ_query_string} (source)<-[:USED]-(oa)<-[:WAS_GENERATED_BY]-(organ:Sample {{sample_category:'{Ontology.ops().specimen_categories().ORGAN}'}})<-[*]-(ds)"
             f" {organ_where_clause}"
             f" WITH ds, FIRSTSAMPLE, SOURCE, REVISIONS, METASAMPLE, RUISAMPLE, COLLECT(DISTINCT organ) AS ORGAN "
             f" OPTIONAL MATCH (ds)<-[:USED]-(a3)<-[:WAS_GENERATED_BY]-(processed_dataset:Dataset)"
             f" WHERE toLower(a3.creation_action) ENDS WITH 'process'"
             f" WITH ds, FIRSTSAMPLE, SOURCE, REVISIONS, METASAMPLE, RUISAMPLE, ORGAN, COLLECT(distinct processed_dataset) AS PROCESSED_DATASET"
             f" RETURN ds.uuid, FIRSTSAMPLE, SOURCE, RUISAMPLE, ORGAN, ds.sennet_id, ds.status, ds.group_name,"
             f" ds.group_uuid, ds.created_timestamp, ds.created_by_user_email, ds.last_modified_timestamp, "
             f" ds.last_modified_user_email, ds.lab_dataset_id, ds.dataset_type, METASAMPLE, PROCESSED_DATASET, REVISIONS")

    logger.info("======get_prov_info() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are non subscriptable. By putting then in a small list, we can address
            # Each item in a record.
            for item in record:
                record_contents.append(item)
            record_dict['uuid'] = record_contents[0]
            content_one = []
            for entry in record_contents[1]:
                node_dict = _node_to_dict(entry)
                content_one.append(node_dict)
            record_dict['first_sample'] = content_one
            content_two = []
            for entry in record_contents[2]:
                node_dict = _node_to_dict(entry)
                content_two.append(node_dict)
            record_dict['distinct_source'] = content_two
            content_three = []
            for entry in record_contents[3]:
                node_dict = _node_to_dict(entry)
                content_three.append(node_dict)
            record_dict['distinct_rui_sample'] = content_three
            content_four = []
            for entry in record_contents[4]:
                node_dict = _node_to_dict(entry)
                content_four.append(node_dict)
            record_dict['distinct_organ'] = content_four
            record_dict['sennet_id'] = record_contents[5]
            record_dict['status'] = record_contents[6]
            record_dict['group_name'] = record_contents[7]
            record_dict['group_uuid'] = record_contents[8]
            record_dict['created_timestamp'] = record_contents[9]
            record_dict['created_by_user_email'] = record_contents[10]
            record_dict['last_modified_timestamp'] = record_contents[11]
            record_dict['last_modified_user_email'] = record_contents[12]
            record_dict['lab_dataset_id'] = record_contents[13]
            record_dict['dataset_type'] = record_contents[14]
            content_fifteen = []
            for entry in record_contents[15]:
                node_dict = _node_to_dict(entry)
                content_fifteen.append(node_dict)
            record_dict['distinct_metasample'] = content_fifteen
            content_sixteen = []
            for entry in record_contents[16]:
                node_dict = _node_to_dict(entry)
                content_sixteen.append(node_dict)
            record_dict['processed_dataset'] = content_sixteen
            content_seventeen = []
            for entry in record_contents[17]:
                content_seventeen.append(entry)
            record_dict['previous_version_sennet_ids'] = content_seventeen
            list_of_dictionaries.append(record_dict)
    return list_of_dictionaries


"""
Returns all of the same information as get_prov_info however only for a single dataset at a time. Returns a dictionary
containing all of the provenance info for a given dataset. For fields such as first sample where there can be multiples,
they are presented in their own dictionary converted from their nodes in neo4j and placed into a list.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : string
    the uuid of the desired dataset
"""


def get_individual_prov_info(neo4j_driver, dataset_uuid):
    query = (
        f"MATCH (ds:Dataset {{uuid: '{dataset_uuid}'}})-[:WAS_GENERATED_BY]->(a)-[:USED]->(firstSample:Sample)-[*]->(source:Source)"
        f" WITH ds, COLLECT(distinct source) AS SOURCE, COLLECT(distinct firstSample) AS FIRSTSAMPLE"
        f" OPTIONAL MATCH (ds)-[*]->(metaSample:Sample)"
        f" WHERE NOT metaSample.metadata IS NULL AND NOT TRIM(metaSample.metadata) = ''"
        f" WITH ds, FIRSTSAMPLE, SOURCE, COLLECT(distinct metaSample) AS METASAMPLE"
        f" OPTIONAL MATCH (ds)-[*]->(ruiSample:Sample)"
        f" WHERE NOT ruiSample.rui_location IS NULL AND NOT TRIM(ruiSample.rui_location) = ''"
        f" WITH ds, FIRSTSAMPLE, SOURCE, METASAMPLE, COLLECT(distinct ruiSample) AS RUISAMPLE"
        f" OPTIONAL match (source)<-[:USED]-(oa)<-[:WAS_GENERATED_BY]-(organ:Sample {{sample_category:'{Ontology.ops().specimen_categories().ORGAN}'}})<-[*]-(ds)"
        f" WITH ds, FIRSTSAMPLE, SOURCE, METASAMPLE, RUISAMPLE, COLLECT(distinct organ) AS ORGAN "
        f" OPTIONAL MATCH (ds)<-[:USED]-(a3)<-[:WAS_GENERATED_BY]-(processed_dataset:Dataset)"
        f" WHERE toLower(a3.creation_action) ENDS WITH 'process'"
        f" WITH ds, FIRSTSAMPLE, SOURCE, METASAMPLE, RUISAMPLE, ORGAN, COLLECT(distinct processed_dataset) AS PROCESSED_DATASET"
        f" RETURN ds.uuid, FIRSTSAMPLE, SOURCE, RUISAMPLE, ORGAN, ds.sennet_id, ds.status, ds.group_name,"
        f" ds.group_uuid, ds.created_timestamp, ds.created_by_user_email, ds.last_modified_timestamp, "
        f" ds.last_modified_user_email, ds.lab_dataset_id, ds.dataset_type, METASAMPLE, PROCESSED_DATASET")

    logger.info("======get_prov_info() query======")
    logger.info(query)

    record_contents = []
    record_dict = {}
    with neo4j_driver.session() as session:
        result = session.run(query)
        if result.peek() is None:
            return
        for record in result:
            for item in record:
                record_contents.append(item)
            record_dict['uuid'] = record_contents[0]
            content_one = []
            for entry in record_contents[1]:
                node_dict = _node_to_dict(entry)
                content_one.append(node_dict)
            record_dict['first_sample'] = content_one
            content_two = []
            for entry in record_contents[2]:
                node_dict = _node_to_dict(entry)
                content_two.append(node_dict)
            record_dict['distinct_source'] = content_two
            content_three = []
            for entry in record_contents[3]:
                node_dict = _node_to_dict(entry)
                content_three.append(node_dict)
            record_dict['distinct_rui_sample'] = content_three
            content_four = []
            for entry in record_contents[4]:
                node_dict = _node_to_dict(entry)
                content_four.append(node_dict)
            record_dict['distinct_organ'] = content_four
            record_dict['sennet_id'] = record_contents[5]
            record_dict['status'] = record_contents[6]
            record_dict['group_name'] = record_contents[7]
            record_dict['group_uuid'] = record_contents[8]
            record_dict['created_timestamp'] = record_contents[9]
            record_dict['created_by_user_email'] = record_contents[10]
            record_dict['last_modified_timestamp'] = record_contents[11]
            record_dict['last_modified_user_email'] = record_contents[12]
            record_dict['lab_dataset_id'] = record_contents[13]
            record_dict['dataset_type'] = record_contents[14]
            content_fifteen = []
            for entry in record_contents[15]:
                node_dict = _node_to_dict(entry)
                content_fifteen.append(node_dict)
            record_dict['distinct_metasample'] = content_fifteen
            content_sixteen = []
            for entry in record_contents[16]:
                node_dict = _node_to_dict(entry)
                content_sixteen.append(node_dict)

            # Sort the derived datasets by status and last_modified_timestamp
            content_sixteen = sorted(content_sixteen, key=lambda d: d['last_modified_timestamp'], reverse=True)

            published_processed_dataset_location = next((i for i, item in enumerate(content_sixteen) if item["status"] == "Published"), None)
            if published_processed_dataset_location and published_processed_dataset_location != 0:
                published_processed_dataset = content_sixteen.pop(published_processed_dataset_location)
                content_sixteen.insert(0, published_processed_dataset)

            record_dict['processed_dataset'] = content_sixteen
    return record_dict


"""
Returns group_name, dataset_type, and status for every primary dataset. Also returns the organ type for the closest
sample above the dataset in the provenance where {sample_category: '{Ontology.ops().specimen_categories().ORGAN}'}.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
"""


def get_sankey_info(neo4j_driver):
    query = (f"MATCH (ds:Dataset)-[]->(a)-[]->(:Sample)"
             f"MATCH (source)<-[:USED]-(oa)<-[:WAS_GENERATED_BY]-(organ:Sample {{sample_category:'{Ontology.ops().specimen_categories().ORGAN}'}})<-[*]-(ds)"
             f"RETURN distinct ds.group_name, organ.organ, ds.dataset_type, ds.status, ds. uuid order by ds.group_name")
    logger.info("======get_sankey_info() query======")
    logger.info(query)
    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are non subscriptable. By putting then in a small list, we can address
            # Each item in a record.
            for item in record:
                record_contents.append(item)
            record_dict['dataset_group_name'] = record_contents[0]
            record_dict['organ_type'] = record_contents[1]
            record_dict['dataset_dataset_type'] = record_contents[2]
            record_dict['dataset_status'] = record_contents[3]
            list_of_dictionaries.append(record_dict)
        return list_of_dictionaries


"""
Returns sample uuid, sample rui location, sample metadata, sample group name, sample created_by_email, sample ancestor
uuid, sample ancestor entity type, organ uuid, organ type, lab tissue sample id, source uuid, source
metadata, sample_sennet_id, organ_sennet_id, source_sennet_id, and sample_type all in a dictionary

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
param_dict : dictionary
    dictionary containing any filters to be applied in the samples-prov-info query
"""


def get_sample_prov_info(neo4j_driver, param_dict):
    group_uuid_query_string = ''
    if 'group_uuid' in param_dict:
        group_uuid_query_string = f" WHERE toUpper(s.group_uuid) = '{param_dict['group_uuid'].upper()}'"
    query = (
        f" MATCH (s:Sample)-[*]->(d:Source)"
        f" {group_uuid_query_string}"
        f" WITH s, d"
        f" OPTIONAL MATCH (s)-[*]->(organ:Sample{{sample_category: '{Ontology.ops().specimen_categories().ORGAN}'}})"
        f" WITH s, organ, d"
        f" MATCH (s)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(da)"
        f" RETURN s.uuid, s.lab_tissue_sample_id, s.group_name, s.created_by_user_email, s.metadata, s.rui_location,"
        f" d.uuid, d.metadata, organ.uuid, organ.sample_category, organ.metadata, da.uuid, da.entity_type, "
        f"s.sample_category, organ.organ, s.organ, s.sennet_id, organ.sennet_id, "
        f"d.sennet_id"
    )

    logger.info("======get_sample_prov_info() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are not subscriptable. By putting them in a small list, we can address
            # each item in a record
            for item in record:
                record_contents.append(item)
            record_dict['sample_uuid'] = record_contents[0]
            record_dict['lab_sample_id'] = record_contents[1]
            record_dict['sample_group_name'] = record_contents[2]
            record_dict['sample_created_by_email'] = record_contents[3]
            record_dict['sample_metadata'] = record_contents[4]
            record_dict['sample_rui_info'] = record_contents[5]
            record_dict['source_uuid'] = record_contents[6]
            record_dict['source_metadata'] = record_contents[7]
            record_dict['organ_uuid'] = record_contents[8]
            record_dict['organ_type'] = record_contents[9]
            record_dict['organ_metadata'] = record_contents[10]
            record_dict['sample_ancestor_id'] = record_contents[11]
            record_dict['sample_ancestor_entity'] = record_contents[12]
            record_dict['sample_sample_category'] = record_contents[13]
            record_dict['organ_organ_type'] = record_contents[14]
            record_dict['sample_organ'] = record_contents[15]
            record_dict['sample_sennet_id'] = record_contents[16]
            record_dict['organ_sennet_id'] = record_contents[17]
            record_dict['source_sennet_id'] = record_contents[18]

            list_of_dictionaries.append(record_dict)
    return list_of_dictionaries


####################################################################################################
## Internal Functions
####################################################################################################

"""
Build the property key-value pairs to be used in the Cypher clause for node creation/update

Parameters
----------
entity_data_dict : dict
    The target Entity node to be created

Returns
-------
str
    A string representation of the node properties map containing
    key-value pairs to be used in Cypher clause
"""


def _build_properties_map(entity_data_dict):
    separator = ', '
    node_properties_list = []

    for key, value in entity_data_dict.items():
        if isinstance(value, (int, bool)):
            # Treat integer and boolean as is
            key_value_pair = f"{key}: {value}"
        elif isinstance(value, str):
            # Special case is the value is 'TIMESTAMP()' string
            # Remove the quotes since neo4j only takes TIMESTAMP() as a function
            if value == 'TIMESTAMP()':
                key_value_pair = f"{key}: {value}"
            else:
                # Escape single quote
                escaped_str = value.replace("'", r"\'")
                # Quote the value
                key_value_pair = f"{key}: '{escaped_str}'"
        else:
            # Convert list and dict to string
            # Must also escape single quotes in the string to build a valid Cypher query
            escaped_str = str(value).replace("'", r"\'")
            # Also need to quote the string value
            key_value_pair = f"{key}: '{escaped_str}'"

        # Add to the list
        node_properties_list.append(key_value_pair)

    # Example: {uuid: 'eab7fd6911029122d9bbd4d96116db9b', rui_location: 'Joe <info>', lab_tissue_sample_id: 'dadsadsd'}
    # Note: all the keys are not quoted, otherwise Cypher syntax error
    node_properties_map = f"{{ {separator.join(node_properties_list)} }}"

    return node_properties_map


"""
Execute a unit of work in a managed read transaction

Parameters
----------
tx : transaction_function
    a function that takes a transaction as an argument and does work with the transaction
query : str
    The target cypher query to run

Returns
-------
neo4j.Record or None
    A single record returned from the Cypher query
"""


def _execute_readonly_tx(tx, query, **kwargs):
    result = tx.run(query, **kwargs)
    record = result.single()
    return record


"""
Create a relationship from the source node to the target node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
source_node_uuid : str
    The uuid of source node
target_node_uuid : str
    The uuid of target node
relationship : str
    The relationship type to be created
direction: str
    The relationship direction from source node to target node: outgoing `->` or incoming `<-`
    Neo4j CQL CREATE command supports only directional relationships
"""


def _create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
    incoming = "-"
    outgoing = "-"

    if direction == "<-":
        incoming = direction

    if direction == "->":
        outgoing = direction

    query = ("MATCH (s), (t) "
             f"WHERE s.uuid = '{source_node_uuid}' AND t.uuid = '{target_node_uuid}' "
             f"CREATE (s){incoming}[r:{relationship}]{outgoing}(t) "
             f"RETURN type(r) AS {record_field_name}")

    logger.info("======_create_relationship_tx() query======")
    logger.info(query)

    tx.run(query)


"""
Convert the neo4j node into Python dict

Parameters
----------
entity_node : neo4j.node
    The target neo4j node to be converted

Returns
-------
dict
    A dictionary of target entity containing all property key/value pairs
"""


def _node_to_dict(entity_node):
    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict


"""
Convert the list of neo4j nodes into a list of Python dicts

Parameters
----------
nodes : list
    The list of neo4j node to be converted

Returns
-------
list
    A list of target entity dicts containing all property key/value pairs
"""


def _nodes_to_dicts(nodes):
    dicts = []

    for node in nodes:
        entity_dict = _node_to_dict(node)
        dicts.append(entity_dict)

    return dicts


"""
Create a new activity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
activity_data_dict : dict
    The dict containing properties of the Activity node to be created

Returns
-------
neo4j.node
    A neo4j node instance of the newly created entity node
"""


def _create_activity_tx(tx, activity_data_dict):
    node_properties_map = _build_properties_map(activity_data_dict)

    query = (f"CREATE (e:Activity) "
             f"SET e = {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======_create_activity_tx() query======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    return node


"""
Create multiple dataset nodes in neo4j
Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
datasets_dict_list : list
    A list of dicts containing the generated data of each sample to be created
activity_dict : dict
    The dict containing generated activity data
direct_ancestor_uuid : str
    The uuid of the direct ancestor to be linked to
"""
def create_multiple_datasets(neo4j_driver, datasets_dict_list, activity_data_dict, direct_ancestor_uuid):
    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            activity_uuid = activity_data_dict['uuid']

            # Step 1: create the Activity node
            _create_activity_tx(tx, activity_data_dict)

            # Step 2: create relationship from source entity node to this Activity node
            _create_relationship_tx(tx, direct_ancestor_uuid, activity_uuid, 'USED', '<-')

            # Step 3: create each new sample node and link to the Activity node at the same time
            output_dicts_list = []
            for dataset_dict in datasets_dict_list:
                # Remove dataset_link_abs_dir once more before entity creation
                dataset_link_abs_dir = dataset_dict.pop('dataset_link_abs_dir', None)
                node_properties_map = _build_properties_map(dataset_dict)

                query = (f"MATCH (a:Activity) "
                         f"WHERE a.uuid = '{activity_uuid}' "
                         # Always define the Entity label in addition to the target `entity_type` label
                         f"CREATE (e:Entity:Dataset {node_properties_map} ) "
                         f"CREATE (a)<-[:WAS_GENERATED_BY]-(e)"
                         f"RETURN e AS {record_field_name}")

                logger.info("======create_multiple_samples() individual query======")
                logger.info(query)

                result = tx.run(query)
                record = result.single()
                entity_node = record[record_field_name]
                entity_dict = _node_to_dict(entity_node)
                entity_dict['dataset_link_abs_dir'] = dataset_link_abs_dir
                output_dicts_list.append(entity_dict)
            # Then
            tx.commit()
            return output_dicts_list

    except TransactionError as te:
        msg = f"TransactionError from calling create_multiple_samples(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit create_multiple_samples() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Get all siblings by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique sibling dictionaries returned from the Cypher query
"""
def get_siblings(neo4j_driver, uuid, status, prop_key, include_revisions):
    sibling_uuids = schema_neo4j_queries.get_siblings(neo4j_driver, uuid, property_key='uuid')
    sibling_uuids_string = str(sibling_uuids)
    revision_query_string = "AND NOT (e)<-[:REVISION_OF]-(:Entity) "
    status_query_string = ""
    prop_query_string = f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}"
    if include_revisions:
        revision_query_string = ""
    if status is not None:
        status_query_string = f"AND (NOT e:Dataset OR TOLOWER(e.status) = '{status}') "
    if prop_key is not None:
        prop_query_string = f"RETURN apoc.coll.toSet(COLLECT(e.{prop_key})) AS {record_field_name}"
    results = []
    query = ("MATCH (e:Entity) "
             f"WHERE e.uuid IN {sibling_uuids_string} "
             f"{revision_query_string}"
             f"{status_query_string}"
             f"{prop_query_string}")

    with neo4j_driver.session() as session:
        record = session.read_transaction(schema_neo4j_queries.execute_readonly_tx, query)

        if record and record[record_field_name]:
            if prop_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = schema_neo4j_queries.nodes_to_dicts(record[record_field_name])
    return results


"""
Get all tuplets by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique tuplet dictionaries returned from the Cypher query
"""
def get_tuplets(neo4j_driver, uuid, status, prop_key):
    tuplet_uuids = schema_neo4j_queries.get_tuplets(neo4j_driver, uuid, property_key='uuid')
    tuplets_uuids_string = str(tuplet_uuids)
    status_query_string = ""
    prop_query_string = f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}"
    if status is not None:
        status_query_string = f"AND (NOT e:Dataset OR TOLOWER(e.status) = '{status}') "
    if prop_key is not None:
        prop_query_string = f"RETURN apoc.coll.toSet(COLLECT(e.{prop_key})) AS {record_field_name}"
    results = []
    query = ("MATCH (e:Entity) "
             f"WHERE e.uuid IN {tuplets_uuids_string} "
             f"{status_query_string}"
             f"{prop_query_string}")

    with neo4j_driver.session() as session:
        record = session.read_transaction(schema_neo4j_queries.execute_readonly_tx, query)

        if record and record[record_field_name]:
            if prop_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = schema_neo4j_queries.nodes_to_dicts(record[record_field_name])
    return results

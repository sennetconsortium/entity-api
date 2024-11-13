import ast

from neo4j.exceptions import TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Directly called by schema_triggers.py
####################################################################################################

"""
Get the direct ancestors uuids of a given dataset by uuid

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
list
    A unique list of uuids of source entities
"""


def get_dataset_direct_ancestors(neo4j_driver, uuid, property_key=None):
    results = []
    if property_key:
        query = (f"MATCH (s:Entity)<-[:USED]-(a:Activity)<-[:WAS_GENERATED_BY]-(t:Dataset) "
                 f"WHERE t.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(s.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (s:Entity)<-[:USED]-(a:Activity)<-[:WAS_GENERATED_BY]-(t:Dataset) "
                 f"WHERE t.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

    logger.info("======get_dataset_direct_ancestors() query======")
    logger.info(query)

    # Sessions will often be created and destroyed using a with block context
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
Get the direct descendant uuids of a given dataset by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
property_key : str
    A target property key for result filtering
match_case : str
    An additional match case query

Returns
-------
list
    A unique list of entities
"""
def get_dataset_direct_descendants(neo4j_driver, uuid, property_key=None, match_case = ''):
    results = []
    if property_key:
        query = (f"MATCH (s:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(t:Dataset) "
                 f"WHERE t.uuid = '{uuid}' {match_case}"
                 f"RETURN apoc.coll.toSet(COLLECT(s.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (s:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(t:Dataset) "
                 f"WHERE t.uuid = '{uuid}' {match_case}"
                 f"RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

    logger.info("======get_dataset_direct_descendants() query======")
    logger.info(query)

    # Sessions will often be created and destroyed using a with block context
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
Get the uuids for each entity in a list that doesn't belong to a certain entity type. Uuids are ordered by type

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
direct_ancestor_uuids : list
    List of the uuids to be filtered
entity_type : string
    The entity to be excluded

Returns
-------
dict
    A dictionary of entity uuids that don't pass the filter, grouped by entity_type
"""


def filter_ancestors_by_type(neo4j_driver, direct_ancestor_uuids, entity_type):
    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid in {direct_ancestor_uuids} AND toLower(e.entity_type) <> '{entity_type.lower()}' "
             f"RETURN e.entity_type AS entity_type, collect(e.uuid) AS uuids")
    logger.info("======filter_ancestors_by_type======")
    logger.info(query)

    with neo4j_driver.session() as session:
        records = session.run(query).data()

    return records if records else None


"""
Get the origin (organ) sample ancestor of a given entity by uuid

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
list
    A unique list of uuids of source entities
"""


def get_origin_samples(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY|USED*]->(s:Sample) "
             f"WHERE e.uuid='{uuid}' and s.sample_category='Organ' "
             f"return apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

    logger.info("======get_origin_sample() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)
        if record and record[record_field_name]:
            # Convert the entity node to dict
            result = _nodes_to_dicts(record[record_field_name])

    return result


"""
Get the sample organ name and source metadata information of the given dataset uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 

Returns
-------
str: The sample organ name
str: The source metadata (string representation of a Python dict)
"""


def get_dataset_organ_and_source_info(neo4j_driver, uuid):
    organ_names = set()
    source_metadata = set()
    source_type = None

    with neo4j_driver.session() as session:
        sample_query = (f"MATCH (e:Dataset)-[:USED|WAS_GENERATED_BY*]->(s:Sample) "
                        f"WHERE e.uuid='{uuid}' AND s.sample_category is not null and s.sample_category='Organ' "
                        f"RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

        logger.info("======get_dataset_organ_and_source_info() sample_query======")
        logger.info(sample_query)

        with neo4j_driver.session() as session:
            record = session.read_transaction(_execute_readonly_tx, sample_query)

            if record and record[record_field_name]:
                # Convert the list of nodes to a list of dicts
                sample_records = _nodes_to_dicts(record[record_field_name])
                for sample_record in sample_records:
                    if sample_record['organ'] is None:
                        organ_names.add(sample_record['sample_category'])
                    else:
                        organ_names.add(sample_record['organ'])

                    sample_uuid = sample_record['uuid']

                    source_query = (f"MATCH (s:Sample)-[:USED|WAS_GENERATED_BY*]->(d:Source) "
                                    f"WHERE s.uuid='{sample_uuid}' AND s.sample_category is not null "
                                    f"RETURN DISTINCT d.metadata AS source_metadata, d.source_type AS source_type")

                    logger.info("======get_dataset_organ_and_source_info() source_query======")
                    logger.info(source_query)

                    source_record = session.read_transaction(_execute_readonly_tx, source_query)

                    if source_record:
                        source_metadata.add(source_record[0])
                        source_type = source_record[1]

    return organ_names, source_metadata, source_type


def get_entity_type(neo4j_driver, entity_uuid: str) -> str:
    query: str = f"Match (ent {{uuid: '{entity_uuid}'}}) return ent.entity_type"

    logger.info("======get_entity_type() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)
        if record and len(record) == 1:
            return record[0]

    return None


def get_entity_creation_action_activity(neo4j_driver, entity_uuid: str) -> str:
    query: str = f"MATCH (ds {{uuid:'{entity_uuid}'}})-[:WAS_GENERATED_BY]->(a:Activity) RETURN a.creation_action"

    logger.info("======get_entity_creation_action() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)
        if record and len(record) == 1:
            return record[0]

    return None

"""
Create or recreate one or more linkages
between the target entity node and the collection nodes in neo4j

Note: the size of direct_ancestor_uuids equals to that of activity_data_dict_list

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
direct_ancestor_uuids : list
    A list of uuids of direct ancestors

"""


def link_collection_to_entity(neo4j_driver, entity_uuid, direct_ancestor_uuids):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(tx, direct_ancestor_uuid, entity_uuid, 'IN_COLLECTION', '->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_collection_to_entity(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_collection_to_entity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Link a Collection to all the Datasets it should contain per the provided
argument.  First, all existing linkages are deleted, then a link between
each entry of the dataset_uuid_list and collection_uuid is created in the
correction direction with an IN_COLLECTION relationship.

No Activity nodes are created in the relationship between a Collection and
its Datasets.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of a Collection entity which is the target of an IN_COLLECTION relationship.
dataset_uuid_list : list of str
    A list of uuids of Dataset entities which are the source of an IN_COLLECTION relationship.
"""


def link_collection_to_entities(neo4j_driver, collection_uuid, entities_uuid_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this Collection and its member Entities
            _delete_collection_linkages_tx(tx=tx
                                           , uuid=collection_uuid)

            # Create relationship from each member Entity node to this Collection node
            for entity_uuid in entities_uuid_list:
                _create_relationship_tx(tx=tx
                                        , source_node_uuid=entity_uuid
                                        , direction='->'
                                        , target_node_uuid=collection_uuid
                                        , relationship='IN_COLLECTION')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_collection_to_entities(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_collection_to_entities() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Create or recreate one or more linkages
between the target entity node and the agent nodes in neo4j


Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
direct_ancestor_uuids : list
    A list of uuids of direct ancestors

"""


def link_entity_to_agent(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this entity and its direct ancestors
            _delete_entity_agent_linkages_tx(tx, entity_uuid)
            _delete_activity_node_and_linkages_tx(tx, entity_uuid)

            # Get the activity uuid
            activity_uuid = activity_data_dict['uuid']

            # Create the Acvitity node
            _create_activity_tx(tx, activity_data_dict)

            # Create relationship from this Activity node to the target entity node
            _create_relationship_tx(tx, activity_uuid, entity_uuid, 'WAS_GENERATED_BY', '<-')

            # Create relationship from each ancestor entity node to this node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(tx, direct_ancestor_uuid, entity_uuid, 'WAS_ATTRIBUTED_TO', '<-')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_agent(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_agent() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Create or recreate one or more linkages
between the target entity node and another entity nodes in neo4j


Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
direct_ancestor_uuids : list
    A list of uuids of direct ancestors

"""


def link_entity_to_entity_via_activity(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages and Activity node between this entity and its direct ancestors
            _delete_activity_node_and_linkages_tx(tx, entity_uuid)

            # Get the activity uuid
            activity_uuid = activity_data_dict['uuid']

            # Create the Acvitity node
            _create_activity_tx(tx, activity_data_dict)

            # Create relationship from this Activity node to the target entity node
            _create_relationship_tx(tx, activity_uuid, entity_uuid, 'WAS_GENERATED_BY', '<-')

            # Create relationship from each ancestor entity node to this Activity node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(tx, direct_ancestor_uuid, activity_uuid, 'USED', '<-')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_entity_via_activity(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_entity_via_activity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Create or recreate one or more linkages
between the target entity node and another entity nodes in neo4j


Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
direct_ancestor_uuids : list
    A list of uuids of direct ancestors

"""


def link_entity_to_entity(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this entity and its direct ancestors
            _delete_entity_entity_linkages_tx(tx, entity_uuid)

            # Create relationship from each ancestor entity node to this node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(tx, direct_ancestor_uuid, entity_uuid, 'WAS_DERIVED_FROM', '<-')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_entity(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_entity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Create a revision linkage from the target entity node to the entity node 
of the previous revision in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target entity
previous_revision_entity_uuid : str
    The uuid of previous revision entity
"""


def link_entity_to_previous_revision(neo4j_driver, entity_uuid, previous_revision_entity_uuid):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # Create relationship from ancestor entity node to this Activity node
            _create_relationship_tx(tx, entity_uuid, previous_revision_entity_uuid, 'REVISION_OF', '->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_previous_revision(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_previous_revision() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

"""
Get the uuids of previous revision entities for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The list of previous revision ids
"""


def get_previous_revision_uuids(neo4j_driver, uuid):
    results = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH p=(e:Entity)-[:REVISION_OF*]->(previous_revision:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             "WITH length(p) as p_len, collect(distinct previous_revision.uuid) AS prev_revisions "
             f"RETURN collect(distinct prev_revisions) AS {record_field_name}")

    logger.info("======get_previous_revision_uuids() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


"""
Get the list of uuids of next revision entities for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The list of next revision ids
"""


def get_next_revision_uuids(neo4j_driver, uuid):
    result = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH n=(e:Entity)<-[:REVISION_OF*]-(next_revision:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             "WITH length(n) as n_len, collect(distinct next_revision.uuid) AS next_revisions "
             f"RETURN collect(distinct next_revisions) AS {record_field_name}")

    logger.info("======get_next_revision_uuids() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get the uuid of previous revision entity for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The parent dict, can either be a Sample or Source
"""


def get_previous_revision_uuid(neo4j_driver, uuid):
    result = None

    # Don't use [r:REVISION_OF] because 
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)-[:REVISION_OF]->(previous_revision:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN previous_revision.uuid AS {record_field_name}")

    logger.info("======get_previous_revision_uuid() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get the uuid of next revision entity for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The parent dict, can either be a Sample or Source
"""


def get_next_revision_uuid(neo4j_driver, uuid):
    result = None

    # Don't use [r:REVISION_OF] because 
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)<-[:REVISION_OF]-(next_revision:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN next_revision.uuid AS {record_field_name}")

    logger.info("======get_next_revision_uuid() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get a list of associated collection uuids for a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of dataset
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of collection uuids
"""


def get_dataset_collections(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

    logger.info("======get_dataset_collections() query======")
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
Get the associated Upload for a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of dataset
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A Upload dict
"""


def get_dataset_upload(neo4j_driver, uuid, property_key=None):
    result = {}

    query = (f"MATCH (e:Entity)-[:IN_UPLOAD]->(s:Upload) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN s AS {record_field_name}")

    logger.info("======get_dataset_upload() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the node to a dict
            result = _node_to_dict(record[record_field_name])

    return result


"""
Get a list of associated dataset dicts for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of collection

Returns
-------
list
    The list containing associated dataset dicts
"""


def get_collection_entities(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_collection_entities() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the list of nodes to a list of dicts
            results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get a dictionary with an entry for each Dataset in a Collection. The dictionary is
keyed by Dataset uuid and contains the Dataset data_access_level.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of a Collection

Returns
-------
dict
     A dictionary with an entry for each Dataset in a Collection. The dictionary is
     keyed by Dataset uuid and contains the Dataset data_access_level.
"""


def get_collection_datasets_data_access_levels(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (d:Dataset)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN COLLECT(DISTINCT d.data_access_level) AS {record_field_name}")

    logger.info("======get_collection_datasets_data_access_levels() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]

    return results


"""
Get a dictionary with an entry for each Dataset in a Collection. The dictionary is
keyed by Dataset uuid and contains the Dataset data_access_level.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of a Collection

Returns
-------
dict
     A dictionary with an entry for each Dataset in a Collection. The dictionary is
     keyed by Dataset uuid and contains the Dataset data_access_level.
"""


def get_collection_datasets_statuses(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (d: Dataset)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN COLLECT(DISTINCT d.status) AS {record_field_name}")

    logger.info("======get_collection_datasets_statuses() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]
        else:
            results = []

    return results


"""
Link the dataset nodes to the target Upload node

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
upload_uuid : str
    The uuid of target Upload 
dataset_uuids_list : list
    A list of dataset uuids to be linked to Upload
"""


def link_datasets_to_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in dataset_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    dataset_uuids_list_str = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Upload and the given Datasets")

            query = (f"MATCH (s:Upload), (d:Dataset) "
                     f"WHERE s.uuid = '{upload_uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     # Use MERGE instead of CREATE to avoid creating the existing relationship multiple times
                     # MERGE creates the relationship only if there is no existing relationship
                     f"MERGE (s)<-[r:IN_UPLOAD]-(d)")

            logger.info("======link_datasets_to_upload() query======")
            logger.info(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_datasets_to_upload(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit link_datasets_to_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Unlink the dataset nodes from the target Upload node

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
upload_uuid : str
    The uuid of target Upload 
dataset_uuids_list : list
    A list of dataset uuids to be unlinked from Upload
"""


def unlink_datasets_from_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in dataset_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    dataset_uuids_list_str = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Delete relationships between the target Upload and the given Datasets")

            query = (f"MATCH (s:Upload)<-[r:IN_UPLOAD]-(d:Dataset) "
                     f"WHERE s.uuid = '{upload_uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     f"DELETE r")

            logger.info("======unlink_datasets_from_upload() query======")
            logger.info(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling unlink_datasets_from_upload(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit unlink_datasets_from_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Get a list of associated dataset dicts for a given Upload

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of Upload
property_key : str
    A target property key for result filtering

Returns
-------
list
    The list containing associated dataset dicts
"""


def get_upload_datasets(neo4j_driver, uuid, property_key=None, query_filter=''):
    results = []

    if property_key:
        query = (f"MATCH (e:Dataset)-[:IN_UPLOAD]->(s:Upload) "
                 f"WHERE s.uuid = '{uuid}' {query_filter} "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Dataset)-[:IN_UPLOAD]->(s:Upload) "
                 f"WHERE s.uuid = '{uuid}' {query_filter} "
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_upload_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get count of published Dataset in the provenance hierarchy for a given Sample/Source

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Sample, Source
uuid : str
    The uuid of target entity 

Returns
-------
int
    The count of published Dataset in the provenance hierarchy 
    below the target entity (Source, Sample and Collection)
"""


def count_attached_published_datasets(neo4j_driver, entity_type, uuid):
    query = (f"MATCH (e:{entity_type})<-[:USED|WAS_GENERATED_BY*]-(d:Dataset) "
             # Use the string function toLower() to avoid case-sensetivity issue
             f"WHERE e.uuid='{uuid}' AND toLower(d.status) = 'published' "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"RETURN COUNT(d) AS {record_field_name}")

    logger.info("======count_attached_published_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        count = record[record_field_name]

        # logger.info("======count_attached_published_datasets() resulting count======")
        # logger.info(count)

        return count


"""
Get the parent of a given Sample entity

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
    The parent dict, can either be a Sample or Source
"""


def get_sample_direct_ancestor(neo4j_driver, uuid, property_key=None):
    result = {}

    if property_key:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN parent.{property_key} AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN parent AS {record_field_name}")

    logger.info("======get_sample_direct_ancestor() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                result = record[record_field_name]
            else:
                # Convert the entity node to dict
                result = _node_to_dict(record[record_field_name])

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

    return result


""" 
Retrieve a boolean value for if an ancestor of this entity contains RUI location information

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 

Returns:
    Boolean: If an ancestor contains RUI location information
"""


def get_has_rui_information(neo4j_driver, entity_uuid):
    results = str(False)

    # First check the ancestry of the given entity and if the origin sample is
    # Adipose Tissue (AD), Blood (BD), Bone Marrow (BM), Breast (BS), Muscle (MU), or Other (OT), then return "N/A"

    organ_query = (f"MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(o:Sample) "
                   f"WHERE e.uuid='{entity_uuid}' AND o.sample_category='Organ' AND o.organ in ['AD', 'BD', 'BM', 'BS', 'MU', 'OT'] "
                   f"return 'N/A' as {record_field_name}")

    logger.info("======get_has_rui_information() organ_query======")
    logger.info(organ_query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, organ_query)

        if record and record[record_field_name]:
            results = (record[record_field_name])
            return str(results)

    # If the first query fails to return then grab the ancestor Block and check if it contains  rui_location
    query = (f"MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(s:Sample) "
             f"WHERE e.uuid='{entity_uuid}' AND s.rui_location IS NOT NULL AND NOT TRIM(s.rui_location) = '' "
             f"RETURN COUNT(s) > 0 as {record_field_name}")

    logger.info("======get_has_rui_information() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = (record[record_field_name])

    return str(results)


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


def _execute_readonly_tx(tx, query):
    result = tx.run(query)
    record = result.single()
    return record


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
Delete the Activity node and linkages between an entity and its direct ancestors

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target entity (child of those direct ancestors)
"""


def _delete_activity_node_and_linkages_tx(tx, uuid):
    query = (f"MATCH (s:Entity)-[in:WAS_GENERATED_BY]->(a:Activity)-[out:USED]->(t:Entity) "
             f"WHERE s.uuid = '{uuid}' "
             f"DELETE in, a, out")

    logger.info("======_delete_activity_node_and_linkages_tx() query======")
    logger.info(query)

    result = tx.run(query)


"""
Delete linkages between a publication and its associated collection

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target publication
"""


def _delete_publication_associated_collection_linkages_tx(tx, uuid):
    query = (f"MATCH (p:Publication)-[r:USES_DATA]->(c:Collection) "
             f"WHERE p.uuid = '{uuid}' "
             f"DELETE r")

    logger.info("======_delete_publication_associated_collection_linkages_tx() query======")
    logger.info(query)

    result = tx.run(query)


"""
Delete the linkages between a Collection and its member Datasets

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid of the Collection, related to Datasets by an IN_COLLECTION relationship
"""


def _delete_collection_linkages_tx(tx, uuid):
    query = (f"MATCH (e:Entity)-[in:IN_COLLECTION]->(c:Collection)"
             f" WHERE c.uuid = '{uuid}' "
             f" DELETE in")

    logger.info("======_delete_collection_linkages_tx() query======")
    logger.info(query)

    result = tx.run(query)


"""
Delete the linkage between an entity and another entity

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target entity (child of those direct ancestors)
"""


def _delete_entity_entity_linkages_tx(tx, uuid):
    query = (f"MATCH (s:Entity)-[out:WAS_DERIVED_FROM]->(t:Entity) "
             f"WHERE s.uuid = '{uuid}' "
             f"DELETE out")

    logger.debug("======_delete_entity_entity_linkages_tx() query======")
    logger.debug(query)

    result = tx.run(query)


"""
Delete the linkage between an entity and agent

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target entity (child of those direct ancestors)
"""


def _delete_entity_agent_linkages_tx(tx, uuid):
    query = (f"MATCH (s:Entity)-[out:WAS_ATTRIBUTED_TO]->(t:Entity) "
             f"WHERE s.uuid = '{uuid}' "
             f"DELETE out")

    logger.debug("======_delete_entity_agent_linkages_tx() query======")
    logger.debug(query)

    result = tx.run(query)


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

    match_case_source = f" IN {source_node_uuid}" if type(source_node_uuid) is list else f" = '{source_node_uuid}'"
    match_case_target = f" IN {target_node_uuid}" if type(target_node_uuid) is list else f" = '{target_node_uuid}'"

    query = (f"MATCH (s), (t) "
             f"WHERE s.uuid {match_case_source} AND t.uuid {match_case_target} "
             f"CREATE (s){incoming}[r:{relationship}]{outgoing}(t) "
             f"RETURN type(r) AS {record_field_name}")

    logger.info("======_create_relationship_tx() query======")
    logger.info(query)

    result = tx.run(query)


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


def execute_readonly_tx(tx, query):
    result = tx.run(query)
    record = result.single()
    return record


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


def node_to_dict(entity_node):
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


def nodes_to_dicts(nodes):
    dicts = []

    for node in nodes:
        entity_dict = node_to_dict(node)
        dicts.append(entity_dict)

    return dicts


"""
Create or recreate linkage 
between the publication node and the associated collection node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of the publication
associated_collection_uuid : str
    the uuid of the associated collection
"""


def link_publication_to_associated_collection(neo4j_driver, entity_uuid, associated_collection_uuid):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete any old linkage between this publication and any associated_collection
            _delete_publication_associated_collection_linkages_tx(tx, entity_uuid)

            # Create relationship from this publication node to the associated collection node
            _create_relationship_tx(tx, entity_uuid, associated_collection_uuid, 'USES_DATA', '->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_publication_to_associated_collection(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_publication_to_associated_collection() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Get a list of associated Datasets and Publications (subclass of Dataset) uuids for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of collection
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of datasets and publications
"""


def get_collection_associated_datasets(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_collection_associated_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get the associated collection for a given publication

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of publication
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A dictionary representation of the collection
"""


def get_publication_associated_collection(neo4j_driver, uuid):
    result = {}

    query = (f"MATCH (p:Publication)-[:USES_DATA]->(c:Collection) "
             f"WHERE p.uuid = '{uuid}' "
             f"RETURN c as {record_field_name}")

    logger.info("=====get_publication_associated_collection() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = node_to_dict(record[record_field_name])

    return result


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
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


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
def build_properties_map(entity_data_dict):
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
            # Convert list and dict to string, retain the original data without removing any control characters
            # Will need to call schema_manager.convert_str_literal() to convert the list/dict literal back to object
            # Note that schema_manager.convert_str_literal() removes any control characters to avoid SyntaxError
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
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
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
    node_properties_map = build_properties_map(entity_data_dict)

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

            entity_dict = node_to_dict(entity_node)

            # logger.info("======update_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit update_entity() transaction, rollback")

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
def get_siblings(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'LAB' "
                 f"MATCH (sibling:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent) "
                 f"WHERE sibling <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(sibling.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'LAB' "
                 f"MATCH (sibling:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent) "
                 f"WHERE sibling <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(sibling)) AS {record_field_name}")

    logger.info("======get_siblings() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

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
def get_tuplets(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (tuplet:Entity)-[:WAS_GENERATED_BY]->(a) "
                 f"WHERE tuplet <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(tuplet.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (tuplet:Entity)-[:WAS_GENERATED_BY]->(a) "
                 f"WHERE tuplet <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(tuplet)) AS {record_field_name}")

    logger.info("======get_tuplets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get all collections by uuid
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
list
    A list of unique collection dictionaries returned from the Cypher query
"""
def get_collections(neo4j_driver, uuid, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (c:Collection)<-[:IN_COLLECTION]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (c:Collection)<-[:IN_COLLECTION]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

    logger.info("======get_collections() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results



"""
Get all uploads by uuid
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
list
    A list of unique upload dictionaries returned from the Cypher query
"""
def get_uploads(neo4j_driver, uuid, property_key = None):
    results = []
    if property_key:
        query = (f"MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(u.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(u)) AS {record_field_name}")

    logger.info("======get_uploads() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)
        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results

"""
Get the associated sources for a given entity (dataset/publication)

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of entity
filter_out : list 
    Any sources that should not be returned

Returns
-------
list
    A list of sources associated with an entity
"""


def get_sources_associated_entity(neo4j_driver, uuid, filter_out = None):
    results = []

    query_filter = ''
    if filter_out is not None:
        query_filter = f" and not s.uuid in {filter_out}"

    query = (f"MATCH (e:Entity)-[*]->(s:Source) "
             f"WHERE e.uuid = '{uuid}' {query_filter} "
             f"RETURN apoc.coll.toSet(COLLECT(s))  as {record_field_name}")

    logger.info("=====get_sources_associated_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            results = nodes_to_dicts(record[record_field_name])

        for result in results:
            if 'metadata' in result and result['metadata'] != '{}':
                result['metadata'] = ast.literal_eval(result['metadata'])
            else:
                result.pop('metadata', None)

    return results

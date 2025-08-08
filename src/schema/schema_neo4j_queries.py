from neo4j.exceptions import TransactionError
import logging
from typing import List, Union

import schema.schema_manager
from lib.property_groups import PropertyGroups
import json

from schema import schema_manager

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = "result"


####################################################################################################
## Directly called by schema_triggers.py
####################################################################################################


def get_dataset_direct_ancestors(neo4j_driver, uuid, property_key=None):
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
    results = []
    if property_key:
        query = (
            "MATCH (t:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(d:Dataset) "
            "WHERE d.uuid = $uuid "
            f"RETURN apoc.coll.toSet(COLLECT(t.{property_key})) AS {record_field_name}"
        )
    else:
        _activity_query_part = activity_query_part(for_all_match=True)
        query = (
            "MATCH (t:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(d:Dataset) "
            "WHERE d.uuid = $uuid "
            f"{_activity_query_part} {record_field_name}"
        )

    logger.info("======get_dataset_direct_ancestors() query======")
    logger.info(query)

    # Sessions will often be created and destroyed using a with block context
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def get_dataset_direct_descendants(neo4j_driver, uuid, property_keys=None, match_case=""):
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
    results = []
    if property_keys:
        with_str = ", ".join([f"s.{key} AS {key}" for key in property_keys])
        return_str = ", ".join([f"{key}: {key}" for key in property_keys])
        query = (
            "MATCH (t:Dataset)<-[:USED]-(a:Activity)<-[:WAS_GENERATED_BY]-(s:Entity) "
            f"WHERE t.uuid = $uuid {match_case} "
            f"WITH {with_str}, a.creation_action AS creation_action "
            f"RETURN COLLECT(apoc.map.merge({{{return_str}}}, {{creation_action: creation_action}})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (t:Dataset)<-[:USED]-(a:Activity)<-[:WAS_GENERATED_BY]-(s:Entity) "
            f"WHERE t.uuid = $uuid {match_case} "
            "WITH s, a.creation_action AS creation_action "
            f"RETURN COLLECT(apoc.map.merge(s, {{creation_action: creation_action}})) AS {record_field_name}"
        )
    logger.info("======get_dataset_direct_descendants() query======")
    logger.info(query)

    # Sessions will often be created and destroyed using a with block context
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)
        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def filter_ancestors_by_type(neo4j_driver, direct_ancestor_uuids, entity_type):
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

    query = (
        "MATCH (e:Entity) "
        "WHERE e.uuid in $direct_ancestor_uuids AND toLower(e.entity_type) <> $entity_type "
        "RETURN e.entity_type AS entity_type, collect(e.uuid) AS uuids"
    )
    logger.info("======filter_ancestors_by_type======")
    logger.info(query)

    with neo4j_driver.session() as session:
        records = session.run(
            query, direct_ancestor_uuids=direct_ancestor_uuids, entity_type=entity_type.lower()
        ).data()

    return records if records else None


def get_origin_samples(neo4j_driver, uuids: List, is_bulk: bool = True):
    """
    Get the origin (organ) sample ancestor of a given entities by uuids

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuids : List[str]
        A list of uuids to be filtered
    is_bulk : bool
        Whether to return the result for bulk processing
    Returns
    -------
    list
        If is_bulk True A list in the form of [{result:List[dict], uuid:str}] where result is a list of results associated with the uuid
        else a regular List[dict]
    """
    result = {}

    activity_grab_part = "WITH e, s, apoc.map.fromPairs([['protocol_url', a.protocol_url], ['creation_action', a.creation_action]]) as a2 WITH e, apoc.map.merge(s,a2) as x  "
    return_part = f"{activity_grab_part} RETURN apoc.coll.toSet(COLLECT(x)) AS "
    if is_bulk:
        return_part = (
            f"{activity_grab_part} "
            "WITH e, COLLECT(x) as list return collect(apoc.map.fromPairs([['uuid', e.uuid], ['result', list]])) AS "
        )

    query = (
        "MATCH (e:Entity)-[:WAS_GENERATED_BY|USED*]->(s:Sample) "
        "WHERE e.uuid IN $uuids and s.sample_category='Organ' "
        "MATCH (e2:Entity)-[:WAS_GENERATED_BY]->(a:Activity) WHERE e2.uuid = s.uuid "
        f"{return_part} {record_field_name}"
    )

    logger.info("======get_origin_samples() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuids=uuids)
        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


def get_dataset_source_organs_info(neo4j_driver, dataset_uuid: str) -> List[dict]:
    """
    For every Sample organ associated with the given dataset_uuid, retrieve the
    organ information and organ source information for use in composing a title for the Dataset.

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    dataset_uuid : str
        The UUID of a Dataset

    Returns
    -------
    list : List of dicts that contain 'source_uuid', 'source_type', 'organ_type' and 'source_metadata'
    """
    with neo4j_driver.session() as session:
        ds_sources_organs_query = (
            "MATCH (d:Dataset)-[:USED|WAS_GENERATED_BY*]->(org:Sample)-[:USED|WAS_GENERATED_BY*]->(s:Source) "
            "WHERE d.uuid = $dataset_uuid "
            "AND org.sample_category = 'Organ' "
            "AND org.organ IS NOT NULL "
            "RETURN apoc.coll.toSet(COLLECT("
            "{source_uuid: s.uuid, source_type: s.source_type, "
            "source_metadata: CASE WHEN s.metadata IS NOT NULL THEN apoc.convert.fromJsonMap(s.metadata) ELSE {} END, "
            "organ_type: org.organ}"
            ")) AS source_organ_set"
        )

        logger.info("======get_dataset_source_organs_info() ds_sources_organs_query======")
        logger.info(ds_sources_organs_query)

        with neo4j_driver.session() as session:
            record = session.read_transaction(
                execute_readonly_tx, ds_sources_organs_query, dataset_uuid=dataset_uuid
            )

    return record["source_organ_set"] if record and record["source_organ_set"] else []


def get_entity_type(neo4j_driver, entity_uuid: str) -> str:
    query: str = "Match (ent {uuid: $entity_uuid}) return ent.entity_type"

    logger.info("======get_entity_type() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, entity_uuid=entity_uuid)
        if record and len(record) == 1:
            return record[0]

    return None


def get_entity_creation_action_activity(neo4j_driver, entity_uuid: str) -> str:
    query: str = (
        "MATCH (ds {uuid: $entity_uuid})-[:WAS_GENERATED_BY]->(a:Activity) RETURN a.creation_action"
    )

    logger.info("======get_entity_creation_action() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, entity_uuid=entity_uuid)
        if record and len(record) == 1:
            return record[0]

    return None


def link_collection_to_entity(neo4j_driver, entity_uuid, direct_ancestor_uuids):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(
                    tx, direct_ancestor_uuid, entity_uuid, "IN_COLLECTION", "->"
                )

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_collection_to_entity(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_collection_to_entity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


def link_collection_to_entities(neo4j_driver, collection_uuid, entities_uuid_list):
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
    entities_uuid_list : list of str
        A list of uuids of Dataset entities which are the source of an IN_COLLECTION relationship.
    """

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this Collection and its member Entities
            _delete_collection_linkages_tx(tx=tx, uuid=collection_uuid)

            # Create relationship from each member Entity node to this Collection node
            for entity_uuid in entities_uuid_list:
                _create_relationship_tx(
                    tx=tx,
                    source_node_uuid=entity_uuid,
                    direction="->",
                    target_node_uuid=collection_uuid,
                    relationship="IN_COLLECTION",
                )

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_collection_to_entities(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_collection_to_entities() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


def link_entity_to_agent(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this entity and its direct ancestors
            _delete_entity_agent_linkages_tx(tx, entity_uuid)
            _delete_activity_node_and_linkages_tx(tx, entity_uuid)

            # Get the activity uuid
            activity_uuid = activity_data_dict["uuid"]

            # Create the Acvitity node
            _create_activity_tx(tx, activity_data_dict)

            # Create relationship from this Activity node to the target entity node
            _create_relationship_tx(tx, activity_uuid, entity_uuid, "WAS_GENERATED_BY", "<-")

            # Create relationship from each ancestor entity node to this node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(
                    tx, direct_ancestor_uuid, entity_uuid, "WAS_ATTRIBUTED_TO", "<-"
                )

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_entity_to_agent(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_agent() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


def link_entity_to_entity_via_activity(
    neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict
):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages and Activity node between this entity and its direct ancestors
            _delete_activity_node_and_linkages_tx(tx, entity_uuid)

            # Get the activity uuid
            activity_uuid = activity_data_dict["uuid"]

            # Create the Acvitity node
            _create_activity_tx(tx, activity_data_dict)

            # Create relationship from this Activity node to the target entity node
            _create_relationship_tx(tx, activity_uuid, entity_uuid, "WAS_GENERATED_BY", "<-")

            # Create relationship from each ancestor entity node to this Activity node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(tx, direct_ancestor_uuid, activity_uuid, "USED", "<-")

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_entity_to_entity_via_activity(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info(
                "Failed to commit link_entity_to_entity_via_activity() transaction, rollback"
            )
            tx.rollback()

        raise TransactionError(msg)


def link_entity_to_entity(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this entity and its direct ancestors
            _delete_entity_entity_linkages_tx(tx, entity_uuid)

            # Create relationship from each ancestor entity node to this node
            for direct_ancestor_uuid in direct_ancestor_uuids:
                _create_relationship_tx(
                    tx, direct_ancestor_uuid, entity_uuid, "WAS_DERIVED_FROM", "<-"
                )

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_entity_to_entity(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_entity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


def link_entity_to_previous_revision(neo4j_driver, entity_uuid, previous_revision_entity_uuid):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # Create relationship from ancestor entity node to this Activity node
            _create_relationship_tx(
                tx, entity_uuid, previous_revision_entity_uuid, "REVISION_OF", "->"
            )

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_entity_to_previous_revision(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_previous_revision() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


def get_previous_revision_uuids(neo4j_driver, uuid):
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
    results = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (
        "MATCH p=(e:Entity)-[:REVISION_OF*]->(previous_revision:Entity) "
        "WHERE e.uuid = $uuid "
        "WITH length(p) as p_len, collect(distinct previous_revision.uuid) AS prev_revisions "
        f"RETURN collect(distinct prev_revisions) AS {record_field_name}"
    )

    logger.info("======get_previous_revision_uuids() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def get_next_revision_uuids(neo4j_driver, uuid):
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
    result = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (
        "MATCH n=(e:Entity)<-[:REVISION_OF*]-(next_revision:Entity) "
        "WHERE e.uuid = $uuid "
        "WITH length(n) as n_len, collect(distinct next_revision.uuid) AS next_revisions "
        f"RETURN collect(distinct next_revisions) AS {record_field_name}"
    )

    logger.info("======get_next_revision_uuids() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


def get_previous_revision_uuid(neo4j_driver, uuid):
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
    result = None

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (
        "MATCH (e:Entity)-[:REVISION_OF]->(previous_revision:Entity) "
        "WHERE e.uuid = $uuid "
        f"RETURN previous_revision.uuid AS {record_field_name}"
    )

    logger.info("======get_previous_revision_uuid() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


def get_next_revision_uuid(neo4j_driver, uuid):
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
    result = None

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (
        "MATCH (e:Entity)<-[:REVISION_OF]-(next_revision:Entity) "
        "WHERE e.uuid = $uuid "
        f"RETURN next_revision.uuid AS {record_field_name}"
    )

    logger.info("======get_next_revision_uuid() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


def get_entity_collections(neo4j_driver, uuid, property_key=None):
    """
    Get a list of associated collection uuids for a given entity

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of entity
    property_key : str
        A target property key for result filtering

    Returns
    -------
    list
        A list of collection uuids
    """
    results = []

    if property_key:
        query = (
            "MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
            "WHERE e.uuid = $uuid "
            f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
            "WHERE e.uuid = $uuid "
            f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}"
        )

    logger.info("======get_entity_collections() query======")
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

    return results


def get_dataset_upload(neo4j_driver, uuid):
    """
    Get the associated Upload for a given dataset

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of dataset

    Returns
    -------
    dict
        A Upload dict
    """
    result = {}

    query = (
        "MATCH (e:Entity)-[:IN_UPLOAD]->(s:Upload) "
        "WHERE e.uuid = $uuid "
        f"RETURN s AS {record_field_name}"
    )

    logger.info("======get_dataset_upload() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Convert the node to a dict
            result = _node_to_dict(record[record_field_name])

    return result


def get_collection_entities(
    neo4j_driver,
    uuid,
    properties: Union[PropertyGroups, List[str]] = None,
    is_include_action: bool = True,
):
    """
    Get a list of associated dataset dicts for a given collection

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of collection
    properties : Union[PropertyGroups, List[str]]
    is_include_action : bool

    Returns
    -------
    list
        The list containing associated dataset dicts
    """
    results = []

    query = (
        "MATCH (t:Entity)-[:IN_COLLECTION]->(c:Collection|Epicollection) "
        "WHERE c.uuid = $uuid "
        f"{exclude_include_query_part(properties, is_include_action)}"
    )

    logger.info("======get_collection_entities() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def get_collection_datasets_data_access_levels(neo4j_driver, uuid):
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
    results = []

    query = (
        "MATCH (d:Dataset)-[:IN_COLLECTION]->(c:Collection) "
        "WHERE c.uuid = $uuid "
        f"RETURN COLLECT(DISTINCT d.data_access_level) AS {record_field_name}"
    )

    logger.info("======get_collection_datasets_data_access_levels() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]

    return results


def get_collection_datasets_statuses(neo4j_driver, uuid):
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
    results = []

    query = (
        "MATCH (d: Dataset)-[:IN_COLLECTION]->(c:Collection) "
        "WHERE c.uuid = $uuid "
        f"RETURN COLLECT(DISTINCT d.status) AS {record_field_name}"
    )

    logger.info("======get_collection_datasets_statuses() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]
        else:
            results = []

    return results


def link_datasets_to_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Upload and the given Datasets")

            query = (
                "MATCH (s:Upload), (d:Dataset) "
                "WHERE s.uuid = $upload_uuid AND d.uuid IN $dataset_uuids_list "
                # Use MERGE instead of CREATE to avoid creating the existing relationship multiple times
                # MERGE creates the relationship only if there is no existing relationship
                "MERGE (s)<-[r:IN_UPLOAD]-(d)"
            )

            logger.info("======link_datasets_to_upload() query======")
            logger.info(query)

            tx.run(query, upload_uuid=upload_uuid, dataset_uuids_list=dataset_uuids_list)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_datasets_to_upload(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit link_datasets_to_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


def unlink_datasets_from_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Delete relationships between the target Upload and the given Datasets")

            query = (
                "MATCH (s:Upload)<-[r:IN_UPLOAD]-(d:Dataset) "
                "WHERE s.uuid = $upload_uuid AND d.uuid IN $dataset_uuids_list "
                "DELETE r"
            )

            logger.info("======unlink_datasets_from_upload() query======")
            logger.info(query)

            tx.run(query, upload_uuid=upload_uuid, dataset_uuids_list=dataset_uuids_list)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling unlink_datasets_from_upload(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit unlink_datasets_from_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


def get_qa_published_derived_dataset(neo4j_driver, uuid):
    """
    Get the most recent QA or Published derived dataset given a primary dataset UUID

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity

    Returns
    -------
    str
        The UUID of the most recent QA or Published derived dataset
    """

def get_upload_datasets(
    neo4j_driver,
    uuid,
    query_filter="",
    properties: Union[PropertyGroups, List[str]] = None,
    is_include_action: bool = True,
):
    """
    Get a list of associated dataset dicts for a given Upload

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    query_filter: str
        An additional filter against the cypher match
    properties : Union[PropertyGroups, List[str]]
        A list of property keys to filter in or out from the normalized results, default is []
    is_include_action : bool
        Whether to include or exclude the listed properties

    Returns
    -------
    list
        The list containing associated dataset dicts
    """
    results = []

    is_filtered = isinstance(properties, PropertyGroups) or isinstance(properties, list)
    if is_filtered:
        query = (
            "MATCH (t:Dataset)-[:IN_UPLOAD]->(s:Upload) "
            f"WHERE s.uuid = $uuid {query_filter} "
            f"{exclude_include_query_part(properties, is_include_action, target_entity_type = 'Dataset')}"
        )
    else:
        _activity_query_part = activity_query_part(for_all_match=True)
        query = (
            "MATCH (t:Dataset)-[:IN_UPLOAD]->(s:Upload) "
            f"WHERE s.uuid = $uuid {query_filter} "
            f"{_activity_query_part} {record_field_name}"
        )

    logger.info("======get_upload_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Just return the list of property values from each entity node
            results = record[record_field_name]

    return results


def count_attached_published_datasets(neo4j_driver, entity_type, uuid):
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
    query = (
        f"MATCH (e: {entity_type})<-[:USED|WAS_GENERATED_BY*]-(d:Dataset) "
        # Use the string function toLower() to avoid case-sensetivity issue
        "WHERE e.uuid = $uuid AND toLower(d.status) = 'published' "
        # COLLECT() returns a list
        # apoc.coll.toSet() reruns a set containing unique nodes
        f"RETURN COUNT(d) AS {record_field_name}"
    )

    logger.info("======count_attached_published_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        count = record[record_field_name]

        # logger.info("======count_attached_published_datasets() resulting count======")
        # logger.info(count)

        return count


def get_sample_direct_ancestor(neo4j_driver, uuid, property_key=None):
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

    result = {}

    if property_key:
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
            # Filter out the Lab entity if it's the ancestor
            "WHERE e.uuid = $uuid AND parent.entity_type <> 'Lab' "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"RETURN parent.{property_key} AS {record_field_name}"
        )
    else:
        _activity_query_part = activity_query_part(for_all_match=True)
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(t:Entity) "
            # Filter out the Lab entity if it's the ancestor
            "WHERE e.uuid = $uuid AND t.entity_type <> 'Lab' "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"{_activity_query_part} {record_field_name}"
        )

    logger.info("======get_sample_direct_ancestor() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                result = record[record_field_name]
            else:
                # Convert the entity node to dict
                result = record[record_field_name][0]

    return result


def get_entity(neo4j_driver, uuid):
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
    result = {}

    query = "MATCH (e:Entity) " "WHERE e.uuid = $uuid " f"RETURN e AS {record_field_name}"

    logger.info("======get_entity() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result


def get_has_rui_information(neo4j_driver, entity_uuid):
    """
    Retrieve a boolean value for if an ancestor of this entity contains RUI location information

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    entity_uuid : str
        The uuid of target entity

    Returns
    --------
        bool str
        If an ancestor contains RUI location information
    """
    results = str(False)

    with neo4j_driver.session() as session:
        # Check the source of the given entity and if the source is not Human then return "N/A"
        source_query = (
            "MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(s:Source) "
            "WHERE e.uuid = $entity_uuid AND s.source_type <> 'Human' "
            f"RETURN 'N/A' as {record_field_name}"
        )

        record = session.read_transaction(
            execute_readonly_tx, source_query, entity_uuid=entity_uuid
        )
        if record and record[record_field_name]:
            results = record[record_field_name]
            return str(results)

        # Check the ancestry of the given entity and if the origin sample is
        # Adipose Tissue (UBERON:0001013), Blood (UBERON:0000178), Bone (UBERON:0001474), Bone Marrow (UBERON:0002371),
        # Muscle (UBERON:0005090), or Other (UBERON:0010000), then return "N/A"
        organ_query = (
            "MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(o:Sample) "
            "WHERE e.uuid = $entity_uuid AND o.sample_category = 'Organ' "
            "AND o.organ IN ['UBERON:0001013', 'UBERON:0000178', 'UBERON:0001474', 'UBERON:0002371', 'UBERON:0005090', 'UBERON:0010000'] "
            f"RETURN 'N/A' as {record_field_name}"
        )

        logger.info("======get_has_rui_information() organ_query======")
        logger.info(organ_query)

        record = session.read_transaction(execute_readonly_tx, organ_query, entity_uuid=entity_uuid)
        if record and record[record_field_name]:
            results = record[record_field_name]
            return str(results)

        # If the first query fails to return then grab the ancestor Block and check if it contains rui_location
        query = (
            "MATCH (e:Entity)-[:USED|WAS_GENERATED_BY*]->(s:Sample) "
            "WHERE e.uuid = $entity_uuid AND s.sample_category = 'Block' "
            "RETURN COLLECT("
            "CASE "
            "WHEN s.rui_exemption = true THEN 'Exempt' "
            "WHEN s.rui_location IS NOT NULL AND NOT TRIM(s.rui_location) = '' THEN 'True' "
            "ELSE 'False' "
            f"END) as {record_field_name}"
        )

        logger.info("======get_has_rui_information() query======")
        logger.info(query)

        record = session.read_transaction(execute_readonly_tx, query, entity_uuid=entity_uuid)
        if record and record[record_field_name]:
            values = record[record_field_name]
            if "True" in values:
                results = "True"
            elif "Exempt" in values:
                results = "Exempt"

        return str(results)


####################################################################################################
## Internal Functions
####################################################################################################


def _execute_readonly_tx(tx, query, **kwargs):
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
    result = tx.run(query, **kwargs)
    record = result.single()
    return record


def _create_activity_tx(tx, activity_data_dict):
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
    parameterized_str, parameterized_data = build_parameterized_map(activity_data_dict)

    query = (
        "CREATE (e:Activity) " f"SET e = {parameterized_str} " f"RETURN e AS {record_field_name}"
    )

    logger.info("======_create_activity_tx() query======")
    logger.info(query)

    result = tx.run(query, **parameterized_data)
    record = result.single()
    node = record[record_field_name]

    return node


def _delete_activity_node_and_linkages_tx(tx, uuid):
    """
    Delete the Activity node and linkages between an entity and its direct ancestors

    Parameters
    ----------
    tx : neo4j.Transaction object
        The neo4j.Transaction object instance
    uuid : str
        The uuid to target entity (child of those direct ancestors)
    """
    query = (
        "MATCH (s:Entity)-[in:WAS_GENERATED_BY]->(a:Activity)-[out:USED]->(t:Entity) "
        "WHERE s.uuid = $uuid "
        "DELETE in, a, out"
    )

    logger.info("======_delete_activity_node_and_linkages_tx() query======")
    logger.info(query)

    tx.run(query, uuid=uuid)


def _delete_publication_associated_collection_linkages_tx(tx, uuid):
    """
    Delete linkages between a publication and its associated collection

    Parameters
    ----------
    tx : neo4j.Transaction object
        The neo4j.Transaction object instance
    uuid : str
        The uuid to target publication
    """
    query = (
        "MATCH (p:Publication)-[r:USES_DATA]->(c:Collection) " "WHERE p.uuid = $uuid " "DELETE r"
    )

    logger.info("======_delete_publication_associated_collection_linkages_tx() query======")
    logger.info(query)

    tx.run(query, uuid=uuid)


def _delete_collection_linkages_tx(tx, uuid):
    """
    Delete the linkages between a Collection and its member Datasets

    Parameters
    ----------
    tx : neo4j.Transaction object
        The neo4j.Transaction object instance
    uuid : str
        The uuid of the Collection, related to Datasets by an IN_COLLECTION relationship
    """
    query = (
        "MATCH (e:Entity)-[in:IN_COLLECTION]->(c:Collection) " "WHERE c.uuid = $uuid " "DELETE in"
    )

    logger.info("======_delete_collection_linkages_tx() query======")
    logger.info(query)

    tx.run(query, uuid=uuid)


def _delete_entity_entity_linkages_tx(tx, uuid):
    """
    Delete the linkage between an entity and another entity

    Parameters
    ----------
    tx : neo4j.Transaction object
        The neo4j.Transaction object instance
    uuid : str
        The uuid to target entity (child of those direct ancestors)
    """
    query = (
        "MATCH (s:Entity)-[out:WAS_DERIVED_FROM]->(t:Entity) " "WHERE s.uuid = $uuid " "DELETE out"
    )

    logger.debug("======_delete_entity_entity_linkages_tx() query======")
    logger.debug(query)

    tx.run(query, uuid=uuid)


def _delete_entity_agent_linkages_tx(tx, uuid):
    """
    Delete the linkage between an entity and agent

    Parameters
    ----------
    tx : neo4j.Transaction object
        The neo4j.Transaction object instance
    uuid : str
        The uuid to target entity (child of those direct ancestors)
    """
    query = (
        "MATCH (s:Entity)-[out:WAS_ATTRIBUTED_TO]->(t:Entity) " "WHERE s.uuid = $uuid " "DELETE out"
    )

    logger.debug("======_delete_entity_agent_linkages_tx() query======")
    logger.debug(query)

    tx.run(query, uuid=uuid)


def _create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
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
    incoming = "-"
    outgoing = "-"

    if direction == "<-":
        incoming = direction

    if direction == "->":
        outgoing = direction

    match_case_source = (
        " IN $source_node_uuid" if type(source_node_uuid) is list else " = $source_node_uuid"
    )
    match_case_target = (
        " IN $target_node_uuid" if type(target_node_uuid) is list else " = $target_node_uuid"
    )

    query = (
        f"MATCH (s), (t) "
        f"WHERE s.uuid {match_case_source} AND t.uuid {match_case_target} "
        f"CREATE (s){incoming}[r:{relationship}]{outgoing}(t) "
        f"RETURN type(r) AS {record_field_name}"
    )

    logger.info("======_create_relationship_tx() query======")
    logger.info(query)

    tx.run(query, source_node_uuid=source_node_uuid, target_node_uuid=target_node_uuid)


def _node_to_dict(entity_node):
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

    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict


def _nodes_to_dicts(nodes):
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
    dicts = []

    for node in nodes:
        entity_dict = _node_to_dict(node)
        dicts.append(entity_dict)

    return dicts


def execute_readonly_tx(tx, query, **kwargs):
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
    result = tx.run(query, **kwargs)
    record = result.single()
    return record


def node_to_dict(entity_node):
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

    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict


def nodes_to_dicts(nodes):
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
    return [node_to_dict(node) for node in nodes]


def link_publication_to_associated_collection(
    neo4j_driver, entity_uuid, associated_collection_uuid
):
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
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete any old linkage between this publication and any associated_collection
            _delete_publication_associated_collection_linkages_tx(tx, entity_uuid)

            # Create relationship from this publication node to the associated collection node
            _create_relationship_tx(tx, entity_uuid, associated_collection_uuid, "USES_DATA", "->")

            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_publication_to_associated_collection(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            # Log the full stack trace, prepend a line with our message
            logger.info(
                "Failed to commit link_publication_to_associated_collection() transaction, rollback"
            )
            tx.rollback()

        raise TransactionError(msg)


def get_collection_associated_datasets(neo4j_driver, uuid, property_key=None):
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
    results = []

    if property_key:
        query = (
            "MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
            "WHERE c.uuid = $uuid "
            f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
            "WHERE c.uuid = $uuid "
            f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}"
        )

    logger.info("======get_collection_associated_datasets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


def get_publication_associated_collection(neo4j_driver, uuid):
    """
    Get the associated collection for a given publication

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of publication

    Returns
    -------
    dict
        A dictionary representation of the collection
    """
    result = {}

    query = (
        "MATCH (p:Publication)-[:USES_DATA]->(c:Collection) "
        "WHERE p.uuid = $uuid "
        f"RETURN c as {record_field_name}"
    )

    logger.info("=====get_publication_associated_collection() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = node_to_dict(record[record_field_name])

    return result


def get_children(
    neo4j_driver,
    uuid,
    properties: Union[PropertyGroups, List[str]] = None,
    is_include_action: bool = True,
):
    """
    Get all children by uuid

    Parameters
    ----------
    neo4j_driver : neo4j.Driver object
        The neo4j database connection pool
    uuid : str
        The uuid of target entity
    properties : List[str]
        A list of property keys to filter in or out from the normalized results, default is []
    is_include_action : bool
        Whether to include or exclude the listed properties

    Returns
    -------
    dict
        A list of unique child dictionaries returned from the Cypher query
    """
    results = []

    is_filtered = isinstance(properties, PropertyGroups) or isinstance(properties, list)
    if is_filtered:
        query = (
            "MATCH (e:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(t:Entity) "
            # The target entity can't be a Lab
            "WHERE e.uuid = $uuid AND e.entity_type <> 'Lab' "
            f"{exclude_include_query_part(properties, is_include_action)}"
        )
    else:
        _activity_query_part = activity_query_part(for_all_match=True)
        query = (
            "MATCH (e:Entity)<-[:USED]-(:Activity)<-[:WAS_GENERATED_BY]-(t:Entity) "
            # The target entity can't be a Lab
            "WHERE e.uuid = $uuid AND e.entity_type <> 'Lab' "
            f"{_activity_query_part} {record_field_name}"
        )

    logger.info("======get_children() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            results = record[record_field_name]

    return results


def build_parameterized_map(entity_data_dict):
    parameterized_list = []
    data = {}

    for key, value in entity_data_dict.items():
        if isinstance(value, (str, int, bool)):
            # Special case is the value is 'TIMESTAMP()' string
            # Remove the quotes since neo4j only takes TIMESTAMP() as a function
            if value == "TIMESTAMP()":
                parameterized_list.append(f"{key}: {value}")
            else:
                parameterized_list.append(f"{key}: ${key}")
                data[key] = value

        else:
            parameterized_list.append(f"{key}: ${key}")
            data[key] = json.dumps(value)

    # Example: {uuid: 'eab7fd6911029122d9bbd4d96116db9b', rui_location: 'Joe <info>', lab_tissue_sample_id: 'dadsadsd'}
    # Note: all the keys are not quoted, otherwise Cypher syntax error
    parametered_str = f"{{ {', '.join(parameterized_list)} }}"
    return parametered_str, data


def update_entity(neo4j_driver, entity_type, entity_data_dict, uuid):
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
    parameterized_str, parameterized_data = build_parameterized_map(entity_data_dict)

    query = (
        f"MATCH (e: {entity_type}) "
        "WHERE e.uuid = $uuid "
        f"SET e += {parameterized_str} "
        f"RETURN e AS {record_field_name}"
    )

    logger.info("======update_entity() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query, uuid=uuid, **parameterized_data)
            record = result.single()
            entity_node = record[record_field_name]

            tx.commit()

            entity_dict = node_to_dict(entity_node)

            # logger.info("======update_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() is False:
            logger.info("Failed to commit update_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


def get_siblings(neo4j_driver, uuid, property_key=None):
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
    results = []

    if property_key:
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
            # filter out the Lab entities
            "WHERE e.uuid = $uuid AND parent.entity_type <> 'LAB' "
            "MATCH (sibling:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent) "
            "WHERE sibling <> e "
            # COLLECT() returns a list
            # apoc.coll.toSet() returns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(sibling.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent:Entity) "
            # filter out the Lab entities
            "WHERE e.uuid = $uuid AND parent.entity_type <> 'LAB' "
            "MATCH (sibling:Entity)-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(parent) "
            "WHERE sibling <> e "
            # COLLECT() returns a list
            # apoc.coll.toSet() returns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(sibling)) AS {record_field_name}"
        )

    logger.info("======get_siblings() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


def get_tuplets(neo4j_driver, uuid, property_key=None):
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
    results = []

    if property_key:
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(parent:Entity) "
            # filter out the Lab entities
            "WHERE e.uuid = $uuid AND parent.entity_type <> 'Lab' "
            "MATCH (tuplet:Entity)-[:WAS_GENERATED_BY]->(a) "
            "WHERE tuplet <> e "
            # COLLECT() returns a list
            # apoc.coll.toSet() returns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(tuplet.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (e:Entity)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED]->(parent:Entity) "
            # filter out the Lab entities
            "WHERE e.uuid = $uuid AND parent.entity_type <> 'Lab' "
            "MATCH (tuplet:Entity)-[:WAS_GENERATED_BY]->(a) "
            "WHERE tuplet <> e "
            # COLLECT() returns a list
            # apoc.coll.toSet() returns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(tuplet)) AS {record_field_name}"
        )

    logger.info("======get_tuplets() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


def get_collections(neo4j_driver, uuid, property_key=None):
    """
    Get all collections by for a given entity uuid

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
    results = []

    if property_key:
        query = (
            "MATCH (c:Collection)<-[:IN_COLLECTION]-(e:Entity) "
            "WHERE e.uuid = $uuid "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (c:Collection)<-[:IN_COLLECTION]-(e:Entity) "
            "WHERE e.uuid = $uuid "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}"
        )

    logger.info("======get_collections() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


def get_uploads(neo4j_driver, uuid, property_key=None):
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
    results = []
    if property_key:
        query = (
            "MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
            "WHERE ds.uuid = $uuid "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(u.{property_key})) AS {record_field_name}"
        )
    else:
        query = (
            "MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
            "WHERE ds.uuid = $uuid "
            # COLLECT() returns a list
            # apoc.coll.toSet() reruns a set containing unique nodes
            f"RETURN apoc.coll.toSet(COLLECT(u)) AS {record_field_name}"
        )

    logger.info("======get_uploads() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query, uuid=uuid)
        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


def get_primary_dataset(neo4j_driver, uuid, property_key=None):
    """
   Get the primary dataset for a given component or derived dataset
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
          A dictionary of entity details returned from the Cypher query
    """
    result = {}

    query = (
        "MATCH (dd:Dataset)-[:WAS_GENERATED_BY|USED*]->(d:Dataset)-[:WAS_GENERATED_BY]-(a:Activity) "
        "WHERE dd.uuid = $uuid AND a.creation_action = 'Create Dataset Activity' "
        f"RETURN d AS {record_field_name}"
    )

    logger.info("======get_primary_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query, uuid=uuid)

        if record and record[record_field_name]:
            # Convert the node to a dict
            result = _node_to_dict(record[record_field_name])

    return result


def get_sources_associated_entity(neo4j_driver, uuid, filter_out=None):
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
    results = []

    query_filter = ""
    if filter_out is not None:
        query_filter = " and not t.uuid in $filter_out"

    _activity_query_part = activity_query_part(for_all_match=True)
    query = (
        "MATCH (e:Entity)-[*]->(t:Source) "
        f"WHERE e.uuid = $uuid {query_filter} "
        f"{_activity_query_part} {record_field_name}"
    )

    logger.info("=====get_sources_associated_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(
            execute_readonly_tx, query, uuid=uuid, filter_out=filter_out
        )

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            results = record[record_field_name]

        for result in results:
            if "metadata" in result and result["metadata"] != "{}":
                result["metadata"] = json.loads(result["metadata"])
            else:
                result.pop("metadata", None)

    return results


def activity_query_part(properties=None, for_all_match=False, only_map_part=False):
    """
    Builds activity query part(s) for grabbing properties like protocol_url from Activity

    Parameters
    ----------
    properties : PropertyGroups
        The properties that will be used to build additional query parts
    for_all_match : bool
        Whether to return a query part used for grabbing the entire node's properties
    only_map_part : bool
        whether to return just the part that creates the map of the activity properties

    Returns
    -------
    Union[str, tuple[str,str,str]]
        A string if using a grab all query, OR
        tuple for exclude_include_query_part with [0] Additional MATCH, [1] map pair query parts for apoc.map.fromPairs, [2] and 'a' variable to use in WITH statements
    """

    query_match_part = "MATCH (e2:Entity)-[:WAS_GENERATED_BY]->(a:Activity) WHERE e2.uuid = t.uuid"

    build_part = " WITH t, apoc.map.fromPairs([['protocol_url', a.protocol_url], ['creation_action', a.creation_action]]) as a2 WITH apoc.map.merge(t,a2) as x RETURN apoc.coll.toSet(COLLECT(x)) AS "

    if only_map_part:
        return build_part

    if for_all_match:
        query_match_part = query_match_part + build_part
        return query_match_part

    def _query_grab_part(_properties, grab_part):
        for p in _properties:
            val_part = f"a.{p}"

            if p in properties.activity_json:
                val_part = f"apoc.convert.fromJsonMap({val_part})"
            elif p in properties.activity_list:
                val_part = f"apoc.convert.fromJsonList({val_part})"

            name_part = p
            # handle name collision for activity and entity
            use_activity_value_count = len(
                schema_manager.get_schema_properties()
                .get(p, {})
                .get("use_activity_value_if_null", [])
            )
            if p in (properties.neo4j + properties.dependency):
                if use_activity_value_count <= 0:
                    name_part = f"activity_{p}"
                if use_activity_value_count > 0:
                    val_part = f"(case when t.{p} is not null then {val_part.replace('a.', 't.')} else {val_part} end)"

            grab_part = grab_part + f", ['{name_part}', {val_part}]"

        return grab_part

    if isinstance(properties, PropertyGroups) and (
        len(properties.activity_neo4j + properties.activity_dep) > 0
    ):
        query_grab_part = ""
        if len(properties.activity_neo4j) > 0:
            query_grab_part = _query_grab_part(properties.activity_neo4j, query_grab_part)
        if len(properties.activity_dep) > 0:
            query_grab_part = _query_grab_part(properties.activity_dep, query_grab_part)

        return query_match_part, query_grab_part, ", a"

    else:
        return "", "", ""


def property_type_query_part(properties: PropertyGroups, is_include_action=True):
    """
    Builds property type query part(s) for parsing properties of certain types

    Parameters
    ----------
    properties : PropertyGroups
        The properties that will be used to build additional query parts
    is_include_action : bool
        whether to include or exclude the listed properties

    Returns
    -------
    str
        The query part(s) for concatenation into apoc.map.fromPairs method
    """
    if is_include_action is False:
        return ""

    map_parts = ""

    for j in properties.json:
        map_parts = map_parts + f", ['{j}', apoc.convert.fromJsonMap(t.{j})]"

    for l in properties.list:
        map_parts = map_parts + f", ['{l}', apoc.convert.fromJsonList(t.{l})]"

    return map_parts


def build_additional_query_parts(properties: PropertyGroups, is_include_action=True):
    """
    Builds additional query parts to be concatenated with other query

    Parameters
    ----------
    properties : PropertyGroups
        The properties that will be used to build additional query parts
    is_include_action : bool
        whether to include or exclude the listed properties

    Returns
    -------
    tuple[str,str,str]
        [0] Additional MATCH, [1] map pair query parts for apoc.map.fromPairs, [2] and variables to use in WITH statements
    """
    _activity_query_part = activity_query_part(properties if is_include_action else None)
    _property_type_query_part = property_type_query_part(properties, is_include_action)

    return (
        _activity_query_part[0],
        _activity_query_part[1] + _property_type_query_part,
        _activity_query_part[2],
    )


def exclude_include_query_part(
    properties: Union[PropertyGroups, List[str]], is_include_action=True, target_entity_type="Any"
):
    """
    Builds a cypher query part that can be used to include or exclude certain properties.
    The preceding MATCH query part should have a label 't'. E.g. MATCH (t:Entity)-[*]->(s:Source)

    Parameters
    ----------
    properties : Union[PropertyGroups, List[str]]
        the properties to be filtered
    is_include_action : bool
        whether to include or exclude the listed properties
    target_entity_type : str
        the entity type that's the target being filtered

    Returns
    -------
    str
        the inclusion exclusion query part to be applied with a MATCH query part
    """
    if isinstance(properties, PropertyGroups):
        _properties = properties.neo4j + properties.dependency
    else:
        _properties = properties

    if _properties is None:
        return f"RETURN apoc.coll.toSet(COLLECT(t)) AS {record_field_name}"

    if is_include_action and len(_properties) == 1 and _properties[0] in ["uuid"]:
        return f"RETURN apoc.coll.toSet(COLLECT(t.{_properties[0]})) AS {record_field_name}"

    action = ""
    if is_include_action is False:
        action = "NOT"

    schema.schema_manager.get_schema_defaults(_properties, is_include_action, target_entity_type)
    more_to_grab_query_part = (
        build_additional_query_parts(properties, is_include_action)
        if isinstance(properties, PropertyGroups)
        else None
    )
    a = more_to_grab_query_part[2] if isinstance(more_to_grab_query_part, tuple) else ""
    map_pairs_part = (
        more_to_grab_query_part[1] if isinstance(more_to_grab_query_part, tuple) else ""
    )
    match_part = more_to_grab_query_part[0] if isinstance(more_to_grab_query_part, tuple) else ""

    # unwind the keys of the results from target/t
    query_part = (
        f"WITH keys(t) AS k1, t{a} unwind k1 AS k2 "
        # filter by a list[] of properties
        f"WITH t{a}, k2 WHERE {action} k2 IN {_properties} "
        # everything is unwinded as separate rows, so let's build it back up by uuid to form: {prop: val, uuid:uuidVal}
        f"WITH t{a}, apoc.map.fromPairs([[k2, t[k2]], ['uuid', t.uuid]{map_pairs_part}]) AS dict "
        # collect all these individual dicts as a list[], and then group them by uuids,
        # which forms a dict with uuid as keys and list of dicts as values:
        # {uuidVal: [{prop: val, uuid:uuidVal}, {prop2: val2, uuid:uuidVal}, ... {propN: valN, uuid:uuidVal}], uuidVal2: [...]}
        "WITH collect(dict) as list WITH apoc.map.groupByMulti(list, 'uuid') AS groups "
        # use the keys of groups dict, and unwind to get uuids as individual rows
        "unwind keys(groups) AS uuids "
        # now merge these individual dicts under their respective uuid
        "WITH apoc.map.mergeList(groups[uuids]) AS rows "
        # collect each row to form a list[] and return
        f"RETURN collect(rows) AS {record_field_name}"
    )

    return f"{match_part} {query_part}"

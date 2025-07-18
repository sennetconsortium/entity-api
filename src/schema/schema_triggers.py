import json
import urllib.parse
from typing import List, Optional
from flask import current_app

import logging
from datetime import datetime, timezone
import requests
from atlas_consortia_commons.string import equals
from neo4j.exceptions import TransactionError
import re

# Local modules
import app_neo4j_queries
from lib import github
from lib.exceptions import create_trigger_error_msg
from lib.ontology import Ontology
from lib.title import (
    generate_title,
    get_source_data_phrase,
    get_attributes_from_source_metadata,
    make_phrase_from_separator_delineated_str,
)
from schema import schema_manager
from schema import schema_errors
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants

logger = logging.getLogger(__name__)

ontology_lookup_cache = {}
sparql_vocabs = {
    "purl.obolibrary.org": "uberon",
    "purl.org": "fma",
}

####################################################################################################
## Trigger methods shared among Collection, Dataset, Source, Sample - DO NOT RENAME
####################################################################################################


def set_timestamp(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of generating current timestamp.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The neo4j TIMESTAMP() function as string
    """
    # Use the neo4j TIMESTAMP() function during entity creation
    # Will be proessed in app_neo4j_queries.build_parameterized_map()
    # and schema_neo4j_queries.build_parameterized_map()
    return property_key, "TIMESTAMP()"


def set_entity_type(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of setting the entity type of a given entity.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The string of normalized entity type
    """
    return property_key, normalized_type


def set_user_sub(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of getting user sub.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The 'sub' string
    """
    if "sub" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'sub' key in 'new_data_dict' during calling 'set_user_sub()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, new_data_dict["sub"]


def set_user_email(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of getting user email.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The 'email' string
    """
    if "email" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'email' key in 'new_data_dict' during calling 'set_user_email()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, new_data_dict["email"]


def set_user_displayname(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting user name.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The 'name' string
    """
    if "name" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'name' key in 'new_data_dict' during calling 'set_user_displayname()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, new_data_dict["name"]


def set_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of getting uuid, hubmap_id for a new entity to be created.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The uuid created via uuid-api
    """
    if "uuid" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'new_data_dict' during calling 'set_uuid()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, new_data_dict["uuid"]


def set_sennet_id(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of getting uuid, hubmap_id for a new entity to be created.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The sennet_id/sennet_id created via uuid-api
    """
    if "sennet_id" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'sennet_id' key in 'new_data_dict' during calling 'set_sennet_id()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, new_data_dict["sennet_id"]


####################################################################################################
## Trigger methods shared by Sample, Source, Dataset - DO NOT RENAME
####################################################################################################


def set_data_access_level(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of generating data access level.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the entity types defined in the schema yaml: Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The data access level string
    """
    if "uuid" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if normalized_type in ["Dataset", "Publication"]:
        # 'contains_human_genetic_sequences' is required on create
        if "contains_human_genetic_sequences" not in new_data_dict:
            msg = create_trigger_error_msg(
                "Missing 'contains_human_genetic_sequences' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)

        # Default to protected
        data_access_level = SchemaConstants.ACCESS_LEVEL_PROTECTED

        # When `contains_human_genetic_sequences` is true, even if `status` is 'Published',
        # the `data_access_level` is still 'protected'
        if new_data_dict["contains_human_genetic_sequences"]:
            data_access_level = SchemaConstants.ACCESS_LEVEL_PROTECTED
        else:
            # When creating a new dataset, status should always be "New"
            # Thus we don't use Dataset.status == "Published" to determine the data_access_level as public
            data_access_level = SchemaConstants.ACCESS_LEVEL_CONSORTIUM
    else:
        # Default to consortium for Source/Sample
        data_access_level = SchemaConstants.ACCESS_LEVEL_CONSORTIUM

        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = schema_neo4j_queries.count_attached_published_datasets(
            schema_manager.get_neo4j_driver_instance(), normalized_type, new_data_dict["uuid"]
        )

        if count > 0:
            data_access_level = SchemaConstants.ACCESS_LEVEL_PUBLIC

    return property_key, data_access_level


def set_group_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of setting the group_uuid.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The group uuid
    """
    group_uuid = None

    # Look for membership in a single "data provider" group and sets to that.
    # Otherwise if not set and no single "provider group" membership throws error.
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if "hmgroupids" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'hmgroupids' key in 'new_data_dict' during calling 'set_group_uuid()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    user_group_uuids = new_data_dict["hmgroupids"]

    # If group_uuid provided from incoming request, validate it
    if "group_uuid" in new_data_dict:
        # A bit validation
        try:
            schema_manager.validate_entity_group_uuid(new_data_dict["group_uuid"], user_group_uuids)
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)
        except schema_errors.UnmatchedDataProviderGroupException as e:
            raise schema_errors.UnmatchedDataProviderGroupException(e)

        group_uuid = new_data_dict["group_uuid"]
    # When no group_uuid provided
    else:
        try:
            group_info = schema_manager.get_entity_group_info(user_group_uuids)
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)
        except schema_errors.MultipleDataProviderGroupException as e:
            # No need to log
            raise schema_errors.MultipleDataProviderGroupException(e)

        group_uuid = group_info["uuid"]

    return property_key, group_uuid


def set_group_name(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of setting the group_name.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The group name
    """
    group_name = None

    # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that.
    # Otherwise if not set and no single "provider group" membership throws error.
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if "hmgroupids" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'hmgroupids' key in 'new_data_dict' during calling 'set_group_name()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    try:
        default_group_uuid = None
        if "group_uuid" in new_data_dict:
            default_group_uuid = new_data_dict["group_uuid"]
        group_info = schema_manager.get_entity_group_info(
            new_data_dict["hmgroupids"], default_group_uuid
        )
        group_name = group_info["name"]
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)
    except schema_errors.MultipleDataProviderGroupException as e:
        # No need to log
        raise schema_errors.MultipleDataProviderGroupException(e)

    return property_key, group_name


####################################################################################################
## Trigger methods shared by Source and Sample - DO NOT RENAME
####################################################################################################


def commit_image_files(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file.

    The information, filename and optional description is saved in the field with name specified by `target_property_key`
    in the provided data_dict.  The image files needed to be previously uploaded
    using the temp file service (UploadFileHelper.save_file).  The temp file id provided
    from UploadFileHelper, paired with an optional description of the file must be provided
    in the field `image_files_to_add` in the data_dict for each file being committed
    in a JSON array like below ("description" is optional):

    [
      {
        "temp_file_id": "eiaja823jafd",
        "description": "Image file 1"
      },
      {
        "temp_file_id": "pd34hu4spb3lk43usdr"
      },
      {
        "temp_file_id": "32kafoiw4fbazd",
        "description": "Image file 3"
      }
    ]

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The file info dicts in a list
    """
    return _commit_files(
        "image_files",
        property_key,
        normalized_type,
        user_token,
        existing_data_dict,
        new_data_dict,
        generated_dict,
    )


def delete_image_files(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event methods for removing files from an entity during update.

    Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
    The files to remove are specified as file uuids in the `property_key` field

    The two outer methods (delete_image_files and delete_metadata_files) pass the target property
    field name to private method, _delete_files along with the other required trigger properties

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The file info dicts in a list
    """
    return _delete_files(
        "image_files",
        property_key,
        normalized_type,
        user_token,
        existing_data_dict,
        new_data_dict,
        generated_dict,
    )


def update_file_descriptions(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event method to ONLY update descriptions of existing files.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The file info dicts (with updated descriptions) in a list
    """
    if property_key not in new_data_dict:
        msg = create_trigger_error_msg(
            f"Missing '{property_key}' key in 'new_data_dict' during calling 'update_file_descriptions()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # If POST or PUT where the target doesn't exist create the file info array
    # if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    # or if it doesn't exist in existing_data_dict create it
    if not property_key in generated_dict:
        if not property_key in existing_data_dict:
            msg = create_trigger_error_msg(
                f"Missing '{property_key}' key in 'existing_data_dict' during call to 'update_file_descriptions()' trigger method.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)
        # Otherwise this is a PUT where the target array exists already
        else:
            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string
            # due to the way that Cypher handles single/double quotes.
            existing_files_list = schema_manager.get_as_dict(existing_data_dict[property_key])
    else:
        if not property_key in generated_dict:
            msg = create_trigger_error_msg(
                f"Missing '{property_key}' key in 'generated_dict' during call to 'update_file_descriptions()' trigger method.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)

        existing_files_list = generated_dict[property_key]

    file_info_by_uuid_dict = {}

    for file_info in existing_files_list:
        file_uuid = file_info["file_uuid"]

        file_info_by_uuid_dict[file_uuid] = file_info

    for file_info in new_data_dict[property_key]:
        file_uuid = file_info["file_uuid"]

        # Existence check in case the file uuid gets edited in the request
        if file_uuid in file_info_by_uuid_dict:
            # Keep filename and file_uuid unchanged
            # Only update the description
            file_info_by_uuid_dict[file_uuid]["description"] = file_info["description"]

    generated_dict[property_key] = list(file_info_by_uuid_dict.values())
    return generated_dict


####################################################################################################
## Trigger methods specific to Collection - DO NOT RENAME
####################################################################################################


def get_collection_entities(
    property_key: str,
    normalized_type: str,
    user_token: str,
    existing_data_dict: dict,
    new_data_dict: dict,
):
    """Trigger event method of getting a list of associated datasets for a given collection.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: A list of associated dataset dicts with all the normalized information
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_collection_entities()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Additional properties of the datasets to exclude
    # We don't want to show too much nested information
    properties_to_skip = [
        "antibodies",
        "collections",
        "contacts",
        "contributors",
        "direct_ancestors",
        "ingest_metadata" "next_revision_uuid",
        "pipeline_message",
        "previous_revision_uuid",
        "sources",
        "status_history",
        "title",
        "upload",
    ]
    collection_entities = get_normalized_collection_entities(
        existing_data_dict["uuid"], user_token, properties_to_exclude=properties_to_skip
    )
    return property_key, collection_entities


def get_normalized_collection_entities(
    uuid: str, token: str, skip_completion: bool = False, properties_to_exclude: List[str] = []
):
    """Query the Neo4j database to get the associated entities for a given Collection UUID and normalize the results.

    Parameters
    ----------
    uuid : str
        The UUID of the Collection entity
    token: str
        The user's globus nexus token or internal token
    skip_completion : bool
        Skip the call to get_complete_entities_list, default is False
    properties_to_exclude : List[str]
        A list of property keys to exclude from the normalized results, default is []


    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: A list of associated entity dicts with all the normalized information
    """
    db = schema_manager.get_neo4j_driver_instance()
    entities_list = schema_neo4j_queries.get_collection_entities(db, uuid)

    if skip_completion:
        complete_entities_list = entities_list
    else:
        complete_entities_list = schema_manager.get_complete_entities_list(
            token=token, entities_list=entities_list, properties_to_filter=properties_to_exclude
        )

    return schema_manager.normalize_entities_list_for_response(
        entities_list=complete_entities_list,
        property_groups=schema_manager.group_verify_properties_list(
            properties=properties_to_exclude
        ),
        is_include_action=False,
    )


def get_publication_associated_collection(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the associated collection for this publication.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: A dictionary representation of the associated collection with all the normalized information
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_publication_associated_collection()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    logger.info(
        f"Executing 'get_publication_associated_collection()' trigger method on uuid: {existing_data_dict['uuid']}"
    )

    collection_dict = schema_neo4j_queries.get_publication_associated_collection(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_object_result_for_response(
        entity_dict=collection_dict
    )


def link_publication_to_associated_collection(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of creating or recreating linkages between this new publication and its associated_collection.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Publication
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_publication_to_associated_collection()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "associated_collection_uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'associated_collection_uuid' key in 'existing_data_dict' during calling 'link_publication_to_associated_collection()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    associated_collection_uuid = existing_data_dict["associated_collection_uuid"]

    # No activity node. We are creating a direct link to the associated collection

    try:
        # Create a linkage
        # between the Publication node and the Collection node in neo4j
        schema_neo4j_queries.link_publication_to_associated_collection(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            associated_collection_uuid,
        )

        # Will need to delete the collection cache if later we add `Collection.associated_publications` field - 7/16/2023 Zhou
    except TransactionError:
        # No need to log
        raise


####################################################################################################
## Trigger methods specific to Dataset - DO NOT RENAME
####################################################################################################


def set_dataset_status_new(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of setting the default "New" status for this new Dataset.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: Initial status of "New"
    """
    # Always 'New' on dataset creation
    return property_key, "New"


def get_entity_collections(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting a list of collections for this new Dataset.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: A list of associated collections with all the normalized information
    """
    return_list = None

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_entity_collections()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # No property key needs to filter the result
    # Get back the list of collection dicts
    collections_list = schema_neo4j_queries.get_entity_collections(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )
    if collections_list:
        # Exclude datasets from each resulting collection
        # We don't want to show too much nested information
        properties_to_skip = ["entities"]
        complete_entities_list = schema_manager.get_complete_entities_list(
            user_token, collections_list, properties_to_skip
        )
        return_list = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return property_key, return_list


def get_dataset_upload(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the associated Upload for this Dataset.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: A dict of associated Upload detail with all the normalized information
    """
    return_dict = None

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_upload()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # It could be None if the dataset doesn't in any Upload
    upload_dict = schema_neo4j_queries.get_dataset_upload(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    if upload_dict:
        # Exclude datasets from each resulting Upload
        # We don't want to show too much nested information
        properties_to_skip = ["datasets"]
        complete_upload_dict = schema_manager.get_complete_entity_result(
            user_token, upload_dict, properties_to_skip
        )
        return_dict = schema_manager.normalize_object_result_for_response(
            "ENTITIES", complete_upload_dict
        )

    return property_key, return_dict


def link_collection_to_entities(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for creating or recreating linkages between this new Collection and the Datasets it contains.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_collection_to_entities()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "entity_uuids" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'entity_uuids' key in 'existing_data_dict' during calling 'link_collection_to_entities()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    entity_uuids = existing_data_dict["entity_uuids"]

    try:
        # Create a linkage (without an Activity node) between the Collection node and each Entity it contains.
        schema_neo4j_queries.link_collection_to_entities(
            neo4j_driver=schema_manager.get_neo4j_driver_instance(),
            collection_uuid=existing_data_dict["uuid"],
            entities_uuid_list=entity_uuids,
        )

        # Delete the cache of each associated dataset and the collection itself if any cache exists
        # Because the `Dataset.collecctions` field and `Collection.datasets` field
        uuids_list = [existing_data_dict["uuid"]] + entity_uuids
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError:
        # No need to log
        raise


def get_dataset_direct_ancestors(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting direct ancestors.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: A list of associated direct ancestors with all the normalized information
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # No property key needs to filter the result
    # Get back the list of ancestor dicts
    driver = schema_manager.get_neo4j_driver_instance()
    direct_ancestors_list = schema_neo4j_queries.get_dataset_direct_ancestors(
        driver, existing_data_dict["uuid"]
    )

    # We don't want to show too much nested information
    # The direct ancestor of a Dataset could be: Dataset or Sample
    # Skip running the trigger methods for 'direct_ancestors' and 'collections' if the direct ancestor is Dataset
    # Skip running the trigger methods for 'direct_ancestor' if the direct ancestor is Sample
    properties_to_skip = ["direct_ancestors", "collections", "direct_ancestor"]
    complete_entities_list = schema_manager.get_complete_entities_list(
        user_token, direct_ancestors_list, properties_to_skip
    )

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)


def get_sample_section_descendant_datasets(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for getting the descendant datasets for a sample section.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
    str: The target property key
    list: A list of descendant datasets
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_section_descendant_datasets()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Check if the entity is a Sample Section, skip otherwise
    if equals(Ontology.ops().entities().SAMPLE, normalized_type):
        if equals(
            Ontology.ops().specimen_categories().SECTION, existing_data_dict["sample_category"]
        ):
            driver = schema_manager.get_neo4j_driver_instance()
            uuid = existing_data_dict["uuid"]

            # HRA EUI only requires the 'dataset_type' field
            properties = ["uuid", "dataset_type"]
            datasets = app_neo4j_queries.get_descendants_by_type(
                driver, uuid, Ontology.ops().entities().DATASET, properties
            )
            return property_key, datasets

    return property_key, None


def get_ancestor_blocks(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for getting the ancestor blocks for a sample section or dataset.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
    str: The target property key
    list: A list of ancestor blocks
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_ancestor_blocks()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Check if the entity is a sample section or dataset, skip otherwise
    entity_types = Ontology.ops().entities()
    sample_categories = Ontology.ops().specimen_categories()
    if (
        equals(entity_types.SAMPLE, normalized_type)
        and equals(sample_categories.SECTION, existing_data_dict["sample_category"])
    ) or equals(entity_types.DATASET, normalized_type):
        driver = schema_manager.get_neo4j_driver_instance()
        uuid = existing_data_dict["uuid"]

        # HRA EUI only requires the 'rui_location' field
        properties = ["uuid", "rui_location"]
        blocks = app_neo4j_queries.get_ancestors_by_type(
            driver, uuid, Ontology.ops().specimen_categories().BLOCK, properties
        )
        blocks = [b for b in blocks if b.get("rui_location") is not None]
        if len(blocks) > 0:
            return property_key, blocks

    return property_key, None


def get_sample_section_ancestor_ids(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for getting the ancestor ids for a sample section.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
    str: The target property key
    list: A list of ancestor ids
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_section_ancestor_blocks()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Check if the entity is a Sample Section, skip otherwise
    if equals(Ontology.ops().entities().SAMPLE, normalized_type):
        if equals(
            Ontology.ops().specimen_categories().SECTION, existing_data_dict["sample_category"]
        ):
            driver = schema_manager.get_neo4j_driver_instance()
            uuid = existing_data_dict["uuid"]
            ancestor_ids = app_neo4j_queries.get_ancestors(
                driver, uuid, properties=["uuid"], is_include_action=True
            )
            if len(ancestor_ids) > 0:
                return property_key, ancestor_ids

    return property_key, None


def get_source_samples(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for getting the samples immediately connected to a given dataset.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
    str: The target property key
    list: A list of source samples
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_section_ancestor_blocks()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    driver = schema_manager.get_neo4j_driver_instance()
    uuid = existing_data_dict["uuid"]
    properties = ["uuid", "sample_category"]
    samples = app_neo4j_queries.get_source_samples(driver, uuid, properties)
    if len(samples) < 1:
        return property_key, None

    return property_key, samples


def remove_fields(d, properties_to_keep):
    return {key: value for key, value in d.items() if key in properties_to_keep}


def get_local_directory_rel_path(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the relative directory path of a given dataset.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The relative directory path
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_local_directory_rel_path()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "data_access_level" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'data_access_level' key in 'existing_data_dict' during calling 'get_local_directory_rel_path()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    uuid = existing_data_dict["uuid"]

    if (not "group_uuid" in existing_data_dict) or (not existing_data_dict["group_uuid"]):
        msg = create_trigger_error_msg(
            "Group uuid not set for dataset during calling 'get_local_directory_rel_path()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Validate the group_uuid and make sure it's one of the valid data providers
    try:
        schema_manager.validate_entity_group_uuid(existing_data_dict["group_uuid"])
    except schema_errors.NoDataProviderGroupException:
        # No need to log
        raise

    group_name = schema_manager.get_entity_group_name(existing_data_dict["group_uuid"])

    dir_path = existing_data_dict["data_access_level"] + "/" + group_name + "/" + uuid + "/"

    return property_key, dir_path


def link_to_previous_revisions(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage from this new Dataset to the dataset of its previous revision.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_to_previous_revision()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "previous_revision_uuids" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'previous_revision_uuids' key in 'existing_data_dict' during calling 'link_to_previous_revision()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Create a revision reltionship from this new Dataset node and its previous revision of dataset node in neo4j
    try:
        schema_neo4j_queries.link_entity_to_previous_revision(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            existing_data_dict["previous_revision_uuids"],
        )
    except TransactionError:
        # No need to log
        raise


def link_to_previous_revision(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage from this new Dataset to the dataset of its previous revision.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_to_previous_revision()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "previous_revision_uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'previous_revision_uuid' key in 'existing_data_dict' during calling 'link_to_previous_revision()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Create a revision reltionship from this new Dataset node and its previous revision of dataset node in neo4j
    try:
        schema_neo4j_queries.link_entity_to_previous_revision(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            existing_data_dict["previous_revision_uuid"],
        )
    except TransactionError:
        # No need to log
        raise


def get_has_metadata(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method for determining if the entity has metadata.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: "True" or "False" if the entity has metadata
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_has_metadata()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if equals(Ontology.ops().entities().DATASET, existing_data_dict["entity_type"]):
        metadata = existing_data_dict.get("metadata")
        return property_key, str(metadata is not None)

    SpecimenCategories = Ontology.ops().specimen_categories()
    if (
        equals(Ontology.ops().entities().SOURCE, existing_data_dict["entity_type"])
        or equals("Collection", existing_data_dict["entity_type"])
        or equals("Publication", existing_data_dict["entity_type"])
        or equals(SpecimenCategories.BLOCK, existing_data_dict.get("sample_category"))
        or equals(SpecimenCategories.SECTION, existing_data_dict.get("sample_category"))
        or equals(SpecimenCategories.SUSPENSION, existing_data_dict.get("sample_category"))
    ):
        has_metadata = "metadata" in existing_data_dict
        return property_key, str(has_metadata)

    return property_key, None


def get_source_mapped_metadata(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of auto generating mapped metadata from 'living_donor_data' or 'organ_donor_data'.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: The auto generated mapped metadata
    """
    if not equals(Ontology.ops().source_types().HUMAN, existing_data_dict["source_type"]):
        return property_key, None
    if "metadata" not in existing_data_dict or existing_data_dict["metadata"] is None:
        return property_key, None

    if (
        "organ_donor_data" not in existing_data_dict["metadata"]
        and "living_donor_data" not in existing_data_dict["metadata"]
    ):
        msg = create_trigger_error_msg(
            "Missing 'organ_donor_data' or 'living_donor_data' key in 'existing_data_dict[metadata]' during calling 'get_source_mapped_metadata()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise schema_errors.InvalidPropertyRequirementsException(msg)

    if not isinstance(existing_data_dict["metadata"], dict):
        metadata = json.loads(existing_data_dict["metadata"].replace("'", '"'))
    else:
        metadata = existing_data_dict["metadata"]

    donor_metadata = metadata.get("organ_donor_data") or metadata.get("living_donor_data") or {}

    mapped_metadata = {}
    for kv in donor_metadata:
        term = kv["grouping_concept_preferred_term"]
        key = re.sub(r"\W+", "_", term).lower()
        value = (
            float(kv["data_value"]) if kv.get("data_type") == "Numeric" else kv["preferred_term"]
        )

        if key not in mapped_metadata:
            mapped_metadata_item = {
                "value": [value],
                "unit": kv.get("units", ""),
                "key_display": term,
                "value_display": source_metadata_display_value(kv),
                "group_display": source_metadata_group(key),
            }
            mapped_metadata[key] = mapped_metadata_item
        else:
            mapped_metadata[key]["value"].append(value)
            mapped_metadata[key]["value_display"] += f", {value}"

    return property_key, mapped_metadata


def get_cedar_mapped_metadata(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of auto generating sample mapped metadata.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: The auto generated mapped metadata
    """
    # No human sources
    if equals(Ontology.ops().source_types().HUMAN, existing_data_dict.get("source_type")):
        return property_key, None

    # For mouse sources, all samples, and all datasets
    if "metadata" not in existing_data_dict or existing_data_dict["metadata"] is None:
        return property_key, None
    if not isinstance(existing_data_dict["metadata"], dict):
        metadata = json.loads(existing_data_dict["metadata"])
    else:
        metadata = existing_data_dict["metadata"]

    mapped_metadata = {}
    try:
        for k, v in metadata.items():
            suffix = None
            parts = [_normalize(word) for word in k.split("_")]
            if parts[-1] == "Value" or parts[-1] == "Unit":
                suffix = parts.pop()

            new_key = " ".join(parts)
            if new_key not in mapped_metadata:
                mapped_metadata[new_key] = v
            else:
                curr_val = str(mapped_metadata[new_key])
                if len(curr_val) < 1:
                    # Prevent space at the beginning if the value is empty
                    mapped_metadata[new_key] = v
                    continue
                if suffix == "Value":
                    mapped_metadata[new_key] = f"{v} {curr_val}"
                if suffix == "Unit":
                    mapped_metadata[new_key] = f"{curr_val} {v}"
    except Exception as e:
        msg = f"Failed to call the trigger method: get_cedar_mapped_metadata {existing_data_dict['uuid']}"
        logger.exception(f"{msg} {str(e)}")
        mapped_metadata["Error"] = (
            "This metadata may be incomplete. If you continue to see this error message, please contact the SenNet Help Desk help@sennetconsortium.org."
        )
        return property_key, mapped_metadata

    return property_key, mapped_metadata


_normalized_words = {
    "rnaseq": "RNAseq",
    "phix": "PhiX",
    "id": "ID",
    "doi": "DOI",
    "io": "IO",
    "pi": "PI",
    "dna": "DNA",
    "rna": "RNA",
    "sc": "SC",
    "pcr": "PCR",
    "umi": "UMI",
}


def _normalize(word: str):
    """Normalize the word. Specific words should be capitalized differently.

    Parameters
    ----------
    word : str
        The word to normalize

    Returns
    -------
    str: The normalized word
    """
    if word in _normalized_words:
        return _normalized_words[word]
    return word.capitalize()


def get_dataset_title(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of auto generating the dataset title.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The generated dataset title
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_title()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    MAX_ENTITY_LIST_LENGTH = 5

    # Statistically improbable phrase to separate items while building a phrase, which can be
    # replaced by a grammatically correct separator like the word 'and' or a comma later
    ITEM_SEPARATOR_SIP = "_-_-_-ENTITY_SEPARATOR-_-_-_"

    logger.info(
        f"Executing 'get_dataset_title()' trigger method on uuid: {existing_data_dict['uuid']}"
    )

    # Assume organ_desc is always available, otherwise will throw parsing error
    organ_desc = "<organ_desc>"

    dataset_type = existing_data_dict["dataset_type"]

    # Get the sample organ name and source metadata information of this dataset
    source_organs_list = schema_neo4j_queries.get_dataset_source_organs_info(
        neo4j_driver=schema_manager.get_neo4j_driver_instance(),
        dataset_uuid=existing_data_dict["uuid"],
    )

    # Determine the number of unique organ types and the number of unique sources in
    # source_organs_list so the format of the title to be created can be determined.
    organ_abbrev_set = set()
    source_metadata_list = list()
    source_uuid_set = set()
    source_type = None
    for source_organ_data in source_organs_list:
        organ_abbrev_set.add(source_organ_data["organ_type"])
        source_metadata_list.append(source_organ_data["source_metadata"])
        source_uuid_set.add(source_organ_data["source_uuid"])
        source_type = source_organ_data["source_type"]

    if source_type is None:
        # This should never happen, but just in case
        raise Exception(f"Unable to determine source type for dataset {existing_data_dict['uuid']}")

    # If the number of unique organ types is no more than MAX_ENTITY_LIST_LENGTH, we need to come up
    # with a phrase to be used to create the title which describes them. If there are more than
    # the threshold, we will just use the number in the title.
    organs_description_phrase = f"{len(organ_abbrev_set)} organs"
    organ_types_dict = Ontology.ops(
        as_data_dict=True, prop_callback=None, key="organ_uberon", val_key="term"
    ).organ_types()

    if len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH:
        organ_description_set = set()
        if organ_abbrev_set:
            for organ_abbrev in organ_abbrev_set:
                # The organ_abbrev is the two-letter code only set for 'organ'
                # Convert the two-letter code to a description
                organ_desc = organ_types_dict[organ_abbrev]
                organ_description_set.add(organ_desc.lower())

        # Turn the set of organ descriptions into a phrase which can be used to compose the Dataset title
        organs_description_phrase = ITEM_SEPARATOR_SIP.join(organ_description_set)
        organs_description_phrase = make_phrase_from_separator_delineated_str(
            separated_phrase=organs_description_phrase, separator=ITEM_SEPARATOR_SIP
        )

    # If the number of unique organ sources is no more than MAX_ENTITY_LIST_LENGTH, we need to come up
    # with a phrase to be used to create the title which describes them.  If there are more than
    # the threshold, we will just use the number in the title.
    # Parse age, race, and sex from the source metadata, but determine the number of sources using source_uuid_set.
    sources_description_phrase = f"{len(source_uuid_set)} sources"
    if len(source_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        sources_grouping_concepts_dict = dict()
        if source_metadata_list:
            for source_metadata in source_metadata_list:
                source_data = get_attributes_from_source_metadata(
                    source_type=source_type, source_metadata=source_metadata
                )
                age_race_sex_info = get_source_data_phrase(
                    source_type=source_type, source_data=source_data
                )
                if age_race_sex_info in sources_grouping_concepts_dict:
                    sources_grouping_concepts_dict[age_race_sex_info] += 1
                else:
                    sources_grouping_concepts_dict[age_race_sex_info] = 1

        sources_description_phrase = ""
        for age_race_sex_info in sources_grouping_concepts_dict.keys():
            if len(sources_grouping_concepts_dict) > 1:
                sources_description_phrase += (
                    f"({sources_grouping_concepts_dict[age_race_sex_info]}) "
                )
            sources_description_phrase += f"{age_race_sex_info}{ITEM_SEPARATOR_SIP}"

        sources_description_phrase = make_phrase_from_separator_delineated_str(
            separated_phrase=sources_description_phrase, separator=ITEM_SEPARATOR_SIP
        )

    # When both the number of unique organ codes is between 2 and MAX_ENTITY_LIST_LENGTH and
    # the number of unique organ sources is between 2 and MAX_ENTITY_LIST_LENGTH, we will
    # use a phrase which associates each organ type and source metadata rather than the
    # phrases previously built.
    source_organ_association_phrase = ""
    if (
        len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH
    ):  # and len(source_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        for source_organ_data in source_organs_list:
            # The organ_abbrev is the two-letter code only set for "organ"
            # Convert the two-letter code to a description
            organ_desc = organ_types_dict[source_organ_data["organ_type"]]
            source_data = get_attributes_from_source_metadata(
                source_type=source_type, source_metadata=source_organ_data["source_metadata"]
            )
            age_race_sex_info = get_source_data_phrase(
                source_type=source_type, source_data=source_data
            )

            source_organ_association_phrase += (
                f"{organ_desc.lower()} of {age_race_sex_info}{ITEM_SEPARATOR_SIP}"
            )

        source_organ_association_phrase = make_phrase_from_separator_delineated_str(
            separated_phrase=source_organ_association_phrase, separator=ITEM_SEPARATOR_SIP
        )

    generated_title = generate_title(
        organ_abbrev_set=organ_abbrev_set,
        source_uuid_set=source_uuid_set,
        dataset_type=dataset_type,
        organs_description_phrase=organs_description_phrase,
        sources_description_phrase=sources_description_phrase,
        source_organ_association_phrase=source_organ_association_phrase,
        max_entity_list_length=MAX_ENTITY_LIST_LENGTH,
    )

    return property_key, generated_title


dataset_category_map = {
    "Create Dataset Activity": "primary",
    "Multi-Assay Split": "component",
    "Central Process": "codcc-processed",
    "Lab Process": "lab-processed",
}


def get_dataset_category(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of auto generating the dataset category.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The generated dataset category
    """
    creation_action = existing_data_dict.get("creation_action")
    if creation_action is None:
        creation_action = dict(
            [
                get_creation_action_activity(
                    "creation_action_activity",
                    normalized_type,
                    user_token,
                    existing_data_dict,
                    new_data_dict,
                )
            ]
        ).get("creation_action_activity")

    if dataset_category := dataset_category_map.get(creation_action):
        return property_key, dataset_category

    return property_key, None


# For Upload, Dataset, Source and Sample objects:
# add a calculated (not stored in Neo4j) field called `display_subtype` to
# all Elasticsearch documents of the above types with the following rules:
# Upload: Just make it "Data Upload" for all uploads
# Source: "Source"
# Sample: if sample_category == 'organ' the display name linked to the corresponding description of organ code
# otherwise the display name linked to the value of the corresponding description of sample_category code
def get_display_subtype(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of generating the display subtype for the entity.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The display subtype
    """
    display_subtype = "{unknown}"

    if equals(Ontology.ops().entities().SOURCE, normalized_type):
        display_subtype = existing_data_dict["source_type"]

    elif equals(Ontology.ops().entities().SAMPLE, normalized_type):
        if "sample_category" in existing_data_dict:
            if equals(
                existing_data_dict["sample_category"], Ontology.ops().specimen_categories().ORGAN
            ):
                if "organ" in existing_data_dict:
                    organ_types = Ontology.ops(
                        as_data_dict=True, prop_callback=None, key="organ_uberon", val_key="term"
                    ).organ_types()
                    display_subtype = get_val_by_key(
                        existing_data_dict["organ"], organ_types, "ubkg.organ_types"
                    )
                else:
                    logger.error(
                        "Missing missing organ when sample_category is set "
                        f"of Sample with uuid: {existing_data_dict['uuid']}"
                    )

            else:
                sample_categories = Ontology.ops(
                    as_data_dict=True, prop_callback=None
                ).specimen_categories()
                display_subtype = get_val_by_key(
                    existing_data_dict["sample_category"],
                    sample_categories,
                    "ubkg.specimen_categories",
                )

        else:
            logger.error(
                f"Missing sample_category of Sample with uuid: {existing_data_dict['uuid']}"
            )

    elif equals(Ontology.ops().entities().DATASET, normalized_type):
        if "dataset_type" in existing_data_dict:
            display_subtype = existing_data_dict["dataset_type"]
        else:
            logger.error(f"Missing dataset_type of Dataset with uuid: {existing_data_dict['uuid']}")

    elif equals(Ontology.ops().entities().UPLOAD, normalized_type):
        display_subtype = "Data Upload"

    else:
        # Do nothing
        logger.error(
            f"Invalid entity_type: {existing_data_dict['entity_type']}. "
            "Only generate display_subtype for Source/Sample/Dataset/Upload"
        )

    return property_key, display_subtype


def get_val_by_key(type_code, data, source_data_name):
    # Use triple {{{}}}
    result_val = f"{{{type_code}}}"

    if type_code in data:
        result_val = data[type_code]
    else:
        # Return the error message as result
        logger.error(f"Missing key {type_code} in {source_data_name}")

    logger.debug(f"======== get_val_by_key: {result_val}")

    return result_val


def get_last_touch(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of when this entity was last modified or published.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The last touch time
    """
    time_stamp = (
        existing_data_dict["published_timestamp"]
        if "published_timestamp" in existing_data_dict
        else existing_data_dict["last_modified_timestamp"]
    )
    timestamp = str(datetime.fromtimestamp(time_stamp / 1000, tz=timezone.utc))
    last_touch = timestamp.split("+")[0]

    return property_key, last_touch


def get_origin_samples(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method to grab the ancestor of this entity where entity type is Sample and the sample_category is Organ.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: The origin sample
    """
    # The origin_sample is the sample that `sample_category` is "organ" and the `organ` code is set at the same time

    try:

        def _get_organ_hierarchy(entity_dict):
            organ_hierarchy_key, organ_hierarchy_value = get_organ_hierarchy(
                property_key="organ_hierarchy",
                normalized_type=Ontology.ops().entities().SAMPLE,
                user_token=user_token,
                existing_data_dict=entity_dict,
                new_data_dict=new_data_dict,
            )
            entity_dict[organ_hierarchy_key] = organ_hierarchy_value

        if equals(
            existing_data_dict.get("sample_category"), Ontology.ops().specimen_categories().ORGAN
        ):
            # Return the organ if this is an organ
            _get_organ_hierarchy(existing_data_dict)
            return property_key, [existing_data_dict]

        origin_samples = None
        if normalized_type in ["Sample", "Dataset", "Publication"]:
            origin_samples = schema_neo4j_queries.get_origin_samples(
                schema_manager.get_neo4j_driver_instance(),
                [existing_data_dict["uuid"]],
                is_bulk=False,
            )

            for origin_sample in origin_samples:
                _get_organ_hierarchy(origin_sample)

        return property_key, origin_samples
    except Exception:
        logger.error(
            f"No origin sample found for {normalized_type} with UUID: {existing_data_dict['uuid']}"
        )
        return property_key, None


def get_has_pipeline_or_validation_message(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method to determine if the dataset has a pipeline message or upload has validation message.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Dataset or Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: "True" or "False" if the dataset has a pipeline message or upload has validation message
    """
    if equals(normalized_type, Ontology.ops().entities().DATASET):
        property = "pipeline_message"
    elif equals(normalized_type, Ontology.ops().entities().UPLOAD):
        property = "validation_message"
    else:
        return property_key, None

    has_msg = property in existing_data_dict and len(existing_data_dict[property]) > 0
    return property_key, str(has_msg)


def get_has_rui_information(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    if normalized_type in ["Sample", "Dataset"]:
        if normalized_type == "Sample":
            if existing_data_dict["sample_category"] == "Block":
                if "rui_location" in existing_data_dict:
                    return property_key, str(True)
                if "rui_exemption" in existing_data_dict and existing_data_dict["rui_exemption"]:
                    return property_key, "Exempt"
            if existing_data_dict["sample_category"] == "Organ":
                return property_key, None

        has_rui_information = schema_neo4j_queries.get_has_rui_information(
            schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
        )
        return property_key, has_rui_information

    return property_key, None


def get_rui_location_anatomical_locations(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method to parse out the anatomical locations from 'rui_location'.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The anatomical locations
    """
    rui_location_anatomical_locations = None
    if "rui_location" in existing_data_dict:
        rui_location = json.loads(existing_data_dict["rui_location"])
        if "ccf_annotations" in rui_location:
            annotation_urls = rui_location["ccf_annotations"]
            labels = [label for url in annotation_urls if (label := _get_ontology_label(url))]
            if len(labels) > 0:
                rui_location_anatomical_locations = labels

    return property_key, rui_location_anatomical_locations


def _get_ontology_label(ann_url: str) -> Optional[str]:
    """Get the label from the appropriate ontology lookup service.

    Parameters
    ----------
        ann_url : str
            The annotation url.

    Returns
    -------
    Optional[dict] : The label and purl if found, otherwise None.
    """
    if ann_url in ontology_lookup_cache:
        return {"label": ontology_lookup_cache[ann_url], "purl": ann_url}

    host = urllib.parse.urlparse(ann_url).hostname
    vocab = sparql_vocabs.get(host)
    if not vocab:
        return None

    schema = "http://www.w3.org/2000/01/rdf-schema#label"
    table = f"https://purl.humanatlas.io/vocab/{vocab}"
    query = f"SELECT ?label FROM <{table}> WHERE {{ <{ann_url}> <{schema}> ?label }}"
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    res = requests.post("https://lod.humanatlas.io/sparql", data={"query": query}, headers=headers)
    if res.status_code != 200:
        return None

    bindings = res.json().get("results", {}).get("bindings", [])
    if len(bindings) != 1:
        return None

    label = bindings[0].get("label", {}).get("value")
    if not label:
        return None

    ontology_lookup_cache[ann_url] = label  # cache the result
    return {"label": label, "purl": ann_url}


def get_previous_revision_uuids(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the list of uuids of the previous revision datasets if exists.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The uuid list of previous revision entities or [] if not found
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_previous_revision_uuids()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    previous_revision_uuid = schema_neo4j_queries.get_previous_revision_uuids(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    # previous_revision_uuid can be None, but will be filtered out by
    # schema_manager.normalize_entity_result_for_response()
    return property_key, previous_revision_uuid


def get_next_revision_uuids(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the uuid of the next version dataset if exists.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The uuid list of next revision entities or [] if not found
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_next_revision_uuids()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    next_revision_uuids = schema_neo4j_queries.get_next_revision_uuids(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    # next_revision_uuid can be None, but will be filtered out by
    # schema_manager.normalize_entity_result_for_response()
    return property_key, next_revision_uuids


def get_previous_revision_uuid(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the uuid of the previous revision dataset if exists.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The uuid string of previous revision entity or None if not found
    """
    if existing_data_dict.get("status") != "Published":
        return property_key, None

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_previous_revision_uuid()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    previous_revision_uuid = schema_neo4j_queries.get_previous_revision_uuid(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    # previous_revision_uuid can be None, but will be filtered out by
    # schema_manager.normalize_entity_result_for_response()
    return property_key, previous_revision_uuid


def get_next_revision_uuid(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the uuid of the next version dataset if exists.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The uuid string of next version entity or None if not found
    """
    if existing_data_dict.get("status") != "Published":
        return property_key, None

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_next_revision_uuid()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    next_revision_uuid = schema_neo4j_queries.get_next_revision_uuid(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    # next_revision_uuid can be None, but will be filtered out by
    # schema_manager.normalize_entity_result_for_response()
    return property_key, next_revision_uuid


def commit_thumbnail_file(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event method to commit thumbnail file saved that were previously uploaded via ingest-api.

    The information, filename is saved in the field with name specified by `target_property_key`
    in the provided data_dict.  The thumbnail file needed to be previously uploaded
    using the temp file service.  The temp file id provided must be provided
    in the field `thumbnail_file_to_add` in the data_dict for file being committed
    in a JSON object like below:

    {"temp_file_id": "eiaja823jafd"}

    Parameters
    ----------
    property_key : str
        The property key for which the original trigger method is defined
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    dict: The updated generated dict
    """
    # The name of the property where the file information is stored
    target_property_key = "thumbnail_file"

    # Do nothing if no thumbnail file to add (missing or empty property)
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    try:
        if "uuid" in new_data_dict:
            entity_uuid = new_data_dict["uuid"]
        else:
            entity_uuid = existing_data_dict["uuid"]

        # Commit the thumbnail file via ingest-api call
        ingest_api_target_url = schema_manager.get_ingest_api_url() + "/file-commit"

        # Example: {"temp_file_id":"dzevgd6xjs4d5grmcp4n"}
        thumbnail_file_dict = new_data_dict[property_key]

        tmp_file_id = thumbnail_file_dict["temp_file_id"]

        json_to_post = {
            "temp_file_id": tmp_file_id,
            "entity_uuid": entity_uuid,
            "user_token": user_token,
        }

        logger.info(
            f"Commit the uploaded thumbnail file of tmp file id {tmp_file_id} for entity {entity_uuid} via ingest-api call..."
        )

        # Disable ssl certificate verification
        response = requests.post(
            url=ingest_api_target_url,
            headers=schema_manager._create_request_headers(user_token),
            json=json_to_post,
            verify=False,
        )

        if response.status_code != 200:
            msg = f"Failed to commit the thumbnail file of tmp file id {tmp_file_id} via ingest-api for entity uuid: {entity_uuid}"
            logger.error(msg)
            raise schema_errors.FileUploadException(msg)

        # Get back the file uuid dict
        file_uuid_info = response.json()

        # Update the target_property_key (`thumbnail_file`) to be saved in Neo4j
        generated_dict[target_property_key] = {
            "filename": file_uuid_info["filename"],
            "file_uuid": file_uuid_info["file_uuid"],
        }

        return generated_dict
    except schema_errors.FileUploadException:
        raise
    except Exception:
        # No need to log
        raise


def delete_thumbnail_file(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event method for removing the thumbnail file from a dataset during update.

    File is stored in a json encoded text field with property name 'target_property_key' in the entity dict
    The file to remove is specified as file uuid in the `property_key` field

    Parameters
    ----------
    property_key : str
        The property key for which the original trigger method is defined
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    dict: The updated generated dict
    """
    # The name of the property where the file information is stored
    target_property_key = "thumbnail_file"

    # Do nothing if no thumbnail file to delete
    # is provided in the field specified by property_key
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            f"Missing 'uuid' key in 'existing_data_dict' during calling 'delete_thumbnail_file()' trigger method for property '{target_property_key}'.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    entity_uuid = existing_data_dict["uuid"]

    # The property_key (`thumbnail_file_to_remove`) is just a file uuid string
    file_uuid = new_data_dict[property_key]

    # If POST or PUT where the target doesn't exist create the file info dict
    # if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    # if it isn't in the existing_dictionary throw and error
    # or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            msg = create_trigger_error_msg(
                f"Missing '{target_property_key}' key missing during calling 'delete_thumbnail_file()' trigger method on entity {entity_uuid}.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)
        # Otherwise this is a PUT where the target thumbnail file exists already
        else:
            # Note: The property, name specified by `target_property_key`,
            # is stored in Neo4j as a string representation of the Python dict
            # It's not stored in Neo4j as a json string! And we can't store it as a json string
            # due to the way that Cypher handles single/double quotes.
            file_info_dict = schema_manager.get_as_dict(existing_data_dict[target_property_key])
    else:
        file_info_dict = generated_dict[target_property_key]

    # Remove the thumbnail file via ingest-api call
    ingest_api_target_url = schema_manager.get_ingest_api_url() + "/file-remove"

    # ingest-api's /file-remove takes a list of files to remove
    # In this case, we only need to remove the single thumbnail file
    json_to_post = {
        "entity_uuid": entity_uuid,
        "file_uuids": [file_uuid],
        "files_info_list": [file_info_dict],
    }

    logger.info(
        f"Remove the uploaded thumbnail file {file_uuid} for entity {entity_uuid} via ingest-api call..."
    )

    # Disable ssl certificate verification
    response = requests.post(
        url=ingest_api_target_url,
        headers=schema_manager._create_request_headers(user_token),
        json=json_to_post,
        verify=False,
    )

    # response.json() returns an empty array because
    # there's no thumbnail file left once the only one gets removed
    if response.status_code != 200:
        msg = f"Failed to remove the thumbnail file {file_uuid} via ingest-api for dataset uuid: {entity_uuid}"
        logger.error(msg)
        raise schema_errors.FileUploadException(msg)

    # Update the value of target_property_key `thumbnail_file` to empty json string
    generated_dict[target_property_key] = {}

    return generated_dict


####################################################################################################
## Trigger methods specific to Entity - DO NOT RENAME
####################################################################################################


def set_was_attributed_to(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage between this new Entity and Agent.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'set_was_attributed_to()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "group_uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'group_uuid' key in 'existing_data_dict' during calling 'set_was_attributed_to()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict["group_uuid"]]
    # direct_ancestor_uuids =  schema_manager.get_as_dict(existing_data_dict['was_attributed_to'])

    activity_data_dict = schema_manager.generate_activity_data(
        normalized_type, user_token, existing_data_dict
    )

    try:
        # Create a linkage
        # between the Entity node and the parent Agent node in neo4j
        schema_neo4j_queries.link_entity_to_agent(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            direct_ancestor_uuids,
            activity_data_dict,
        )
    except TransactionError:
        # No need to log
        raise


def set_was_generated_by(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage between this new Entity and another Entity through an Activity.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'set_was_generated_by()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    if normalized_type in ["Dataset", "Publication"]:
        if "direct_ancestor_uuids" not in existing_data_dict:
            msg = create_trigger_error_msg(
                "Missing 'direct_ancestor_uuids' key in 'existing_data_dict' during calling 'set_was_generated_by()' trigger method.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)
        direct_ancestor_uuids = existing_data_dict["direct_ancestor_uuids"]
    else:
        if "direct_ancestor_uuid" not in existing_data_dict:
            msg = create_trigger_error_msg(
                "Missing 'direct_ancestor_uuid' key in 'existing_data_dict' during calling 'set_was_generated_by()' trigger method.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)
        direct_ancestor_uuids = [existing_data_dict["direct_ancestor_uuid"]]

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(
        normalized_type, user_token, existing_data_dict
    )

    try:
        # Create a linkage  (via Activity node)
        # between the Entity node and the parent Agent node in neo4j
        schema_neo4j_queries.link_entity_to_entity_via_activity(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            direct_ancestor_uuids,
            activity_data_dict,
        )
    except TransactionError:
        # No need to log
        raise


def set_was_derived_from(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage between this new Entity and another Entity.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'set_was_derived_from()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "was_derived_from" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'was_derived_from' key in 'existing_data_dict' during calling 'set_was_derived_from()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = schema_manager.get_as_dict(existing_data_dict["was_derived_from"])

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(
        normalized_type, user_token, existing_data_dict
    )

    try:
        # Create a linkage  (via Activity node)
        # between the Entity node and the parent Agent node in neo4j
        schema_neo4j_queries.link_entity_to_entity(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            direct_ancestor_uuids,
            activity_data_dict,
        )
    except TransactionError:
        # No need to log
        raise


def update_status(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method that calls related functions involved with updating the status value.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    status = existing_data_dict.get("status")

    # execute set_status_history
    set_status_history(property_key, normalized_type, user_token, existing_data_dict, new_data_dict)

    if status is not None and not equals(status, "published"):
        # execute sync_component_dataset_status
        sync_component_dataset_status(
            property_key, normalized_type, user_token, existing_data_dict, new_data_dict
        )


def sync_component_dataset_status(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Function that changes the status of component datasets when their parent multi-assay dataset's status changes.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """

    if "uuid" not in existing_data_dict:
        raise KeyError(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'sync_component_dataset_status()' trigger method."
        )
    uuid = existing_data_dict["uuid"]
    if "status" not in existing_data_dict:
        raise KeyError(
            "Missing 'status' key in 'existing_data_dict' during calling 'sync_component_dataset_status()' trigger method."
        )
    status = existing_data_dict["status"]
    children_uuids_list = schema_neo4j_queries.get_children(
        schema_manager.get_neo4j_driver_instance(), uuid, properties=["uuid"]
    )
    status_body = {"status": status}

    for child_uuid in children_uuids_list:
        creation_action = schema_neo4j_queries.get_entity_creation_action_activity(
            schema_manager.get_neo4j_driver_instance(), child_uuid
        )
        if creation_action == "Multi-Assay Split":
            # Update the status of the child entities
            url = schema_manager.get_entity_api_url() + "entities/" + child_uuid
            header = schema_manager._create_request_headers(user_token)
            header[SchemaConstants.SENNET_APP_HEADER] = SchemaConstants.INGEST_API_APP
            header[SchemaConstants.INTERNAL_TRIGGER] = SchemaConstants.COMPONENT_DATASET
            response = requests.put(url=url, headers=header, json=status_body)
            if response.status_code != 200:
                logger.error(
                    f"Failed to update status of child entity {child_uuid} when parent dataset status changed: {response.text}"
                )


####################################################################################################
## Trigger methods specific to Collection - DO NOT RENAME
####################################################################################################


def set_in_collection(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method of building linkage between this new Collection and the entities it contains.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'set_in_collection()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "entities" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'entities' key in 'existing_data_dict' during calling 'set_in_collection()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    direct_ancestor_uuids = schema_manager.get_as_dict(existing_data_dict["entities"])

    try:
        # Create a linkage
        # between the Entity node and the parent Agent node in neo4j
        schema_neo4j_queries.link_collection_to_entity(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            direct_ancestor_uuids,
        )
    except TransactionError:
        # No need to log
        raise


####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################


def commit_metadata_files(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file.

    The information, filename and optional description is saved in the field with name specified by `target_property_key`
    in the provided data_dict.  The image files needed to be previously uploaded
    using the temp file service (UploadFileHelper.save_file).  The temp file id provided
    from UploadFileHelper, paired with an optional description of the file must be provided
    in the field `image_files_to_add` in the data_dict for each file being committed
    in a JSON array like below ("description" is optional):

    [
      {
        "temp_file_id": "eiaja823jafd",
        "description": "Metadata file 1"
      },
      {
        "temp_file_id": "pd34hu4spb3lk43usdr"
      },
      {
        "temp_file_id": "32kafoiw4fbazd",
        "description": "Metadata file 3"
      }
    ]


    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The file info dicts in a list
    """
    return _commit_files(
        "metadata_files",
        property_key,
        normalized_type,
        user_token,
        existing_data_dict,
        new_data_dict,
        generated_dict,
    )


def delete_metadata_files(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict, generated_dict
):
    """Trigger event methods for removing files from an entity during update.

    Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
    The files to remove are specified as file uuids in the `property_key` field

    The two outer methods (delete_image_files and delete_metadata_files) pass the target property
    field name to private method, _delete_files along with the other required trigger properties

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    -----------
    target_property_key: str
        The name of the property where the file information is stored

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The file info dicts in a list
    """
    return _delete_files(
        "metadata_files",
        property_key,
        normalized_type,
        user_token,
        existing_data_dict,
        new_data_dict,
        generated_dict,
    )


def get_sample_direct_ancestor(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting the parent of a Sample.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: The direct ancestor entity (either another Sample or a Source) with all the normalized information
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_direct_ancestor()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    direct_ancestor_dict = schema_neo4j_queries.get_sample_direct_ancestor(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )

    if "entity_type" not in direct_ancestor_dict:
        msg = create_trigger_error_msg(
            "Missing 'entity_type' key in 'direct_ancestor_dict' during calling 'get_sample_direct_ancestor()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Generate trigger data for sample's direct_ancestor and skip the direct_ancestor's direct_ancestor
    properties_to_skip = ["direct_ancestor"]
    complete_dict = schema_manager.get_complete_entity_result(
        user_token, direct_ancestor_dict, properties_to_skip
    )

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_object_result_for_response(
        "ENTITIES", complete_dict
    )


####################################################################################################
## Trigger methods specific to Publication - DO NOT RENAME
####################################################################################################


def set_publication_date(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of truncating the time part of publication_date if provided by users.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Publication
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The date part YYYY-MM-DD of ISO 8601
    """
    # We only store the date part 'YYYY-MM-DD', base on the ISO 8601 format, it's fine if the user entered the time part
    date_obj = datetime.fromisoformat(new_data_dict[property_key])

    return property_key, date_obj.date().isoformat()


####################################################################################################
## Trigger methods specific to Upload - DO NOT RENAME
####################################################################################################


def set_upload_status_new(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of setting the Upload initial status - "New".

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The "New" status
    """
    return property_key, "New"


def link_upload_to_lab(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkage between this new Upload and Lab.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_upload_to_lab()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "group_uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'group_uuid' key in 'existing_data_dict' during calling 'link_upload_to_lab()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict["group_uuid"]]

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(
        normalized_type, user_token, existing_data_dict
    )

    try:
        # Create a linkage (via Activity node)
        # between the Submission node and the parent Lab node in neo4j
        schema_neo4j_queries.link_entity_to_entity_via_activity(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            direct_ancestor_uuids,
            activity_data_dict,
        )

        # No need to delete any cache here since this is one-time upload creation
    except TransactionError:
        # No need to log
        raise


def link_datasets_to_upload(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of building linkages between this Submission and the given datasets.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'link_datasets_to_upload()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "dataset_uuids_to_link" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'dataset_uuids_to_link' key in 'existing_data_dict' during calling 'link_datasets_to_upload()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    upload_uuid = existing_data_dict["uuid"]
    dataset_uuids = existing_data_dict["dataset_uuids_to_link"]

    try:
        # Create a direct linkage (Dataset) - [:IN_UPLOAD] -> (Submission) for each dataset
        schema_neo4j_queries.link_datasets_to_upload(
            schema_manager.get_neo4j_driver_instance(), upload_uuid, dataset_uuids
        )

        # Delete the cache of each associated dataset and the target upload if any cache exists
        # Because the `Dataset.upload` and `Upload.datasets` fields, and
        uuids_list = [upload_uuid] + dataset_uuids
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError:
        # No need to log
        raise


def unlink_datasets_from_upload(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of deleting linkages between this target Submission and the given datasets.

    Parameters
    ----------
    property_key : str
        The target property key
    normalized_type : str
        One of the types defined in the schema yaml: Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'unlink_datasets_from_upload()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if "dataset_uuids_to_unlink" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'dataset_uuids_to_unlink' key in 'existing_data_dict' during calling 'unlink_datasets_from_upload()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    upload_uuid = existing_data_dict["uuid"]
    dataset_uuids = existing_data_dict["dataset_uuids_to_unlink"]

    try:
        # Delete the linkage (Dataset) - [:IN_UPLOAD] -> (Upload) for each dataset
        schema_neo4j_queries.unlink_datasets_from_upload(
            schema_manager.get_neo4j_driver_instance(), upload_uuid, dataset_uuids
        )

        # Delete the cache of each associated dataset and the upload itself if any cache exists
        # Because the associated datasets have this `Dataset.upload` field and Upload has `Upload.datasets` field
        uuids_list = dataset_uuids + [upload_uuid]
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError:
        # No need to log
        raise


def get_upload_datasets(
    property_key: str,
    normalized_type: str,
    user_token: str,
    existing_data_dict: dict,
    new_data_dict: dict,
):
    """Trigger event method of getting a list of associated datasets for a given Upload.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: A list of associated dataset dicts with all the normalized information
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_upload_datasets()' trigger method.",
            existing_data_dict,
        )
        raise KeyError(msg)

    logger.info(
        f"Executing 'get_upload_datasets()' trigger method on uuid: {existing_data_dict['uuid']}"
    )

    upload_datasets = get_normalized_upload_datasets(existing_data_dict["uuid"], user_token)
    return property_key, upload_datasets


def get_normalized_upload_datasets(uuid: str, token, properties_to_exclude: List[str] = []):
    """Query the Neo4j database to get the associated datasets for a given Upload UUID and normalize the results.

    Parameters
    ----------
    uuid : str
        The UUID of the Upload entity
    token: str
        Either the user's globus nexus token or the internal token
    properties_to_exclude : List[str]
        A list of property keys to exclude from the normalized results

    Returns
    -------
    list: A list of associated dataset dicts with all the normalized information
    """
    db = schema_manager.get_neo4j_driver_instance()
    datasets_list = schema_neo4j_queries.get_upload_datasets(db, uuid)

    complete_list = schema_manager.get_complete_entities_list(
        token, datasets_list, properties_to_exclude
    )

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return schema_manager.normalize_entities_list_for_response(
        complete_list,
        property_groups=schema_manager.group_verify_properties_list(
            properties=properties_to_exclude
        ),
        is_include_action=False,
        is_strict=True,
    )


####################################################################################################
## Trigger methods specific to Activity - DO NOT RENAME
####################################################################################################


def set_activity_creation_action(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of getting creation_action for Activity.

    Lab->Activity->Source (Not needed for now)
    Lab->Activity->Submission
    Source->Activity->Sample
    Sample->Activity->Sample
    Sample->Activity->Dataset
    Dataset->Activity->Dataset

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The creation_action string
    """
    if new_data_dict and new_data_dict.get("creation_action"):
        return property_key, new_data_dict["creation_action"].title()

    if "normalized_entity_type" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'normalized_entity_type' key in 'new_data_dict' during calling 'set_activity_creation_action()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    return property_key, f"Create {new_data_dict['normalized_entity_type']} Activity"


URL_SCHEME_REGEX = re.compile(r"^https?://")


def set_activity_protocol_url(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of passing the protocol_url from the entity to the activity. This function
    normalizes the protocol url before storage.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The normalized protocol_url string
    """
    if normalized_type in ["Activity"] and "protocol_url" not in new_data_dict:
        return property_key, None

    if "entity_type" in new_data_dict and new_data_dict["entity_type"] in [
        "Dataset",
        "Upload",
        "Publication",
    ]:
        return property_key, None

    if "protocol_url" not in new_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'protocol_url' key in 'new_data_dict' during calling 'set_activity_protocol_url()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    protocol_url = new_data_dict["protocol_url"].strip()
    normalized_protocol_url = URL_SCHEME_REGEX.sub("", protocol_url)

    return property_key, normalized_protocol_url


def get_creation_action_activity(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_creation_action_activity()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    uuid: str = existing_data_dict["uuid"]
    logger.info(f"Executing 'get_creation_action_activity()' trigger method on uuid: {uuid}")

    neo4j_driver_instance = schema_manager.get_neo4j_driver_instance()
    creation_action_activity = schema_neo4j_queries.get_entity_creation_action_activity(
        neo4j_driver_instance, uuid
    )

    return property_key, creation_action_activity


def set_processing_information(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method of passing the processing_information from the entity to the activity.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The processing_information list
    """
    # Need to hard set `processing_information` as this gets called
    # when `metadata` is passed in the payload
    if "entity_type" in new_data_dict and not equals(new_data_dict["entity_type"], "Dataset"):
        return "processing_information", None

    metadata = None
    for key in ["metadata", "ingest_metadata"]:
        if key in new_data_dict:
            metadata = schema_manager.get_as_dict(new_data_dict[key])
            break
    if metadata is None or "dag_provenance_list" not in metadata:
        return "processing_information", None

    dag_provs = metadata["dag_provenance_list"]

    if len(dag_provs) < 1:
        # dag_provenance_list is empty
        return "processing_information", None

    if any([d.get("hash") is None or d.get("origin") is None for d in dag_provs]):
        # dag_provenance_list contains invalid entries
        # entries must have both hash and origin
        return "processing_information", None

    proc_info = {"description": "", "pipelines": []}
    for idx, dag_prov in enumerate(dag_provs):
        parts = github.parse_repo_name(dag_prov["origin"])
        if parts is None:
            continue
        owner, repo = parts

        if idx == 0 and repo != SchemaConstants.INGEST_PIPELINE_APP:
            # first entry must be the SenNet ingest pipeline
            return "processing_information", None

        if idx > 0 and repo == SchemaConstants.INGEST_PIPELINE_APP:
            # Ignore duplicate ingest pipeline entries
            continue

        # Set description to first non ingest pipeline repo
        if proc_info.get("description") == "" and repo != SchemaConstants.INGEST_PIPELINE_APP:
            proc_info["description"] = github.get_repo_description(owner, repo)

        hash = dag_prov["hash"]
        tag = github.get_tag(owner, repo, hash)
        if tag:
            url = github.create_tag_url(owner, repo, tag)
        else:
            url = github.create_commit_url(owner, repo, hash)
            if url is None:
                continue
        info = {"github": url}

        if "name" in dag_prov:
            cwl_url = github.create_commonwl_url(owner, repo, hash, dag_prov["name"])
            info["commonwl"] = cwl_url

        proc_info["pipelines"].append({repo: info})

    # Prevents invalid json if description is None
    if proc_info["description"] is None:
        proc_info["description"] = ""

    return "processing_information", proc_info


####################################################################################################
## Internal functions
####################################################################################################


def _commit_files(
    target_property_key,
    property_key,
    normalized_type,
    user_token,
    existing_data_dict,
    new_data_dict,
    generated_dict,
):
    """Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file.

    The information, filename and optional description is saved in the field with name specified by `target_property_key`
    in the provided data_dict.  The image files needed to be previously uploaded
    using the temp file service (UploadFileHelper.save_file).  The temp file id provided
    from UploadFileHelper, paired with an optional description of the file must be provided
    in the field `image_files_to_add` in the data_dict for each file being committed
    in a JSON array like below ("description" is optional):

    [
      {
        "temp_file_id": "eiaja823jafd",
        "description": "File 1"
      },
      {
        "temp_file_id": "pd34hu4spb3lk43usdr"
      },
      {
        "temp_file_id": "32kafoiw4fbazd",
        "description": "File 3"
      }
    ]


    Parameters
    ----------
    target_property_key : str
        The name of the property where the file information is stored
    property_key : str
        The property key for which the original trigger method is defined
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    dict: The updated generated dict
    """
    # Do nothing if no files to add are provided (missing or empty property)
    # For image files the property name is "image_files_to_add"
    # For metadata files the property name is "metadata_files_to_add"
    # But other may be used in the future
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    # If POST or PUT where the target doesn't exist create the file info array
    # if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    # or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            files_info_list = []
        # Otherwise this is a PUT where the target array exists already
        else:
            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string
            # due to the way that Cypher handles single/double quotes.
            files_info_list = schema_manager.get_as_dict(existing_data_dict[target_property_key])
    else:
        files_info_list = generated_dict[target_property_key]

    try:
        if "uuid" in new_data_dict:
            entity_uuid = new_data_dict["uuid"]
        else:
            entity_uuid = existing_data_dict["uuid"]

        # Commit the files via ingest-api call
        ingest_api_target_url = schema_manager.get_ingest_api_url() + "/file-commit"

        for file_info in new_data_dict[property_key]:
            temp_file_id = file_info["temp_file_id"]

            json_to_post = {
                "temp_file_id": temp_file_id,
                "entity_uuid": entity_uuid,
                "user_token": user_token,
            }

            logger.info(
                f"Commit the uploaded file of temp_file_id {temp_file_id} for entity {entity_uuid} via ingest-api call..."
            )

            # Disable ssl certificate verification
            response = requests.post(
                url=ingest_api_target_url,
                headers=schema_manager._create_request_headers(user_token),
                json=json_to_post,
                verify=False,
            )

            if response.status_code != 200:
                msg = create_trigger_error_msg(
                    f"Failed to commit the file of temp_file_id {temp_file_id} via ingest-api for entity uuid: {entity_uuid}",
                    existing_data_dict,
                    new_data_dict,
                )
                logger.error(msg)
                raise schema_errors.FileUploadException(msg)

            # Get back the file uuid dict
            file_uuid_info = response.json()

            file_info_to_add = {
                "filename": file_uuid_info["filename"],
                "file_uuid": file_uuid_info["file_uuid"],
            }

            # The `description` is optional
            if "description" in file_info:
                file_info_to_add["description"] = file_info["description"]

            # Add to list
            files_info_list.append(file_info_to_add)

            # Update the target_property_key value
            generated_dict[target_property_key] = files_info_list

        return generated_dict
    except schema_errors.FileUploadException:
        raise
    except Exception:
        # No need to log
        raise


def _delete_files(
    target_property_key,
    property_key,
    normalized_type,
    user_token,
    existing_data_dict,
    new_data_dict,
    generated_dict,
):
    """Trigger event method for removing files from an entity during update.

    Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
    The files to remove are specified as file uuids in the `property_key` field

    The two outer methods (delete_image_files and delete_metadata_files) pass the target property
    field name to private method, _delete_files along with the other required trigger properties

    Parameters
    ----------
    target_property_key : str
        The name of the property where the file information is stored
    property_key : str
        The property key for which the original trigger method is defined
    normalized_type : str
        One of the types defined in the schema yaml: Source, Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    generated_dict : dict
        A dictionary that contains all final data

    Returns
    -------
    dict: The updated generated dict
    """
    # do nothing if no files to delete are provided in the field specified by property_key
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            f"Missing 'uuid' key in 'existing_data_dict' during calling '_delete_files()' trigger method for property '{target_property_key}'.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    entity_uuid = existing_data_dict["uuid"]

    # If POST or PUT where the target doesn't exist create the file info array
    # if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    # if it isn't in the existing_dictionary throw and error
    # or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            msg = create_trigger_error_msg(
                f"Missing '{target_property_key}' key in 'existing_data_dict' during calling '_delete_files()' trigger method on entity {entity_uuid}.",
                existing_data_dict,
                new_data_dict,
            )
            raise KeyError(msg)
        # Otherwise this is a PUT where the target array exists already
        else:
            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string
            # due to the way that Cypher handles single/double quotes.
            files_info_list = schema_manager.get_as_dict(existing_data_dict[target_property_key])
    else:
        files_info_list = generated_dict[target_property_key]

    file_uuids = []
    for file_uuid in new_data_dict[property_key]:
        file_uuids.append(file_uuid)

    # Remove the files via ingest-api call
    ingest_api_target_url = schema_manager.get_ingest_api_url() + "/file-remove"

    json_to_post = {
        "entity_uuid": entity_uuid,
        "file_uuids": file_uuids,
        "files_info_list": files_info_list,
    }

    logger.info(f"Remove the uploaded files for entity {entity_uuid} via ingest-api call...")

    # Disable ssl certificate verification
    response = requests.post(
        url=ingest_api_target_url,
        headers=schema_manager._create_request_headers(user_token),
        json=json_to_post,
        verify=False,
    )

    if response.status_code != 200:
        msg = create_trigger_error_msg(
            f"Failed to remove the files via ingest-api for entity uuid: {entity_uuid}",
            existing_data_dict,
            new_data_dict,
        )
        logger.error(msg)
        raise schema_errors.FileUploadException(msg)

    files_info_list = response.json()

    # Update the target_property_key value to be saved in Neo4j
    generated_dict[target_property_key] = files_info_list

    return generated_dict


def _get_organ_description(organ_code):
    """Get the organ description based on the given organ code.

    Parameters
    ----------
    organ_code : str
        The two-letter organ code

    Returns
    -------
    str: The organ code description
    """
    ORGAN_TYPES = Ontology.ops(
        as_arr=False, prop_callback=None, as_data_dict=True, data_as_val=True
    ).organ_types()

    for key in ORGAN_TYPES:
        if ORGAN_TYPES[key]["organ_uberon"] == organ_code:
            return ORGAN_TYPES[key]["term"].lower()


def source_metadata_group(key: str) -> str:
    """Get the source mapped metadata group for the given key.

    Parameters
    ----------
    key : str
        The metadata key

    Returns
    -------
    str: The group display name
    """
    groups_map = {
        "abo_blood_group_system": "Demographics",
        "age": "Demographics",
        "amylase": "Lab Values",
        "body_mass_index": "Vitals",
        "cause_of_death": "Donation Information",
        "ethnicity": "Demographics",
        "hba1c": "Lab Values",
        "height": "Vitals",
        "lipase": "Lab Values",
        "mechanism_of_injury": "Donation Information",
        "medical_history": "History",
        "race": "Demographics",
        "rh_blood_group": "Demographics",
        "sex": "Demographics",
        "social_history": "History",
        "weight": "Vitals",
    }
    return groups_map.get(key, "Other Information")


def source_metadata_display_value(metadata_item: dict) -> str:
    """Get the display value for the given source metadata item.

    Parameters
    ----------
    metadata_item : dict
        The source metadata item

    Returns
    -------
    str: The display value
    """
    if metadata_item.get("data_type") != "Numeric":
        return metadata_item["preferred_term"]

    value = float(metadata_item.get("data_value"))
    units = metadata_item.get("units", "")
    if units.startswith("year"):
        value = int(value)
        return f"{value} years" if value != 1 else f"{value} year"
    if units == "%":
        return f"{value}%"

    units_map = {
        "cm": (
            0.393701,
            "in",
        ),
        "kg": (
            2.20462,
            "lb",
        ),
    }
    display_value = f"{value} {units}"
    if units in units_map:
        display_value += f" ({round(value * units_map[units][0], 1)} {units_map[units][1]})"

    return display_value


####################################################################################################
## Trigger methods shared by Dataset, Upload, and Publication - DO NOT RENAME
####################################################################################################


def set_status_history(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for setting the status history for a given dataset or upload

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Dataset, Upload
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used
    """
    new_status_history = []
    status_entry = {}

    if "status_history" in existing_data_dict:
        status_history_string = existing_data_dict["status_history"].replace("'", '"')
        new_status_history += json.loads(status_history_string)

    if "status" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'status' key in 'existing_data_dict' during calling 'set_status_history()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)
    if "last_modified_timestamp" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'last_modified_timestamp' key in 'existing_data_dict' during calling 'set_status_history()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)
    if "last_modified_user_email" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'last_modified_user_email' key in 'existing_data_dict' during calling 'set_status_history()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    status = existing_data_dict["status"]
    last_modified_user_email = existing_data_dict["last_modified_user_email"]
    last_modified_timestamp = existing_data_dict["last_modified_timestamp"]
    uuid = existing_data_dict["uuid"]

    status_entry["status"] = status
    status_entry["changed_by_email"] = last_modified_user_email
    status_entry["change_timestamp"] = last_modified_timestamp
    new_status_history.append(status_entry)
    entity_data_dict = {"status_history": new_status_history}

    schema_neo4j_queries.update_entity(
        schema_manager.get_neo4j_driver_instance(), normalized_type, entity_data_dict, uuid
    )


def set_publication_dataset_type(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method setting the dataset_type immutable property for a Publication.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Publication
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: Immutable dataset_type of "Publication"
    """
    # Count upon the dataset_type generated: true property in provenance_schema.yaml to assure the
    # request does not contain a value which will be overwritten.
    return property_key, "Publication"


def set_dataset_sources(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method setting the sources list for a dataset.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Dataset|Publication
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, list]
        str: The target property key
        list: The list of sources associated with a dataset
    """
    sources = schema_neo4j_queries.get_sources_associated_entity(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )
    return property_key, sources


def set_sample_source(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method setting the source dict for a sample.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, dict]
        str: The target property key
        dict: The source associated with a sample
    """
    sources = schema_neo4j_queries.get_sources_associated_entity(
        schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
    )
    return property_key, sources[0]


def get_organ_hierarchy(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method setting the name of the top level of the hierarchy this organ belongs to based on its laterality.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The organ hierarchy
    """
    organ_hierarchy = None
    if equals(existing_data_dict["sample_category"], "organ"):
        organ_types_categories = Ontology.ops(
            as_data_dict=True, prop_callback=None, key="organ_uberon", val_key="category"
        ).organ_types()

        organ_hierarchy = existing_data_dict["organ"]
        if organ_types_categories.get(organ_hierarchy) is not None:
            return property_key, organ_types_categories[organ_hierarchy]["term"]

        organ_types = Ontology.ops(
            as_data_dict=True, prop_callback=None, key="organ_uberon", val_key="term"
        ).organ_types()
        if existing_data_dict["organ"] in organ_types:
            organ_name = organ_types[organ_hierarchy]
            organ_hierarchy = organ_name

            # Deprecated. For backwards compatibility. Can eventually remove this regex on the text.
            res = re.findall(
                ".+?(?=\()", organ_name
            )  # the pattern will find everything up to the first (
            if len(res) > 0:
                organ_hierarchy = res[0].strip()

    return property_key, organ_hierarchy


def get_dataset_type_hierarchy(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method for setting the dataset type hierarchy.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        dict: The dataset type hierarchy with keys of 'first_level' and 'second_level'
    """

    if equals(existing_data_dict['entity_type'], 'Dataset'):
        if (
            "DATASET_TYPE_HIERARCHY" not in current_app.config
            or existing_data_dict["dataset_type"] not in current_app.config["DATASET_TYPE_HIERARCHY"]
        ):
            return property_key, {
                "first_level": existing_data_dict["dataset_type"],
                "second_level": existing_data_dict["dataset_type"],
            }

        return property_key, {
            "first_level": current_app.config["DATASET_TYPE_HIERARCHY"][
                existing_data_dict["dataset_type"]
            ],
            "second_level": existing_data_dict["dataset_type"],
        }
    else:
        return property_key, None


def get_has_qa_published_derived_dataset(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method that determines if a primary dataset a processed/derived dataset with a status of 'QA' and 'Published'.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: Whether a primary dataset has at least one processed dataset with a status of 'QA', 'True' or 'False'
    """
    _, dataset_category = get_dataset_category(
        property_key, normalized_type, user_token, existing_data_dict, new_data_dict
    )
    if equals(dataset_category, "primary"):
        match_case = "AND s.status IN ['QA', 'Published']"
        descendants = schema_neo4j_queries.get_dataset_direct_descendants(
            schema_manager.get_neo4j_driver_instance(),
            existing_data_dict["uuid"],
            match_case=match_case,
        )
        for d in descendants:
            if equals(d.get('entity_type'), 'Dataset'):
                _, descendant_category = get_dataset_category(
                    property_key, normalized_type, user_token, d, d
                )
                if "processed" in descendant_category:
                    return property_key, "True"
        return property_key, "False"
    else:
        return property_key, "False"


def get_has_all_published_datasets(
    property_key, normalized_type, user_token, existing_data_dict, new_data_dict
):
    """Trigger event method that determines if the datasets of an upload are all published.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: The result whether all the primary datasets which are all published
    """

    db = schema_manager.get_neo4j_driver_instance()

    published_filter = 'AND t.status = "Published"'
    datasets_primary_list = schema_neo4j_queries.get_upload_datasets(
        db, existing_data_dict["uuid"], properties=["uuid"]
    )
    datasets_primary_list_published = schema_neo4j_queries.get_upload_datasets(
        db, existing_data_dict["uuid"], properties=["uuid"], query_filter=f"{published_filter}"
    )

    return property_key, (
        str(len(datasets_primary_list) == len(datasets_primary_list_published))
        if len(datasets_primary_list) > 0
        else "False"
    )

def get_primary_dataset_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method that grabs the primary dataset UUID for derived and component datasets
        Parameters
        ----------
        property_key : str
            The target property key of the value to be generated
        normalized_type : str
            One of the types defined in the schema yaml: Sample
        user_token: str
            The user's globus nexus token
        existing_data_dict : dict
            A dictionary that contains all existing entity properties
        new_data_dict : dict
            A merged dictionary that contains all possible input data to be used

        Returns
        -------
        Tuple[str, str]
            str: The target property key
            str: "True" or "False" if the sample has any descendant datasets
        """
    if equals(existing_data_dict["entity_type"], "Dataset"):
        if existing_data_dict['creation_action'] == "Multi-Assay Split":
            return (
                property_key,
                schema_neo4j_queries.get_primary_dataset(
                    schema_manager.get_neo4j_driver_instance(), existing_data_dict["uuid"]
                )["uuid"],
            )
        return property_key, None
    else:
        return property_key, None


def get_contains_data(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    """Trigger event method that determines if a sample has any descendant datasets.

    Parameters
    ----------
    property_key : str
        The target property key of the value to be generated
    normalized_type : str
        One of the types defined in the schema yaml: Sample
    user_token: str
        The user's globus nexus token
    existing_data_dict : dict
        A dictionary that contains all existing entity properties
    new_data_dict : dict
        A merged dictionary that contains all possible input data to be used

    Returns
    -------
    Tuple[str, str]
        str: The target property key
        str: "True" or "False" if the sample has any descendant datasets
    """
    if "uuid" not in existing_data_dict:
        msg = create_trigger_error_msg(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_contains_data()' trigger method.",
            existing_data_dict,
            new_data_dict,
        )
        raise KeyError(msg)

    if not equals(Ontology.ops().entities().SAMPLE, existing_data_dict["entity_type"]):
        return property_key, None

    datasets = app_neo4j_queries.get_descendants_by_type(
        neo4j_driver=schema_manager.get_neo4j_driver_instance(),
        uuid=existing_data_dict["uuid"],
        descendant_type=Ontology.ops().entities().DATASET,
        property_keys=["uuid"],
    )

    return property_key, str(len(datasets) > 0)

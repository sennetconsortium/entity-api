import yaml
import logging
import requests
from datetime import datetime

from flask import Response
from hubmap_commons.file_helper import ensureTrailingSlashURL

# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning


from lib.commons import get_as_dict
from lib.property_groups import PropertyGroups

# Local modules
from schema import schema_errors
from schema import schema_triggers
from schema import schema_bulk_triggers
from schema import schema_validators
from schema.schema_bulk_triggers import BulkTriggersManager
from schema.schema_constants import SchemaConstants, MetadataScopeEnum, TriggerTypeEnum
from schema import schema_neo4j_queries

# Atlas Consortia commons
from atlas_consortia_commons.rest import abort_bad_req
from atlas_consortia_commons.string import equals
from typing import List

from lib.ontology import Ontology

logger = logging.getLogger(__name__)

try:
    logger.info("logger initialized")
except Exception as e:
    print("Error opening log file during startup")
    print(str(e))

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# In Python, "privacy" depends on "consenting adults'" levels of agreement, we can't force it.
# A single leading underscore means you're not supposed to access it "from the outside"
_schema = None
_uuid_api_url = None
_entity_api_url = None
_ingest_api_url = None
_search_api_url = None
_auth_helper = None
_neo4j_driver = None
_ubkg = None
_memcached_client = None
_memcached_prefix = None
_schema_properties = {}

# For handling cached requests to uuid-api and external static resources (github raw yaml files)
request_cache = {}

####################################################################################################
## Provenance yaml schema initialization
####################################################################################################

"""
Initialize the schema_manager module with loading the schema yaml file
and create an neo4j driver instance (some trigger methods query neo4j)

Parameters
----------
valid_yaml_file : file
    A valid yaml file
neo4j_session_context : neo4j.Session object
    The neo4j database session
"""


def initialize(
    valid_yaml_file,
    uuid_api_url,
    entity_api_url,
    ingest_api_url,
    search_api_url,
    auth_helper_instance,
    neo4j_driver_instance,
    ubkg_instance,
    memcached_client_instance,
    memcached_prefix,
):

    # Specify as module-scope variables
    global _schema
    global _uuid_api_url
    global _entity_api_url
    global _ingest_api_url
    global _search_api_url
    global _auth_helper
    global _neo4j_driver
    global _ubkg
    global _memcached_client
    global _memcached_prefix
    global _schema_properties

    logger.info(f"Initialize schema_manager using valid_yaml_file={valid_yaml_file}.")
    _schema = load_provenance_schema(valid_yaml_file)
    _schema_properties = group_schema_properties_by_name()
    if _schema is None:
        logger.error(f"Failed to load _schema using {valid_yaml_file}.")
    _uuid_api_url = uuid_api_url
    _ingest_api_url = ingest_api_url
    _search_api_url = search_api_url

    if entity_api_url is not None:
        _entity_api_url = entity_api_url
    else:
        msg = f"Unable to initialize schema manager with entity_api_url={entity_api_url}."
        logger.critical(msg=msg)
        raise Exception(msg)

    # Get the helper instances
    _auth_helper = auth_helper_instance
    _neo4j_driver = neo4j_driver_instance
    _ubkg = ubkg_instance

    _memcached_client = memcached_client_instance
    _memcached_prefix = memcached_prefix


####################################################################################################
## Provenance yaml schema loading
####################################################################################################


def load_provenance_schema(valid_yaml_file):
    """
    Load the schema yaml file

    Parameters
    ----------
    valid_yaml_file : file
        A valid yaml file

    Returns
    -------
    dict
        A dict containing the schema details
    """

    with open(valid_yaml_file) as file:
        schema_dict = yaml.safe_load(file)

        logger.info("Schema yaml file loaded successfully")
        # For entities with properties set to None/Null, remove them as these represent private values not inherited by subclass
        for entity in schema_dict["ENTITIES"]:
            schema_dict["ENTITIES"][entity]["properties"] = remove_none_values(
                schema_dict["ENTITIES"][entity]["properties"]
            )
        return schema_dict


def get_schema_properties():
    global _schema_properties
    return _schema_properties


def group_schema_properties_by_name():
    """
    This formats the entities schema properties using property names as key.
    Then within various buckets, has a set containing entity names which the property belongs to.

    This allows for constant time access when filtering responses by property names.

    Returns
    -------
    dict
    """
    global _schema

    schema_properties_by_name = {}
    for entity in _schema["ENTITIES"]:
        entity_properties = _schema["ENTITIES"][entity].get("properties", {})
        for p in entity_properties:
            if p not in schema_properties_by_name:
                schema_properties_by_name[p] = {}
                schema_properties_by_name[p]["dependencies"] = set()
                schema_properties_by_name[p]["trigger"] = set()
                schema_properties_by_name[p]["neo4j"] = set()
                schema_properties_by_name[p]["json_string"] = set()
                schema_properties_by_name[p]["list"] = set()
                schema_properties_by_name[p]["use_activity_value_if_null"] = set()
                schema_properties_by_name[p]["dependencies"].update(
                    entity_properties[p].get("dependency_properties", [])
                )

            if (
                "on_read_trigger" in entity_properties[p]
                or "on_bulk_read_trigger" in entity_properties[p]
            ):
                schema_properties_by_name[p]["trigger"].add(entity)
            else:
                schema_properties_by_name[p]["neo4j"].add(entity)

                if "use_activity_value_if_null" in entity_properties[p]:
                    schema_properties_by_name[p]["use_activity_value_if_null"].add(entity)

                if "type" in entity_properties[p]:
                    if entity_properties[p]["type"] == "json_string":
                        schema_properties_by_name[p]["json_string"].add(entity)
                    if entity_properties[p]["type"] == "list":
                        schema_properties_by_name[p]["list"].add(entity)

    return schema_properties_by_name


####################################################################################################
## Helper functions
####################################################################################################


def get_all_types():
    """
    Get a list of all the supported types in the schema yaml

    Returns
    -------
    list
        A list of types
    """
    global _schema

    entity_types = _schema["ENTITIES"].keys()
    activity_types = _schema["ACTIVITIES"].keys()

    # Need convert the dict_keys object to a list
    return list(entity_types) + list(activity_types)


def get_entity_superclass(normalized_entity_class):
    """
    Get the superclass (if defined) of the given entity class

    Parameters
    ----------
    normalized_entity_class : str
        The normalized target entity class

    Returns
    -------
    string or None
        One of the normalized entity classes if defined (currently only Publication has Dataset as superclass). None otherwise
    """
    normalized_superclass = None

    all_entity_types = get_all_entity_types()

    if normalized_entity_class in all_entity_types:
        if "superclass" in _schema["ENTITIES"][normalized_entity_class]:
            normalized_superclass = normalize_entity_type(
                _schema["ENTITIES"][normalized_entity_class]["superclass"]
            )

            if normalized_superclass not in all_entity_types:
                msg = f"Invalid 'superclass' value defined for {normalized_entity_class}: {normalized_superclass}"
                logger.error(msg)
                raise ValueError(msg)
        else:
            # Since the 'superclass' property is optional, we just log the warning message, no need to bubble up
            msg = f"The 'superclass' property is not defined for entity class: {normalized_entity_class}"
            logger.warning(msg)

    return normalized_superclass


def entity_type_instanceof(entity_type: str, entity_class: str) -> bool:
    """
    Determine if the Entity type with 'entity_type' is an instance of 'entity_class'.
    Use this function if you already have the Entity type. Use entity_instanceof(uuid, class)
    if you just have the Entity uuid.

    :param entity_type: from Entity
    :param entity_class: found in .yaml file
    :return:  True or False
    """
    if entity_type is None:
        return False

    normalized_entry_class: str = normalize_entity_type(entity_class)
    super_entity_type: str = normalize_entity_type(entity_type)
    while super_entity_type is not None:
        if normalized_entry_class == super_entity_type:
            return True
        super_entity_type = get_entity_superclass(super_entity_type)
    return False


def entity_instanceof(entity_uuid: str, entity_class: str) -> bool:
    """
    Determine if the Entity with 'entity_uuid' is an instance of 'entity_class'.

    :param entity_uuid: from Entity
    :param entity_class: found in .yaml file
    :return: True or False
    """
    entity_type: str = schema_neo4j_queries.get_entity_type(
        get_neo4j_driver_instance(), entity_uuid
    )
    return entity_type_instanceof(entity_type, entity_class)


def extend_dicts(dict1: dict, dict2: dict) -> dict:
    """
    Extends to dicts together checking for None before extending

    Returns
    -------
    dict
        The extended dict
    """
    if dict1 is None:
        dict1 = {}

    if dict2 is None:
        dict2 = {}

    dict1.update(dict2)

    return dict1


def get_entity_properties(schema_section: dict, normalized_class: str) -> dict:
    """
    Gets all properties by entity

    Returns
    -------
    dict
        The Entity properties dict
    """
    properties = schema_section[normalized_class]["properties"]
    super_class = schema_section[normalized_class].get("superclass")

    if super_class is not None and super_class in schema_section:
        super_class_properties = schema_section[super_class]["properties"]
        return extend_dicts(dict(super_class_properties), dict(properties))

    return dict(properties)


def get_all_entity_types():
    """
    Get a list of all the supported entity types in the schema yaml

    Returns
    -------
    list
        A list of entity types
    """
    global _schema

    dict_keys = _schema["ENTITIES"].keys()
    # Need convert the dict_keys object to a list
    return list(dict_keys)


def get_fields_to_exclude(normalized_class=None):
    """Retrieves fields designated in the provenance schema yaml under
    excluded_properties_from_public_response and returns the fields in a list.

    Parameters
    ----------
    normalized_class : Optional[str]
        the normalized entity type of the entity whose fields are to be removed

    Returns
    -------
    list[str]
        A list of strings where each entry is a field to be excluded
    """
    # Determine the schema section based on class
    excluded_fields = []
    schema_section = _schema["ENTITIES"]
    exclude_list = schema_section[normalized_class].get("excluded_properties_from_public_response")
    if exclude_list:
        excluded_fields.extend(exclude_list)
    return excluded_fields


def get_schema_defaults(properties=[], is_include_action=True, target_entity_type="Any"):
    """
    Adds entity defaults to list

    Parameters
    ----------
    properties : list
        the properties to be filtered
    is_include_action : bool
        whether to include or exclude the listed properties
    target_entity_type : str
        the entity type that's the target being filtered

    Returns
    -------
    List[str]
        list of defaults based on entity type
    """
    property_defaults = {
        "Any": [
            "data_access_level",
            "group_name",
            "group_uuid",
            "sennet_id",
            "entity_type",
            "uuid",
        ],
        "Source": ["source_type"],
        "Sample": ["sample_category", "organ"],
        "Dataset": ["dataset_type", "contains_human_genetic_sequences", "status"],
    }
    defaults = []
    if target_entity_type in property_defaults:
        defaults = property_defaults[target_entity_type]
    if target_entity_type == "Any":
        defaults = (
            defaults
            + property_defaults["Source"]
            + property_defaults["Sample"]
            + property_defaults["Dataset"]
        )
    else:
        defaults = defaults + property_defaults["Any"]

    for d in defaults:
        if is_include_action and not d in properties:
            properties.append(d)
        else:
            if is_include_action is False and d in properties:
                properties.remove(d)

    return defaults


def rearrange_datasets(results, entity_type="Dataset"):
    """
    If asked for the descendants of a Dataset then sort by last_modified_timestamp and place the published dataset at the top

    :param results : List[dict]
    :param entity_type : str
    :return:
    """
    if isinstance(results[0], str) is False and equals(
        entity_type, Ontology.ops().entities().DATASET
    ):
        results = sorted(results, key=lambda d: d["last_modified_timestamp"], reverse=True)

        published_processed_dataset_location = next(
            (i for i, item in enumerate(results) if item["status"] == "Published"), None
        )
        if published_processed_dataset_location and published_processed_dataset_location != 0:
            published_processed_dataset = results.pop(published_processed_dataset_location)
            results.insert(0, published_processed_dataset)


def group_verify_properties_list(normalized_class="All", properties=[]):
    """Separates neo4j properties from transient ones. More over, buckets neo4j properties that are
     either json_string or list to allow them to be handled via apoc.convert.* functions.
     Will also gather specific property dependencies via a `dependency_properties` list setting in the schema yaml.
     Also filters out any unknown properties.

    Parameters
    ----------
    normalized_class : str
        the normalized entity type of the entity
    properties : List[str]
        A list of property keys to filter in or out from the normalized results, default is []

    Returns
    -------
    PropertyGroups
        An instance of simple class PropertyGroups containing entity and activity neo4j, trigger, dependency properties
    """
    # Determine the schema section based on class
    global _schema
    global _schema_properties

    defaults = get_schema_defaults([])

    if len(properties) == 1 and properties[0] in defaults:
        return PropertyGroups(properties)

    neo4j_fields = set()
    trigger_fields = set()
    json_fields = set()
    list_fields = set()
    dependencies = set()
    activity_fields = []
    activity_json_fields = []
    activity_list_fields = []

    activity_properties = _schema["ACTIVITIES"]["Activity"].get("properties", {})

    for p in properties:
        if p in _schema_properties:
            if "trigger" in _schema_properties[p] and (
                len(_schema_properties[p]["trigger"])
                or normalized_class in _schema_properties[p]["trigger"]
            ):
                trigger_fields.add(p)

            if "neo4j" in _schema_properties[p] and (
                len(_schema_properties[p]["neo4j"])
                or normalized_class in _schema_properties[p]["neo4j"]
            ):
                neo4j_fields.add(p)

                if (
                    len(_schema_properties[p]["json_string"])
                    or normalized_class in _schema_properties[p]["json_string"]
                ):
                    json_fields.add(p)
                if (
                    len(_schema_properties[p]["list"])
                    or normalized_class in _schema_properties[p]["list"]
                ):
                    list_fields.add(p)

            if "dependencies" in _schema_properties[p] and len(
                _schema_properties[p]["dependencies"]
            ):
                dependencies.update(list(_schema_properties[p]["dependencies"]))

        if p in activity_properties:
            activity_fields.append(p)
            # TODO: To add support, if ever became a requirement, would need to grab trigger and dependencies and add to return instance below
            # trigger fields would also have to be appended in calls: get_complete_entities_list(PropertyGroups.trigger + PropertyGroups.activity_trigger)
            # if 'on_read_trigger' in activity_properties[p]:
            #     activity_trigger.append(p)
            # activity_dependencies.update(activity_properties[p].get('dependency_properties', []))

            if "type" in activity_properties[p]:
                if activity_properties[p]["type"] == "json_string":
                    activity_json_fields.append(p)
                if activity_properties[p]["type"] == "list":
                    activity_list_fields.append(p)

    return PropertyGroups(
        list(neo4j_fields),
        list(trigger_fields),
        list(json_fields),
        list(list_fields),
        dep=list(dependencies),
        activity_neo4j=activity_fields,
        activity_json=activity_json_fields,
        activity_list=activity_list_fields,
    )


def exclude_properties_from_response(excluded_fields, output_dict):
    """Removes specified fields from an existing dictionary.

    Parameters
    ----------
    excluded_fields : list
        A list of the fields to be excluded
    output_dict : dictionary
        A dictionary representing the data to be modified

    Returns
    -------
    dict
        The modified data with removed fields
    """

    def delete_nested_field(data, nested_path):
        if isinstance(nested_path, dict):
            for key, value in nested_path.items():
                if key in data:
                    if isinstance(value, list):
                        for nested_field in value:
                            if isinstance(nested_field, dict):
                                if isinstance(data[key], list):
                                    for item in data[key]:
                                        delete_nested_field(item, nested_field)
                                else:
                                    delete_nested_field(data[key], nested_field)

                            elif isinstance(data[key], list):
                                for item in data[key]:
                                    if nested_field in item:
                                        del item[nested_field]

                            elif nested_field in data[key]:
                                del data[key][nested_field]
                    elif isinstance(value, dict):
                        delete_nested_field(data[key], value)

        elif nested_path in data:
            if isinstance(data[nested_path], list):
                for item in data[nested_path]:
                    if nested_path in item:
                        del item[nested_path]
            else:
                del data[nested_path]

    for field in excluded_fields:
        delete_nested_field(output_dict, field)

    return output_dict


def generate_triggered_data(
    trigger_type: TriggerTypeEnum,
    normalized_class,
    user_token,
    existing_data_dict,
    new_data_dict,
    properties_to_filter=[],
    is_include_action=False,
    bulk_trigger_manager_instance: BulkTriggersManager = None,
):
    """
    Generating triggered data based on the target events and methods

    Parameters
    ----------
    trigger_type : str
        One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
    normalized_class : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    user_token: str
        The user's globus nexus token, 'on_read_trigger' doesn't really need this
    existing_data_dict : dict
        A dictionary that contains existing entity data
    new_data_dict : dict
        A dictionary that contains incoming entity data
    properties_to_filter : list
        Any properties to skip or include when running triggers.
    is_include_action : bool
        Whether to include or exclude the properties listed in properties_to_skip
    bulk_trigger_manager_instance : BulkTriggersManager
         Instance of helper class for managing bulk triggers

    Returns
    -------
    dict
        A dictionary of trigger event methods generated data
    """

    global _schema

    schema_section = None

    # A bit validation
    validate_trigger_type(trigger_type)
    # Use validate_normalized_class instead of validate_normalized_entity_type()
    # to allow "Activity"
    validate_normalized_class(normalized_class)

    # Determine the schema section based on class
    if normalized_class == "Activity":
        schema_section = _schema["ACTIVITIES"]
    elif normalized_class == "Lab":
        schema_section = _schema["AGENTS"]
    else:
        schema_section = _schema["ENTITIES"]

    # The ordering of properties of this entity class defined in the yaml schema
    # decides the ordering of which trigger method gets to run first
    properties = get_entity_properties(schema_section, normalized_class)

    # Set each property value and put all resulting data into a dictionary for:
    # before_create_trigger|before_update_trigger|on_read_trigger
    # No property value to be set for: after_create_trigger|after_update_trigger
    trigger_generated_data_dict = {}
    for key in properties:
        # Among those properties that have the target trigger type,
        # we can skip the ones specified in the `properties_to_skip` by not running their triggers
        if (trigger_type.value in properties[key]) and (
            (key not in properties_to_filter and is_include_action is False)
            or (key in properties_to_filter and is_include_action)
        ):
            # 'after_create_trigger' and 'after_update_trigger' don't generate property values
            # E.g., create relationships between nodes in neo4j
            # So just return the empty trigger_generated_data_dict
            if trigger_type in [TriggerTypeEnum.AFTER_CREATE, TriggerTypeEnum.AFTER_UPDATE]:
                # Only call the triggers if the propery key presents from the incoming data
                # E.g., 'direct_ancestor_uuid' for Sample, 'dataset_uuids' for Collection
                # This `existing_data_dict` is the newly created or updated entity dict
                if key in existing_data_dict:
                    trigger_method_name = properties[key][trigger_type.value]

                    try:
                        # Get the target trigger method defined in the schema_triggers.py module
                        trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                        logger.info(
                            f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}"
                        )

                        # No return values for 'after_create_trigger' and 'after_update_trigger'
                        # because the property value is already set and stored in neo4j
                        # Normally it's building linkages between entity nodes
                        # Use {} since no incoming new_data_dict
                        trigger_method_to_call(
                            key, normalized_class, user_token, existing_data_dict, {}
                        )
                    except Exception:
                        msg = (
                            "Failed to call the "
                            + trigger_type.value
                            + " method: "
                            + trigger_method_name
                        )
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)

                        if trigger_type == TriggerTypeEnum.AFTER_CREATE:
                            raise schema_errors.AfterCreateTriggerException
                        elif trigger_type == TriggerTypeEnum.AFTER_UPDATE:
                            raise schema_errors.AfterUpdateTriggerException
            elif trigger_type in [TriggerTypeEnum.BEFORE_UPDATE]:
                # IMPORTANT! Call the triggers for the properties:
                # Case 1: specified in request JSON to be updated explicitly
                # Case 2: defined as `auto_update: true` in the schema yaml, meaning will always be updated if the entity gets updated
                if (key in new_data_dict) or (
                    ("auto_update" in properties[key]) and properties[key]["auto_update"]
                ):
                    trigger_method_name = properties[key][trigger_type.value]

                    try:
                        trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                        logger.info(
                            f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}"
                        )

                        # Will set the trigger return value as the property value by default
                        # Unless the return value is to be assigned to another property different target key

                        # the updated_peripherally tag is a temporary measure to correctly handle any attributes
                        # which are potentially updated by multiple triggers
                        # we keep the state of the attribute(s) directly in the trigger_generated_data_dict
                        # dictionary, which is used to track and save all changes from triggers in general
                        # the trigger methods for the 'updated_peripherally' attributes take an extra argument,
                        # the trigger_generated_data_dict, and must initialize this dictionary with the value for
                        # the attribute from the existing_data_dict as well as make any updates to this attribute
                        # within this dictionary and return it so it can be saved in the scope of this loop and
                        # passed to other 'updated_peripherally' triggers
                        if (
                            "updated_peripherally" in properties[key]
                            and properties[key]["updated_peripherally"]
                        ):
                            trigger_generated_data_dict = trigger_method_to_call(
                                key,
                                normalized_class,
                                user_token,
                                existing_data_dict,
                                new_data_dict,
                                trigger_generated_data_dict,
                            )
                        else:
                            target_key, target_value = trigger_method_to_call(
                                key, normalized_class, user_token, existing_data_dict, new_data_dict
                            )
                            trigger_generated_data_dict[target_key] = target_value

                            # Meanwhile, set the original property as None if target_key is different
                            # This is especially important when the returned target_key is different from the original key
                            # Because we'll be merging this trigger_generated_data_dict with the original user input
                            # and this will overwrite the original key so it doesn't get stored in Neo4j
                            if key != target_key:
                                trigger_generated_data_dict[key] = None
                    # If something wrong with file upload
                    except schema_errors.FileUploadException as e:
                        msg = (
                            f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                        )
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)
                        raise schema_errors.FileUploadException(e)
                    except Exception:
                        msg = (
                            f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                        )
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)

                        # We can't create/update the entity
                        # without successfully executing this trigger method
                        raise schema_errors.BeforeUpdateTriggerException
            elif (
                trigger_type in [TriggerTypeEnum.ON_READ]
                and TriggerTypeEnum.ON_BULK_READ.value in properties[key]
                and bulk_trigger_manager_instance
            ):
                trigger_method_name = properties[key][TriggerTypeEnum.ON_BULK_READ.value]
                storage_key = f"{key}_{trigger_method_name}"
                uuid = existing_data_dict["uuid"]
                bulk_trigger_manager_instance.set_item_to_group_by_key(storage_key, uuid)
                bulk_trigger_manager_instance.set_reference(
                    key=storage_key, item=[key, trigger_method_name]
                )
            else:
                # Handling of all other trigger types: before_create_trigger|on_read_trigger
                trigger_method_name = properties[key][trigger_type.value]

                try:
                    trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                    logger.info(
                        f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}"
                    )

                    # Will set the trigger return value as the property value by default
                    # Unless the return value is to be assigned to another property different target key

                    # the updated_peripherally tag is a temporary measure to correctly handle any attributes
                    # which are potentially updated by multiple triggers
                    # we keep the state of the attribute(s) directly in the trigger_generated_data_dict
                    # dictionary, which is used to track and save all changes from triggers in general
                    # the trigger methods for the 'updated_peripherally' attributes take an extra argument,
                    # the trigger_generated_data_dict, and must initialize this dictionary with the value for
                    # the attribute from the existing_data_dict as well as make any updates to this attribute
                    # within this dictionary and return it so it can be saved in the scope of this loop and
                    # passed to other 'updated_peripherally' triggers
                    if (
                        "updated_peripherally" in properties[key]
                        and properties[key]["updated_peripherally"]
                    ):
                        trigger_generated_data_dict = trigger_method_to_call(
                            key,
                            normalized_class,
                            user_token,
                            existing_data_dict,
                            new_data_dict,
                            trigger_generated_data_dict,
                        )
                    else:
                        target_key, target_value = trigger_method_to_call(
                            key, normalized_class, user_token, existing_data_dict, new_data_dict
                        )
                        if target_value is not None:
                            trigger_generated_data_dict[target_key] = target_value

                        # Meanwhile, set the original property as None if target_key is different
                        # This is especially important when the returned target_key is different from the original key
                        # Because we'll be merging this trigger_generated_data_dict with the original user input
                        # and this will overwrite the original key so it doesn't get stored in Neo4j
                        if key != target_key:
                            trigger_generated_data_dict[key] = None
                except schema_errors.NoDataProviderGroupException:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.NoDataProviderGroupException
                except schema_errors.MultipleDataProviderGroupException:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.MultipleDataProviderGroupException
                except schema_errors.UnmatchedDataProviderGroupException:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.UnmatchedDataProviderGroupException
                # If something wrong with file upload
                except schema_errors.FileUploadException as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.FileUploadException(e)
                # Certain requirements were not met to create this triggered field
                except schema_errors.InvalidPropertyRequirementsException:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                except Exception:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    if trigger_type == TriggerTypeEnum.BEFORE_CREATE:
                        # We can't create/update the entity
                        # without successfully executing this trigger method
                        raise schema_errors.BeforeCreateTriggerException
                    else:
                        # Assign the error message as the value of this property
                        # No need to raise exception
                        trigger_generated_data_dict[key] = msg

    # Return after for loop
    return trigger_generated_data_dict


"""
Filter out the merged dict by getting rid of properties with None values
This method is used by get_complete_entity_result() for the 'on_read_trigger'

Parameters
----------
merged_dict : dict
    A merged dict that may contain properties with None values

Returns
-------
dict
    A filtered dict that removed all properties with None values
"""


def remove_none_values(merged_dict):
    filtered_dict = {}
    for k, v in merged_dict.items():
        # Only keep the properties whose value is not None
        if v is not None:
            filtered_dict[k] = v

    return filtered_dict


def remove_transient_and_none_values(provenance_type, merged_dict, normalized_entity_type):
    """
    Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    and properties with None value
    Meaning the returned target property key is different from the original key
    in the trigger method, e.g., Source.image_files_to_add

    Parameters
    ----------
    merged_dict : dict
        A merged dict that may contain properties with None values
    normalized_entity_type : str
        One of the normalized entity types: Dataset, Collection, Sample, Source, Upload, Publication

    Returns
    -------
    dict
        A filtered dict that removed all transient properties and the ones with None values
    """
    global _schema

    properties = get_entity_properties(_schema[provenance_type], normalized_entity_type)

    filtered_dict = {}
    for k, v in merged_dict.items():
        # Only keep the properties that don't have `transitent` flag or are marked as `transitent: false`
        # and at the same time the property value is not None
        if normalized_entity_type == "Sample" or normalized_entity_type == "Source":
            if k != "protocol_url":
                if (
                    ("transient" not in properties[k])
                    or ("transient" in properties[k] and not properties[k]["transient"])
                ) and (v is not None):
                    filtered_dict[k] = v
        else:
            if (
                ("transient" not in properties[k])
                or ("transient" in properties[k] and not properties[k]["transient"])
            ) and (v is not None):
                filtered_dict[k] = v

    return filtered_dict


def get_complete_entity_result(
    token,
    entity_dict,
    properties_to_filter=[],
    is_include_action=False,
    use_memcache=True,
    bulk_trigger_manager_instance: BulkTriggersManager = None,
):
    """
    Generate the complete entity record as well as result filtering for response

    Parameters
    ----------
    token: str
        Either the user's globus nexus token or the internal token
    entity_dict : dict
        The entity dict based on neo4j record
    properties_to_filter : list
        Any properties to skip running triggers
    is_include_action : bool
    use_memcache : bool
    bulk_trigger_manager_instance : BulkTriggersManager

    Returns
    -------
    dict
        A dictionary of complete entity with all the generated 'on_read_trigger' data
    """
    global _memcached_client
    global _memcached_prefix

    complete_entity = {}

    # In case entity_dict is None or
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ("entity_type" in entity_dict) and ("uuid" in entity_dict):
        entity_uuid = entity_dict["uuid"]
        entity_type = entity_dict["entity_type"]
        cache_result = None

        # Need both client and prefix when fetching the cache
        # Do NOT fetch cache if properties_to_skip is specified or use_memcache is False
        if _memcached_client and _memcached_prefix and (not properties_to_filter and use_memcache):
            cache_key = f"{_memcached_prefix}_complete_{entity_uuid}"
            cache_result = _memcached_client.get(cache_key)

        # Use the cached data if found and still valid
        # Otherwise, calculate and add to cache
        if cache_result is None:
            if _memcached_client and _memcached_prefix:
                logger.info(
                    f"Cache of complete entity of {entity_type} {entity_uuid} not found or expired at time {datetime.now()}"
                )

            # No error handling here since if a 'on_read_trigger' method fails,
            # the property value will be the error message
            # Pass {} since no new_data_dict for 'on_read_trigger'
            generated_on_read_trigger_data_dict = generate_triggered_data(
                trigger_type=TriggerTypeEnum.ON_READ,
                normalized_class=entity_type,
                user_token=token,
                existing_data_dict=entity_dict,
                new_data_dict={},
                properties_to_filter=properties_to_filter,
                is_include_action=is_include_action,
                bulk_trigger_manager_instance=bulk_trigger_manager_instance,
            )

            # Merge the entity info and the generated on read data into one dictionary
            complete_entity_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

            # Remove properties of None value
            complete_entity = remove_none_values(complete_entity_dict)

            # Need both client and prefix when creating the cache
            # Do NOT cache when properties_to_skip is specified
            if (
                _memcached_client
                and _memcached_prefix
                and (not properties_to_filter and use_memcache)
            ):
                logger.info(
                    f"Creating complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}"
                )

                cache_key = f"{_memcached_prefix}_complete_{entity_uuid}"
                _memcached_client.set(
                    cache_key, complete_entity, expire=SchemaConstants.MEMCACHED_TTL
                )

                logger.debug(
                    f"Following is the complete {entity_type} cache created at time {datetime.now()} using key {cache_key}:"
                )
                logger.debug(complete_entity)
        else:
            logger.info(
                f"Using complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}"
            )
            logger.debug(cache_result)

            complete_entity = cache_result
    else:
        # Just return the original entity_dict otherwise
        complete_entity = entity_dict

    # One final return
    return complete_entity


def get_index_metadata(token, entity_dict, properties_to_skip=[]):
    """
    Generate the entity metadata by reading Neo4j data and only running triggers for data which will go into an
    OpenSearch document. Any data from Neo4j which will not go into the OSS document must also be removed e.g.
    local_directory_rel_path.

    Parameters
    ----------
    token: str
        Either the user's globus nexus token or the internal token
    entity_dict : dict
        The entity dict based on neo4j record
    properties_to_skip : list
        Any properties to skip running triggers

    Returns
    -------
    dict
        A dictionary of metadata to be included in an OpenSearch index document for the entity.
    """
    metadata_dict = _get_metadata_result(
        token=token,
        entity_dict=entity_dict,
        metadata_scope=MetadataScopeEnum.INDEX,
        properties_to_skip=properties_to_skip,
    )
    return metadata_dict


def _get_metadata_result(
    token, entity_dict, metadata_scope: MetadataScopeEnum, properties_to_skip=[]
):
    """
    Generate the entity metadata by reading Neo4j data and appropriate triggers based upon the scope of
    metadata requested e.g. complete data for a another service, indexing data for an OpenSearch document, etc.

    Parameters
    ----------
    token: str
        Either the user's globus nexus token or the internal token
    entity_dict : dict
        The entity dict based on neo4j record
    metadata_scope:
        A recognized scope from the SchemaConstants, controlling the triggers which are fired and elements
        from Neo4j which are retained.
    properties_to_skip : list
        Any properties to skip running triggers

    Returns
    -------
    dict
        A dictionary of metadata appropriate for the metadata_scope argument value.
    """
    global _memcached_client
    global _memcached_prefix

    complete_entity = {}

    # In case entity_dict is None or
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ("entity_type" in entity_dict) and ("uuid" in entity_dict):
        entity_uuid = entity_dict["uuid"]
        entity_type = entity_dict["entity_type"]
        cache_result = None

        # Need both client and prefix when fetching the cache
        # Do NOT fetch cache if properties_to_skip is specified
        if _memcached_client and _memcached_prefix and (not properties_to_skip):
            cache_key = f"{_memcached_prefix}_complete_index_{entity_uuid}"
            cache_result = _memcached_client.get(cache_key)

        # Use the cached data if found and still valid
        # Otherwise, calculate and add to cache
        if cache_result is None:
            if _memcached_client and _memcached_prefix:
                logger.info(
                    f"Cache of complete entity of {entity_type} {entity_uuid} not found or expired at time {datetime.now()}"
                )

            if metadata_scope == MetadataScopeEnum.COMPLETE:
                # No error handling here since if a 'on_read_trigger' method fails,
                # the property value will be the error message
                # Pass {} since no new_data_dict for 'on_read_trigger'
                # generated_on_read_trigger_data_dict = generate_triggered_data('on_read_trigger', entity_type, token,
                #                                                              entity_dict, {}, properties_to_skip)
                generated_on_read_trigger_data_dict = generate_triggered_data(
                    trigger_type=TriggerTypeEnum.ON_READ,
                    normalized_class=entity_type,
                    user_token=token,
                    existing_data_dict=entity_dict,
                    new_data_dict={},
                    properties_to_filter=properties_to_skip,
                )

                # Merge the entity info and the generated on read data into one dictionary
                complete_entity_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

                # Remove properties of None value
                metadata_dict = remove_none_values(complete_entity_dict)
            elif metadata_scope == MetadataScopeEnum.INDEX:
                # No error handling here since if a 'on_index_trigger' method fails,
                # the property value will be the error message
                # Pass {} since no new_data_dict for 'on_index_trigger'
                generated_on_index_trigger_data_dict = generate_triggered_data(
                    trigger_type=TriggerTypeEnum.ON_INDEX,
                    normalized_class=entity_type,
                    user_token=token,
                    existing_data_dict=entity_dict,
                    new_data_dict={},
                    properties_to_filter=properties_to_skip,
                )

                # Merge the entity info and the generated on read data into one dictionary
                complete_entity_dict = {**entity_dict, **generated_on_index_trigger_data_dict}

                # Remove properties of None value
                metadata_dict = remove_none_values(complete_entity_dict)
            else:
                # Merge the entity info and the generated on read data into one dictionary
                metadata_dict = {**entity_dict}

            # Need both client and prefix when creating the cache
            # Do NOT cache when properties_to_skip is specified
            if _memcached_client and _memcached_prefix and (not properties_to_skip):
                logger.info(
                    f"Creating complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}"
                )

                cache_key = f"{_memcached_prefix}_complete_index_{entity_uuid}"
                _memcached_client.set(
                    cache_key, metadata_dict, expire=SchemaConstants.MEMCACHED_TTL
                )

                logger.debug(
                    f"Following is the complete {entity_type} cache created at time {datetime.now()} using key {cache_key}:"
                )
                logger.debug(metadata_dict)
        else:
            logger.info(
                f"Using complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}"
            )
            logger.debug(cache_result)

            metadata_dict = cache_result
    else:
        # Just return the original entity_dict otherwise
        metadata_dict = entity_dict

    # One final return
    return metadata_dict


def get_complete_entities_list(
    token,
    entities_list,
    properties_to_filter: List = [],
    is_include_action=False,
    use_memcache=True,
):
    """
    Generate the complete entity records as well as result filtering for response

    Parameters
    ----------
    token: str
        Either the user's globus nexus token or the internal token
    entities_list : list
        A list of entity dictionaries
    properties_to_filter : list
        Any properties to skip running triggers
    is_include_action : bool
        Whether to include or exclude the listed properties
    use_memcache : bool
        Whether to use a cached result or not

    Returns
    -------
    list
        A list a complete entity dictionaries with all the normalized information
    """
    bulk_trigger_manager_instance = BulkTriggersManager()
    complete_entities_list = []

    for entity_dict in entities_list:
        complete_entity_dict = get_complete_entity_result(
            token,
            entity_dict,
            properties_to_filter,
            is_include_action=is_include_action,
            use_memcache=use_memcache,
            bulk_trigger_manager_instance=bulk_trigger_manager_instance,
        )
        complete_entities_list.append(complete_entity_dict)

    final_result = handle_bulk_triggers(
        token, complete_entities_list, bulk_trigger_manager_instance=bulk_trigger_manager_instance
    )
    return final_result


def handle_bulk_triggers(token, entities_list, bulk_trigger_manager_instance: BulkTriggersManager):
    """

    Parameters
    ----------
    token : str
        Either the user's globus nexus token or the internal token
    entities_list : List[dict]
        The list to perform triggers on
    bulk_trigger_manager_instance : BulkTriggersManager
         Instance of helper class for managing bulk triggers

    Returns
    -------
    List[dict]
        A list a complete entity dictionaries with updated bulk triggers

    """

    if bulk_trigger_manager_instance.groups != {}:
        bulk_trigger_manager_instance.build_lists_index_references(entities_list)

        for storage_key in bulk_trigger_manager_instance.groups:
            trigger_method_name = ""
            try:
                trigger_method_name = bulk_trigger_manager_instance.get_trigger_method_name(
                    storage_key
                )
                trigger_method_to_call = getattr(schema_bulk_triggers, trigger_method_name)
                trigger_method_to_call(
                    token, bulk_trigger_manager_instance, storage_key, entities_list
                )

                logger.info(f"To run {TriggerTypeEnum.ON_BULK_READ.value}: {trigger_method_name}")

            except Exception as ex:
                msg = f"Failed to call the {TriggerTypeEnum.ON_BULK_READ.value} method: {trigger_method_name} \n {str(ex)}"
                # Log the full stack trace, prepend a line with our message
                logger.exception(msg)
    return entities_list


def normalize_activity_result_for_response(activity_dict, properties_to_exclude=[]):
    """
    Normalize the activity result by filtering out properties that are not defined in the yaml schema
    and the ones that are marked as `exposed: false` prior to sending the response

    Parameters
    ----------
    activity_dict : dict
        A dictionary that contains all activity details
    properties_to_exclude : list
        Any additional properties to exclude from the response

    Returns
    -------
    dict
        A dictionary of activity information with keys that are all normalized
    """
    global _schema

    properties = _schema["ACTIVITIES"]["Activity"]["properties"]

    normalized_activity = {}
    for key in activity_dict:
        # Only return the properties defined in the schema yaml
        # Exclude additional properties if specified
        if (key in properties) and (key not in properties_to_exclude):
            # By default, all properties are exposed
            # It's possible to see `exposed: true`
            if ("exposed" not in properties[key]) or (
                ("exposed" in properties[key]) and properties[key]["exposed"]
            ):
                # Add to the normalized_activity dict
                normalized_activity[key] = activity_dict[key]

    return normalized_activity


def normalize_object_result_for_response(
    provenance_type="ENTITIES",
    entity_dict=None,
    property_groups: PropertyGroups = PropertyGroups(),
    is_include_action=True,
    is_strict=False,
):
    """

    Parameters
    ----------
    entity_dict : dict
        The entity dictionary to normalize
    property_groups : PropertyGroups
        A list of property keys to filter in or out from the normalized results, default is []
    is_include_action : bool
        Whether to include or exclude the listed properties
    provenance_type : str
        The provenance type of the object
    is_strict : bool
        Determines whether to liberally return other exposed properties not necessarily listed in PropertyGroups instance

    Returns
    -------

    """
    if entity_dict is None or entity_dict == {}:
        return {}

    global _schema

    normalized_entity = {}
    properties = []
    activity_properties = _schema["ACTIVITIES"]["Activity"]["properties"]
    properties_to_filter = list(
        set(
            property_groups.neo4j
            + property_groups.trigger
            + property_groups.activity_neo4j
            + property_groups.activity_trigger
        )
    )

    check_activity_list = not is_strict
    if len(property_groups.activity_neo4j + property_groups.activity_trigger) > 0:
        check_activity_list = True

    # In case entity_dict is None or
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ("entity_type" in entity_dict):
        normalized_entity_type = entity_dict["entity_type"]
        properties = get_entity_properties(_schema[provenance_type], normalized_entity_type)
    else:
        if provenance_type == "ENTITIES":
            logger.error(
                f"Unable to normalize object result with"
                f" entity_dict={str(entity_dict)} and"
                f" provenance_type={provenance_type}."
            )
            raise schema_errors.SchemaValidationException(
                "Unable to normalize object, missing entity_type."
            )

    for key in entity_dict:
        _key = key.replace("activity_", "")
        # Only return the properties defined in the schema yaml
        # Exclude additional properties if specified
        if (key in properties) or (check_activity_list and (_key in activity_properties)):
            if (
                (is_include_action and _key in properties_to_filter)
                or (is_include_action is False and _key not in properties_to_filter)
                or (key in get_schema_defaults())
                # By default, all properties are exposed
                # It's possible to see `exposed: true`
                or (not is_strict and key in properties and properties[key].get("exposed", True))
                # any activity properties in the dict will need to be returned even if not listed in PropertyGroups
                or (_key in activity_properties and activity_properties[_key].get("exposed", True))
            ):

                if entity_dict[key] and (
                    _key in properties and properties[_key]["type"] in ["list", "json_string"]
                ):
                    # Safely evaluate a string containing a Python dict or list literal
                    # Only convert to Python list/dict when the string literal is not empty
                    # instead of returning the json-as-string or array-as-string
                    entity_dict[key] = get_as_dict(entity_dict[key])

                # Add the target key with correct value of data type to the normalized_entity dict
                normalized_entity[key] = entity_dict[key]

                # Final step: remove properties with empty string value, empty dict {}, and empty list []
                if isinstance(normalized_entity[key], (str, dict, list)) and (
                    not normalized_entity[key]
                ):
                    normalized_entity.pop(key)

    return normalized_entity


def normalize_entities_list_for_response(
    entities_list: List,
    property_groups: PropertyGroups = PropertyGroups(),
    is_include_action=True,
    is_strict=False,
):
    """

    Parameters
    ----------
    entities_list : List[dict]
        List of entities to normalize
    property_groups : PropertyGroups
        A list of property keys to filter in or out from the normalized results, default is []
    is_include_action : bool
        Whether to include or exclude the listed properties
    is_strict : bool
        Determines whether to liberally return other exposed properties not necessarily listed in PropertyGroups instance

    Returns
    -------

    """
    if len(entities_list) <= 0:
        return []

    if isinstance(entities_list[0], str):
        return entities_list

    normalized_entities_list = []

    for entity_dict in entities_list:
        normalized_entity_dict = normalize_object_result_for_response(
            entity_dict=entity_dict,
            property_groups=property_groups,
            is_include_action=is_include_action,
            is_strict=is_strict,
        )
        normalized_entities_list.append(normalized_entity_dict)

    return normalized_entities_list


def remove_unauthorized_fields_from_response(entities_list: List, unauthorized: bool):
    """
    If a user is unauthorized fields listed in excluded_properties_from_public_response under the respective
    schema yaml will be removed from the results

    Parameters
    ----------
    entities_list : List[dict]
        The list to be potentially filtered
    unauthorized : bool
        Whether user is authorized or not

    Returns
    -------
    List[dict]
    """
    if len(entities_list) > 0 and isinstance(entities_list[0], str):
        return entities_list

    if unauthorized:
        filtered_final_result = []
        for entity in entities_list:
            if isinstance(entity, dict):
                entity_type = entity.get("entity_type")
                fields_to_exclude = get_fields_to_exclude(entity_type)
                filtered_entity = exclude_properties_from_response(fields_to_exclude, entity)
                filtered_final_result.append(filtered_entity)
            else:
                filtered_final_result.append(entity)
        return filtered_final_result
    else:
        return entities_list


def validate_json_data_against_schema(
    provenance_type, json_data_dict, normalized_entity_type, existing_entity_dict={}
):
    """
    Validate json data from user request against the schema

    Parameters
    ----------
    provenance_type : str
    json_data_dict : dict
        The json data dict from user request
    normalized_entity_type : str
        One of the normalized entity types: Dataset, Collection, Sample, Source, Upload, Publication
    existing_entity_dict : dict
        Entity dict for creating new entity, otherwise pass in the existing entity dict for update validation
    """

    global _schema

    properties = get_entity_properties(_schema[provenance_type], normalized_entity_type)
    schema_keys = properties.keys()
    json_data_keys = json_data_dict.keys()
    separator = ", "

    # Check if keys in request json are supported
    unsupported_keys = []
    for key in json_data_keys:
        if key not in schema_keys:
            if normalized_entity_type == "Sample" or normalized_entity_type == "Source":
                if key != "protocol_url":
                    unsupported_keys.append(key)
            else:
                unsupported_keys.append(key)

    if len(unsupported_keys) > 0:
        # No need to log the validation errors
        raise schema_errors.SchemaValidationException(
            f"Unsupported keys in request json: {separator.join(unsupported_keys)}"
        )

    # Check if keys in request json are the ones to be auto generated
    # Disallow direct creation via POST, but allow update via PUT
    generated_keys = []
    if not existing_entity_dict:
        for key in json_data_keys:
            if normalized_entity_type == "Sample" or normalized_entity_type == "Source":
                if key != "protocol_url":
                    if ("generated" in properties[key]) and properties[key]["generated"]:
                        if properties[key]:
                            generated_keys.append(key)
            else:
                if ("generated" in properties[key]) and properties[key]["generated"]:
                    if properties[key]:
                        generated_keys.append(key)

    if len(generated_keys) > 0:
        # No need to log the validation errors
        raise schema_errors.SchemaValidationException(
            f"Auto generated keys are not allowed in request json: {separator.join(generated_keys)}"
        )

    # Checks for entity update via HTTP PUT
    if existing_entity_dict:
        # Check if keys in request json are immutable
        immutable_keys = []
        for key in json_data_keys:
            if normalized_entity_type == "Sample" or normalized_entity_type == "Source":
                if key != "protocol_url":
                    if ("immutable" in properties[key]) and properties[key]["immutable"]:
                        if properties[key]:
                            immutable_keys.append(key)
            else:
                if ("immutable" in properties[key]) and properties[key]["immutable"]:
                    if properties[key]:
                        immutable_keys.append(key)

        if len(immutable_keys) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(
                f"Immutable keys are not allowed in request json: {separator.join(immutable_keys)}"
            )

    # Check if any schema keys that are `required_on_create: true` but missing from POST request on creating new entity
    # No need to check on entity update
    if not existing_entity_dict:
        missing_required_keys_on_create = []
        empty_value_of_required_keys_on_create = []
        for key in schema_keys:
            # By default, the schema treats all entity properties as optional on creation.
            # Use `required_on_create: true` to mark a property as required for creating a new entity
            if (
                ("required_on_create" in properties[key])
                and properties[key]["required_on_create"]
                and ("trigger" not in properties[key])
            ):
                if key not in json_data_keys:
                    missing_required_keys_on_create.append(key)
                else:
                    # Empty values or None(null in request json) of required keys are invalid too
                    # The data type check will be handled later regardless of it's required or not
                    if (
                        (json_data_dict[key] is None)
                        or (
                            isinstance(json_data_dict[key], (list, dict))
                            and (not json_data_dict[key])
                        )
                        or (
                            isinstance(json_data_dict[key], str)
                            and (not json_data_dict[key].strip())
                        )
                    ):
                        empty_value_of_required_keys_on_create.append(key)

        if len(missing_required_keys_on_create) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(
                f"Missing required keys in request json: {separator.join(missing_required_keys_on_create)}"
            )

        if len(empty_value_of_required_keys_on_create) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(
                f"Required keys in request json with empty values: {separator.join(empty_value_of_required_keys_on_create)}"
            )

    # Verify data type of each key
    invalid_data_type_keys = []
    for key in json_data_keys:
        if normalized_entity_type == "Sample" or normalized_entity_type == "Source":
            if key != "protocol_url":
                # boolean starts with bool, string starts with str, integer starts with int, list is list
                if (properties[key]["type"] in ["string", "integer", "list", "boolean"]) and (
                    not properties[key]["type"].startswith(type(json_data_dict[key]).__name__)
                ):
                    invalid_data_type_keys.append(key)
                # Handling json_string as dict
                if (properties[key]["type"] == "json_string") and (
                    not isinstance(json_data_dict[key], dict)
                ):
                    invalid_data_type_keys.append(key)
        else:
            if (properties[key]["type"] in ["string", "integer", "list", "boolean"]) and (
                not properties[key]["type"].startswith(type(json_data_dict[key]).__name__)
            ):
                invalid_data_type_keys.append(key)
            if (properties[key]["type"] == "json_string") and (
                not isinstance(json_data_dict[key], dict)
            ):
                invalid_data_type_keys.append(key)

    if len(invalid_data_type_keys) > 0:
        # No need to log the validation errors
        raise schema_errors.SchemaValidationException(
            f"Keys in request json with invalid data types: {separator.join(invalid_data_type_keys)}"
        )


def execute_entity_level_validator(validator_type, normalized_entity_type, request):
    """
    Execute the entity level validator of the given type defined in the schema yaml
    before entity creation via POST or entity update via PUT
    Only one validator defined per given validator type, no need to support multiple validators

    Parameters
    ----------
    validator_type : str
        One of the validator types: before_entity_create_validator
    normalized_entity_type : str
        One of the normalized entity types defined in the schema yaml: Source, Sample, Dataset, Upload, Publication
    request: Flask request object
        The instance of Flask request passed in from application request
    """
    global _schema

    # A bit validation
    validate_entity_level_validator_type(validator_type)
    validate_normalized_entity_type(normalized_entity_type)

    entity = _schema["ENTITIES"][normalized_entity_type]

    entity["properties"] = get_entity_properties(_schema["ENTITIES"], normalized_entity_type)

    for key in entity:
        if validator_type == key:
            validator_method_name = entity[validator_type]

            try:
                # Get the target validator method defined in the schema_validators.py module
                validator_method_to_call = getattr(schema_validators, validator_method_name)

                logger.info(
                    f"To run {validator_type}: {validator_method_name} defined for entity {normalized_entity_type}"
                )

                validator_method_to_call(normalized_entity_type, request)
            except schema_errors.MissingApplicationHeaderException as e:
                raise schema_errors.MissingApplicationHeaderException(e)
            except schema_errors.InvalidApplicationHeaderException as e:
                raise schema_errors.InvalidApplicationHeaderException(e)
            except Exception:
                msg = f"Failed to call the {validator_type} method: {validator_method_name} defiend for entity {normalized_entity_type}"
                # Log the full stack trace, prepend a line with our message
                logger.exception(msg)


def execute_property_level_validators(
    provenance_type,
    validator_type,
    normalized_entity_type,
    request,
    existing_data_dict,
    new_data_dict,
):
    """
    Execute the property level validators defined in the schema yaml
    before property update via PUT

    Parameters
    ----------
    validator_type : str
        before_property_create_validators|before_property_update_validators (support multiple validators)
    normalized_entity_type : str
        One of the normalized entity types defined in the schema yaml: Source, Sample, Dataset, Upload, Publication
    request: Flask request object
        The instance of Flask request passed in from application request
    existing_data_dict : dict
        A dictionary that contains all existing entity properties, {} for before_property_create_validators
    new_data_dict : dict
        The json data in request body, already after the regular validations
    """
    global _schema

    # A bit validation
    validate_property_level_validator_type(validator_type)
    if provenance_type != "ACTIVITIES":
        validate_normalized_entity_type(normalized_entity_type)

    # properties = _schema[provenance_type][normalized_entity_type]['properties']

    properties = get_entity_properties(_schema[provenance_type], normalized_entity_type)

    for key in properties:
        # Only run the validators for keys present in the request json
        if (key in new_data_dict) and (validator_type in properties[key]):
            # Get a list of defined validators on this property
            validators_list = properties[key][validator_type]
            # Run each validator defined on this property
            for validator_method_name in validators_list:
                try:
                    # Get the target validator method defined in the schema_validators.py module
                    validator_method_to_call = getattr(schema_validators, validator_method_name)

                    logger.info(
                        f"To run {validator_type}: {validator_method_name} defined for entity {normalized_entity_type} on property {key}"
                    )

                    validator_method_to_call(
                        key, normalized_entity_type, request, existing_data_dict, new_data_dict
                    )
                except schema_errors.MissingApplicationHeaderException as e:
                    raise schema_errors.MissingApplicationHeaderException(e)
                except schema_errors.InvalidApplicationHeaderException as e:
                    raise schema_errors.InvalidApplicationHeaderException(e)
                except ValueError as ve:
                    raise ValueError(ve)
                except Exception as e:
                    msg = f"Failed to call the {validator_type} method: {validator_method_name} defined for entity {normalized_entity_type} on property {key}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(f"{msg}. {str(e)}")


def get_derivation_source_entity_types():
    """
    Get a list of entity types that can be used as derivation source in the schema yaml

    Returns
    -------
    list
        A list of entity types
    """

    global _schema

    derivation_source_entity_types = []
    entity_types = get_all_entity_types()
    for entity_type in entity_types:
        if _schema["ENTITIES"][entity_type]["derivation"]["source"]:
            derivation_source_entity_types.append(entity_type)

    return derivation_source_entity_types


def get_derivation_target_entity_types():
    """
    Get a list of entity types that can be used as derivation target in the schema yaml

    Returns
    -------
    list
        A list of entity types
    """
    global _schema

    derivation_target_entity_types = []
    entity_types = get_all_entity_types()
    for entity_type in entity_types:
        if _schema["ENTITIES"][entity_type]["derivation"]["target"]:
            derivation_target_entity_types.append(entity_type)

    return derivation_target_entity_types


def normalize_entity_type(entity_type):
    """
    Lowercase and capitalize the entity type string

    Parameters
    ----------
    entity_type : str
        One of the normalized entity types: Dataset, Collection, Sample, Source, Upload, Publication

    Returns
    -------
    string
        One of the normalized entity types: Dataset, Collection, Sample, Source, Upload, Publication
    """
    normalized_entity_type = entity_type.lower().capitalize()
    return normalized_entity_type


def normalize_status(status):
    """
    Lowercase and capitalize the status string unless its "qa", then make it capitalized.

    Parameters
    ----------
    status : str
        One of the status types: New|Processing|QA|Published|Error|Hold|Invalid

    Returns
    -------
    string
        One of the normalized status types: New|Processing|QA|Published|Error|Hold|Invalid
    """
    if status.lower() == "qa":
        normalized_status = "QA"
    else:
        normalized_status = status.lower().capitalize()
    return normalized_status


def normalize_document_result_for_response(
    entity_dict, properties_to_exclude=[], properties_to_include=[]
):
    """
    Normalize the entity result to got into the OpenSearch index by filtering out properties that are not defined in
    the yaml schema, properties marked as `exposed: false` in the yaml schema, and properties lacking `indexed: true`
    marking in the yaml schema.

    Parameters
    ----------
    entity_dict : dict
        Either a neo4j node converted dict or metadata dict generated from get_index_metadata()
    properties_to_exclude : list
        Any additional properties to exclude from the response

    Returns
    -------
    dict
        An entity metadata dictionary with keys that are all normalized
    """
    return _normalize_metadata(
        entity_dict=entity_dict,
        metadata_scope=MetadataScopeEnum.INDEX,
        properties_to_exclude=properties_to_exclude,
        properties_to_include=properties_to_include,
    )


def _normalize_metadata(
    entity_dict,
    metadata_scope: MetadataScopeEnum,
    properties_to_exclude=[],
    properties_to_include=[],
):
    """
    Normalize the entity result by filtering the properties to those appropriate for the
    scope of metadata requested e.g. complete data for a another service, indexing data for an OpenSearch document, etc.

    Properties that are not defined in the yaml schema and properties marked as `exposed: false` in the yaml schema are
    removed. Properties are also filter based upon the metadata_scope argument e.g. properties lacking `indexed: true`
    marking in the yaml schema are removed when `metadata_scope` has a value of `MetadataScopeEnum.INDEX`.

    Parameters
    ----------
    entity_dict : dict
        Either a neo4j node converted dict or metadata dict generated from get_index_metadata()
    metadata_scope:
        A recognized scope from the SchemaConstants, controlling the triggers which are fired and elements
        from Neo4j which are retained.  Default is MetadataScopeEnum.INDEX.
    properties_to_exclude : list
        Any additional properties to exclude from the response
    properties_to_include : list
        Any additional properties to include in the response

    Returns
    -------
    dict
        An entity metadata dictionary with keys that are all normalized appropriately for the metadata_scope argument value.
    """
    global _schema

    # When the entity_dict is unavailable or the entity was incorrectly created, do not
    # try to normalize.
    if not entity_dict or "entity_type" not in entity_dict:
        return {}

    normalized_metadata = {}

    normalized_entity_type = entity_dict["entity_type"]
    properties = get_entity_properties(_schema["ENTITIES"], normalized_entity_type)

    for key in entity_dict:
        if key in properties_to_include:
            # Add the target key with correct value of data type to the normalized_metadata dict
            normalized_metadata[key] = entity_dict[key]

            # Max Sibilla: We do not want to remove any properties as the Search API does an update on all fields and if
            # a property is missing then the Elasticsearch document can be incorrect

            # Final step: remove properties with empty string value, empty dict {}, and empty list []
            # if (isinstance(normalized_metadata[key], (str, dict, list)) and (not normalized_metadata[key])):
            #     normalized_metadata.pop(key)

        # Only return the properties defined in the schema yaml
        # Exclude additional schema yaml properties, if specified
        if key not in properties:
            # Skip Neo4j entity properties not found in the schema yaml
            continue
        if key in properties_to_exclude:
            # Skip properties if directed by the calling function
            continue
        if entity_dict[key] is None:
            # Do not include properties in the metadata if they are empty
            continue
        if "exposed" in properties[key] and properties[key]["exposed"] is False:
            # Do not include properties in the metadata if they are not exposed
            continue
        if (
            metadata_scope is MetadataScopeEnum.INDEX
            and "indexed" in properties[key]
            and properties[key]["indexed"] is False
        ):
            # Do not include properties in metadata for indexing if they are not True i.e. False or non-boolean
            continue
        if entity_dict[key] and (properties[key]["type"] in ["list", "json_string"]):
            logger.info(
                f"Executing get_as_dict() on {normalized_entity_type}.{key} of uuid: {entity_dict['uuid']}"
            )

            entity_dict[key] = get_as_dict(entity_dict[key])

        # Add the target key with correct value of data type to the normalized_entity dict
        normalized_metadata[key] = entity_dict[key]

        # Max Sibilla: We do not want to remove any properties as the Search API does an update on all fields and if
        # a property is missing then the Elasticsearch document can be incorrect

        # if (isinstance(normalized_metadata[key], (str, dict, list)) and (not normalized_metadata[key])):
        #     normalized_metadata.pop(key)

    return normalized_metadata


def validate_trigger_type(trigger_type: TriggerTypeEnum):
    """
    Validate the provided trigger type

    Parameters
    ----------
    trigger_type : str
        One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
    """
    separator = ", "

    if trigger_type not in TriggerTypeEnum:
        msg = f"Invalid trigger type: {trigger_type.value}. The trigger type must be one of the following: {separator.join([t.value for t in TriggerTypeEnum])}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)


def validate_entity_level_validator_type(validator_type):
    """
    Validate the provided entity level validator type

    Parameters
    ----------
    validator_type : str
        One of the validator types: before_entity_create_validator
    """
    accepted_validator_types = ["before_entity_create_validator"]
    separator = ", "

    if validator_type.lower() not in accepted_validator_types:
        msg = f"Invalid validator type: {validator_type}. The validator type must be one of the following: {separator.join(accepted_validator_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)


def validate_property_level_validator_type(validator_type):
    """
    Validate the provided property level validator type

    Parameters
    ----------
    validator_type : str
        One of the validator types: before_property_create_validators|before_property_update_validators
    """
    accepted_validator_types = [
        "before_property_create_validators",
        "before_property_update_validators",
    ]
    separator = ", "

    if validator_type.lower() not in accepted_validator_types:
        msg = f"Invalid validator type: {validator_type}. The validator type must be one of the following: {separator.join(accepted_validator_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)


def validate_normalized_entity_type(normalized_entity_type):
    """
    Validate the normalized entity class

    Parameters
    ----------
    normalized_entity_type : str
        The normalized entity class: Collection|Source|Sample|Dataset|Upload|Publication
    """
    separator = ", "
    accepted_entity_types = get_all_entity_types()

    # Validate provided entity_type
    if normalized_entity_type not in accepted_entity_types:
        msg = f"Invalid entity class: {normalized_entity_type}. The entity class must be one of the following: {separator.join(accepted_entity_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.InvalidNormalizedEntityTypeException(msg)


def validate_normalized_class(normalized_class):
    """
    Validate the normalized class

    Parameters
    ----------
    normalized_class : str
        The normalized class: Activity|Collection|Source|Sample|Dataset
    """
    separator = ", "
    accepted_types = get_all_types()

    # Validate provided entity_type
    if normalized_class not in accepted_types:
        msg = f"Invalid class: {normalized_class}. The class must be one of the following: {separator.join(accepted_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.InvalidNormalizedTypeException(msg)


def validate_target_entity_type_for_derivation(normalized_target_entity_type):
    """
    Validate the source and target entity types for creating derived entity

    Parameters
    ----------
    normalized_target_entity_type : str
        The normalized target entity class
    """
    separator = ", "
    accepted_target_entity_types = get_derivation_target_entity_types()

    if normalized_target_entity_type not in accepted_target_entity_types:
        abort_bad_req(
            f"Invalid target entity type specified for creating the derived entity."
            f" Accepted types: {separator.join(accepted_target_entity_types)}"
        )


def validate_source_entity_type_for_derivation(normalized_source_entity_type):
    """
    Validate the source and target entity types for creating derived entity

    Parameters
    ----------
    normalized_source_entity_type : str
        The normalized source entity class
    """
    separator = ", "
    accepted_source_entity_types = get_derivation_source_entity_types()

    if normalized_source_entity_type not in accepted_source_entity_types:
        abort_bad_req(
            f"Invalid source entity class specified for creating the derived entity."
            f" Accepted types: {separator.join(accepted_source_entity_types)}"
        )


####################################################################################################
## Other functions used in conjuction with the trigger methods
####################################################################################################


def get_user_info(request):
    """
    Get user information dict based on the http request(headers)
    The result will be used by the trigger methods

    Parameters
    ----------
    request : Flask request object
        The Flask request passed from the API endpoint

    Returns
    -------
    dict
        A dict containing all the user info

        {
            "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
            "name": "First Last",
            "iss": "https://auth.globus.org",
            "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
            "active": True,
            "nbf": 1603761442,
            "token_type": "Bearer",
            "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
            "iat": 1603761442,
            "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
            "exp": 1603934242,
            "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
            "email": "email@pitt.edu",
            "username": "username@pitt.edu",
            "snscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
        }
    """
    global _auth_helper

    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    user_info = _auth_helper.getUserInfoUsingRequest(request, True)

    logger.info("======get_user_info()======")
    logger.info(user_info)

    # For debugging purposes
    try:
        auth_helper_instance = get_auth_helper_instance()
        token = auth_helper_instance.getAuthorizationTokens(request.headers)
        groups_list = auth_helper_instance.get_user_groups_deprecated(token)

        logger.info("======Groups using get_user_groups_deprecated()======")
        logger.info(groups_list)
    except Exception:
        msg = "For debugging purposes, failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

    # It returns error response when:
    # - invalid header or token
    # - token is valid but not nexus token, can't find group info
    if isinstance(user_info, Response):
        # Bubble up the actual error message from commons
        # The Response.data returns binary string, need to decode
        msg = user_info.get_data().decode()
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise Exception(msg)

    return user_info


def get_sennet_ids(id):
    """
    Retrieve target uuid and sennet_id based on the given id

    Parameters
    ----------
    id : str
        Either the uuid or sennet_id of target entity

    Returns
    -------
    dict
        The dict returned by uuid-api that contains all the associated ids, e.g.:
        {
            "ancestor_id": "23c0ffa90648358e06b7ac0c5673ccd2",
            "ancestor_ids":[
                "23c0ffa90648358e06b7ac0c5673ccd2"
            ],
            "email": "marda@ufl.edu",
            "sn_uuid": "1785aae4f0fb8f13a56d79957d1cbedf",
            "sennet_id": "SN966.VNKN.965",
            "time_generated": "2020-10-19 15:52:02",
            "type": "SOURCE",
            "user_id": "694c6f6a-1deb-41a6-880f-d1ad8af3705f"
        }
    """
    global _uuid_api_url

    target_url = _uuid_api_url + "/uuid/" + id

    # Function cache to improve performance
    response = make_request_get(target_url, internal_token_used=True)

    # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
    response.raise_for_status()

    if response.status_code == 200:
        ids_dict = response.json()
        return ids_dict
    else:
        # uuid-api will also return 400 if the given id is invalid
        # We'll just hanle that and all other cases all together here
        msg = f"Unable to make a request to query the id via uuid-api: {id}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======get_sennet_ids() status code from uuid-api======")
        logger.debug(response.status_code)

        logger.debug("======get_sennet_ids() response text from uuid-api======")
        logger.debug(response.text)

        # Also bubble up the error message from uuid-api
        raise requests.exceptions.RequestException(response.text)


def create_sennet_ids(normalized_class, json_data_dict, user_token, user_info_dict, count=1):
    """
    Create a set of new ids for the new entity to be created

    Parameters
    ----------
    normalized_class : str
        One of the types defined in the schema yaml: Activity, Collection, Source, Sample, Dataset
    json_data_dict: dict
        The json request dict from user input, required when creating ids for Source/Sample/Dataset only
    user_token: str
        The user's globus nexus token
    user_info_dict: dict
        A dict containing all the user info, requried when creating ids for Source only:
        {
            "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
            "name": "First Last",
            "iss": "https://auth.globus.org",
            "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
            "active": True,
            "nbf": 1603761442,
            "token_type": "Bearer",
            "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
            "iat": 1603761442,
            "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
            "exp": 1603934242,
            "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
            "email": "email@pitt.edu",
            "username": "username@pitt.edu",
            "snscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
        }
    count : int
        The optional number of ids to generate. If omitted, defaults to 1

    Returns
    -------
    list
        The list of new ids dicts, the number of dicts is based on the count
    """
    global _uuid_api_url

    """
    POST arguments in json
    entity_type - required: the type of entity, SOURCE, SAMPLE, DATASET
    parent_ids - required for entity types of SAMPLE, SOURCE and DATASET
               an array of UUIDs for the ancestors of the new entity
               For SAMPLEs and SOURCEs a single uuid is required (one entry in the array)
               and multiple ids are not allowed (SAMPLEs and SOURCEs are required to
               have a single ancestor, not multiple).  For DATASETs at least one ancestor
               UUID is required, but multiple can be specified. (A DATASET can be derived
               from multiple SAMPLEs or DATASETs.)
    organ_code - required only in the case where an id is being generated for a SAMPLE that
               has a SOURCE as a direct ancestor.  Must be one of the codes from:
               https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml

    Query string (in url) arguments:
        entity_count - optional, the number of ids to generate. If omitted, defaults to 1
    """
    json_to_post = {"entity_type": normalized_class}

    # Activity and Collection don't require the `parent_ids` in request json
    if normalized_class in ["Source", "Sample", "Dataset", "Upload", "Publication"]:
        # The direct ancestor of Source and Upload must be Lab
        # The group_uuid is the Lab id in this case
        if normalized_class in ["Source", "Upload"]:
            # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that.
            # Otherwise if not set and no single "provider group" membership throws error.
            # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
            if "hmgroupids" not in user_info_dict:
                raise KeyError(
                    "Missing 'hmgroupids' key in 'user_info_dict' when calling 'create_sennet_ids()' to create new ids for this Source."
                )

            user_group_uuids = user_info_dict["hmgroupids"]

            # If group_uuid is provided by the request, use it with validation
            if "group_uuid" in json_data_dict:
                group_uuid = json_data_dict["group_uuid"]
                # Validate the group_uuid and make sure it's one of the valid data providers
                # and the user also belongs to this group
                try:
                    validate_entity_group_uuid(group_uuid, user_group_uuids)
                except schema_errors.NoDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.NoDataProviderGroupException(e)
                except schema_errors.UnmatchedDataProviderGroupException as e:
                    raise schema_errors.UnmatchedDataProviderGroupException(e)

                # Use group_uuid as parent_id for Source
                parent_id = group_uuid
            # Otherwise, parse user token to get the group_uuid
            else:
                # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that.
                # Otherwise if not set and no single "provider group" membership throws error.
                # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
                if "hmgroupids" not in user_info_dict:
                    raise KeyError(
                        "Missing 'hmgroupids' key in 'user_info_dict' when calling 'create_sennet_ids()' to create new ids for this Source."
                    )

                try:
                    group_info = get_entity_group_info(user_info_dict["hmgroupids"])
                except schema_errors.NoDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.NoDataProviderGroupException(e)
                except schema_errors.MultipleDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.MultipleDataProviderGroupException(e)

                parent_id = group_info["uuid"]

            # Add the parent_id to the request json
            json_to_post["parent_ids"] = [parent_id]
        elif normalized_class == "Sample":
            # 'Sample.direct_ancestor_uuid' is marked as `required_on_create` in the schema yaml
            # The application-specific code should have already validated the 'direct_ancestor_uuid'
            parent_id = json_data_dict["direct_ancestor_uuid"]
            json_to_post["parent_ids"] = [parent_id]

            # 'Sample.sample_category' is marked as `required_on_create` in the schema yaml
            if json_data_dict["sample_category"].lower() == "organ":
                # The 'organ' field containing the organ code is required in this case
                json_to_post["organ_code"] = json_data_dict["organ"]
        else:
            # `Dataset.direct_ancestor_uuids` is `required_on_create` in yaml
            json_to_post["parent_ids"] = json_data_dict["direct_ancestor_uuids"]

    request_headers = _create_request_headers(user_token)

    query_parms = {"entity_count": count}

    logger.info("======create_sennet_ids() json_to_post to uuid-api======")
    logger.info(json_to_post)

    uuid_url = _uuid_api_url + "/uuid"
    # Disable ssl certificate verification
    response = requests.post(
        url=uuid_url, headers=request_headers, json=json_to_post, verify=False, params=query_parms
    )

    # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
    response.raise_for_status()

    if response.status_code == 200:
        # For Collection/Dataset/Activity/Upload, no submission_id gets
        # generated, the uuid-api response looks like:
        """
        [{
            "uuid": "3bcc20f4f9ba19ed837136d19f530fbe",
            "base_id": "965PRGB226",
            "sennet_id": "SN965.PRGB.226"
        }]
        """

        ids_list = response.json()

        # Remove the "base_id" key from each dict in the list
        for d in ids_list:
            # Return None when the key is not in the dict
            # Will get keyError exception without the default value when the key is not found
            d.pop("base_id")

        logger.info("======create_sennet_ids() generated ids from uuid-api======")
        logger.info(ids_list)

        return ids_list
    else:
        msg = f"Unable to create new ids via the uuid-api service during the creation of this new {normalized_class}"

        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======create_sennet_ids() status code from uuid-api======")
        logger.debug(response.status_code)

        logger.debug("======create_sennet_ids() response text from uuid-api======")
        logger.debug(response.text)

        # Also bubble up the error message from uuid-api
        raise requests.exceptions.RequestException(response.text)


def get_entity_group_info(user_groupids_list, default_group=None):
    """
    Get the group info (group_uuid and group_name) based on user's hmgroupids list

    Parameters
    ----------
    user_groupids_list : list
        A list of globus group uuids that the user has access to

    Returns
    -------
    dict
        The group info (group_uuid and group_name)
    """

    global _auth_helper

    # Default
    group_info = {"uuid": "", "name": ""}

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info["by_id"]

    # A list of data provider uuids
    data_provider_uuids = []
    for uuid_key in groups_by_id_dict:
        if ("data_provider" in groups_by_id_dict[uuid_key]) and groups_by_id_dict[uuid_key][
            "data_provider"
        ]:
            data_provider_uuids.append(uuid_key)

    user_data_provider_uuids = []
    for group_uuid in user_groupids_list:
        if group_uuid in data_provider_uuids:
            user_data_provider_uuids.append(group_uuid)

    if len(user_data_provider_uuids) == 0:
        msg = "No data_provider groups found for this user. Can't continue."
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.NoDataProviderGroupException(msg)

    if len(user_data_provider_uuids) > 1:
        if default_group is not None and default_group in user_groupids_list:
            uuid = default_group
        else:
            msg = "More than one data_provider groups found for this user and no group_uuid specified. Can't continue."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise schema_errors.MultipleDataProviderGroupException(msg)
    else:
        # By now only one data provider group found, this is what we want
        uuid = user_data_provider_uuids[0]
    group_info["uuid"] = uuid
    group_info["name"] = groups_by_id_dict[uuid]["displayname"]

    return group_info


def validate_entity_group_uuid(group_uuid, user_group_uuids=None):
    """
    Check if the given group uuid is valid

    Parameters
    ----------
    group_uuid : str
        The target group uuid string
    user_group_uuids: list
        An optional list of group uuids to check against, a subset of all the data provider group uuids
    """
    global _auth_helper

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info["by_id"]

    # First make sure the group_uuid is one of the valid group UUIDs defiend in the json
    if group_uuid not in groups_by_id_dict:
        msg = (
            f"No data_provider groups found for the given group_uuid: {group_uuid}. Can't continue."
        )
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.NoDataProviderGroupException(msg)

    # Optional check depending if user_group_uuids is provided
    if user_group_uuids:
        # Next, make sure the given group_uuid is associated with the user
        if group_uuid not in user_group_uuids:
            msg = (
                f"The user doesn't belong to the given group of uuid: {group_uuid}. Can't continue."
            )
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise schema_errors.UnmatchedDataProviderGroupException(msg)


def get_entity_group_name(group_uuid):
    """
    Get the group_name based on the given group_uuid

    Parameters
    ----------
    group_uuid : str
        UUID of the target group

    Returns
    -------
    str
        The group_name corresponding to this group_uuid
    """

    global _auth_helper

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info["by_id"]
    group_dict = groups_by_id_dict[group_uuid]
    group_name = group_dict["displayname"]

    return group_name


def generate_activity_data(
    normalized_entity_type, user_token, user_info_dict, creation_action=None
):
    """
    Generate properties data of the target Activity node

    Parameters
    ----------
    normalized_entity_type : str
        One of the entity types defined in the schema yaml: Source, Sample, Dataset
    user_token: str
        The user's globus nexus token
    user_info_dict : dict
        A dictionary that contains all user info to be used to generate the related properties
    count : int
        The number of Activities to be generated

    Returns
    -------
    dict: A dict of gnerated Activity data
    """

    # Activity is not an Entity
    normalized_activity_type = "Activity"

    # Target entity type dict
    # Will be used when calling `set_activity_creation_action()` trigger method
    normalized_entity_type_dict = {"normalized_entity_type": normalized_entity_type}

    # Create new ids for the Activity node
    # This resulting list has only one dict
    new_ids_dict_list = create_sennet_ids(
        normalized_activity_type, json_data_dict=None, user_token=user_token, user_info_dict=None
    )
    data_dict_for_activity = {
        **user_info_dict,
        **normalized_entity_type_dict,
        **new_ids_dict_list[0],
    }

    if creation_action:
        data_dict_for_activity["creation_action"] = creation_action
    # Generate property values for Activity node
    generated_activity_data_dict = generate_triggered_data(
        TriggerTypeEnum.BEFORE_CREATE,
        normalized_activity_type,
        user_token,
        {},
        data_dict_for_activity,
    )

    return generated_activity_data_dict


def get_ingest_api_url():
    """
    Get the ingest-api URL to be used by trigger methods

    Returns
    -------
    str
        The ingest-api URL
    """
    global _ingest_api_url

    return _ingest_api_url


def get_search_api_url():
    """
    Get the search-api URL to be used by trigger methods

    Returns
    -------
    str
        The search-api URL
    """
    global _search_api_url

    return _search_api_url


def get_auth_helper_instance():
    """
    Get the AUthHelper instance to be used by trigger methods

    Returns
    -------
    AuthHelper
        The AuthHelper instance
    """
    global _auth_helper

    return _auth_helper


def get_neo4j_driver_instance():
    """
    Get the neo4j.Driver instance to be used by trigger methods

    Returns
    -------
    neo4j.Driver
        The neo4j.Driver instance
    """
    global _neo4j_driver

    return _neo4j_driver


def get_ubkg_instance():
    """
    Get the UBKG instance to be used by trigger methods

    Returns
    -------
    ubkg
        The UBKG instance
    """
    global _ubkg

    return _ubkg


####################################################################################################
## Internal functions
####################################################################################################


def _create_request_headers(user_token):
    """
    Create a dict of HTTP Authorization header with Bearer token for making calls to uuid-api

    Parameters
    ----------
    user_token: str
        The user's globus nexus token

    Returns
    -------
    dict
        The headers dict to be used by requests
    """
    auth_header_name = "Authorization"
    auth_scheme = "Bearer"

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme
        + " "
        + user_token
    }

    return headers_dict


def make_request_get(target_url, internal_token_used=False):
    """
    Cache the request response for the given URL with using function cache (memoization)

    Parameters
    ----------
    target_url: str
        The target URL

    Returns
    -------
    flask.Response
        The response object
    """
    global _memcached_client
    global _memcached_prefix

    response = None

    if _memcached_client and _memcached_prefix:
        cache_key = f"{_memcached_prefix}{target_url}"
        response = _memcached_client.get(cache_key)

    # Use the cached data if found and still valid
    # Otherwise, make a fresh query and add to cache
    if response is None:
        if _memcached_client and _memcached_prefix:
            logger.info(
                f"HTTP response cache not found or expired. Making a new HTTP request of GET {target_url} at time {datetime.now()}"
            )

        if internal_token_used:
            # Use modified version of globus app secret from configuration as the internal token
            auth_helper_instance = get_auth_helper_instance()
            request_headers = _create_request_headers(auth_helper_instance.getProcessSecret())

            # Disable ssl certificate verification
            response = requests.get(url=target_url, headers=request_headers, verify=False)
        else:
            response = requests.get(url=target_url, verify=False)

        if _memcached_client and _memcached_prefix:
            logger.info(
                f"Creating HTTP response cache of GET {target_url} at time {datetime.now()}"
            )

            cache_key = f"{_memcached_prefix}{target_url}"
            _memcached_client.set(cache_key, response, expire=SchemaConstants.MEMCACHED_TTL)
    else:
        logger.info(f"Using HTTP response cache of GET {target_url} at time {datetime.now()}")

    return response


def delete_memcached_cache(uuids_list):
    """
    Delete the cached data for the given entity uuids

    Parameters
    ----------
    uuids_list : list
        A list of target uuids
    """
    global _memcached_client
    global _memcached_prefix

    if _memcached_client and _memcached_prefix:
        cache_keys = []
        for uuid in uuids_list:
            cache_keys.append(f"{_memcached_prefix}_neo4j_{uuid}")
            cache_keys.append(f"{_memcached_prefix}_complete_{uuid}")
            cache_keys.append(f"{_memcached_prefix}_complete_index_{uuid}")

        _memcached_client.delete_many(cache_keys)

        logger.info(f"Deleted cache by key: {', '.join(cache_keys)}")


def get_entity_api_url():
    """Get the entity-api URL to be used by trigger methods.

    Returns
    -------
    str
        The entity-api URL ending with a trailing slash
    """
    global _entity_api_url
    return ensureTrailingSlashURL(_entity_api_url)

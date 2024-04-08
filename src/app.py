import ast
import collections
from datetime import datetime
from typing import List

from flask import Flask, g
from neo4j.exceptions import TransactionError
import os
import re
import csv
import requests
import urllib.request
from io import StringIO
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging
import json
import time
from lib.constraints import get_constraints_by_ancestor, get_constraints_by_descendant, build_constraint, \
    build_constraint_unit

# pymemcache.client.base.PooledClient is a thread-safe client pool
# that provides the same API as pymemcache.client.base.Client
from pymemcache.client.base import PooledClient
from pymemcache import serde

# Local modules
import app_neo4j_queries
import provenance
from schema import schema_manager, schema_validators, schema_triggers
from schema import schema_errors
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants
from schema.schema_constants import DataVisibilityEnum

# HuBMAP commons
from hubmap_commons import string_helper
from hubmap_commons import file_helper as hm_file_helper
from hubmap_commons import neo4j_driver
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.exceptions import HTTPException

# Atlas Consortia commons
from atlas_consortia_commons.ubkg import initialize_ubkg
from atlas_consortia_commons.rest import *
from atlas_consortia_commons.string import equals
from atlas_consortia_commons.ubkg.ubkg_sdk import init_ontology
from lib.ontology import Ontology

# Root logger configuration
global logger

# Use `getLogger()` instead of `getLogger(__name__)` to apply the config to the root logger
# will be inherited by the sub-module loggers
try:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # All the API logging is gets written into the same log file
    # The uWSGI logging for each deployment disables the request logging
    # but still captures the 4xx and 5xx errors to the file `log/uwsgi-entity-api.log`
    # Log rotation is handled via logrotate on the host system with a configuration file
    # Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
    log_file_handler = logging.FileHandler('../log/entity-api-' + time.strftime("%m-%d-%Y-%H-%M-%S") + '.log')
    log_file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s'))
    logger.addHandler(log_file_handler)
except Exception as e:
    print("Error setting up global log file.")
    print(str(e))

try:
    logger.info("logger initialized")
except Exception as e:
    print("Error opening log file during startup")
    print(str(e))

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['INGEST_API_URL'] = app.config['INGEST_API_URL'].strip('/')
app.config['SEARCH_API_URL'] = app.config['SEARCH_API_URL'].strip('/')

# This mode when set True disables the PUT and POST calls, used on STAGE to make entity-api READ-ONLY
# to prevent developers from creating new UUIDs and new entities or updating existing entities
READ_ONLY_MODE = app.config['READ_ONLY_MODE']

# Whether Memcached is being used or not
# Default to false if the property is missing in the configuration file

if 'MEMCACHED_MODE' in app.config:
    MEMCACHED_MODE = app.config['MEMCACHED_MODE']
    # Use prefix to distinguish the cached data of same source across different deployments
    MEMCACHED_PREFIX = app.config['MEMCACHED_PREFIX']
else:
    MEMCACHED_MODE = False
    MEMCACHED_PREFIX = 'NONE'

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

####################################################################################################
## UBKG Ontology and REST initialization
####################################################################################################

try:
    for exception in get_http_exceptions_classes():
        app.register_error_handler(exception, abort_err_handler)
    app.ubkg = initialize_ubkg(app.config)
    with app.app_context():
        init_ontology()
        Ontology.modify_entities_cache()

    logger.info("Initialized ubkg module successfully :)")
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the ubkg module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## AuthHelper initialization
####################################################################################################

# Initialize AuthHelper class and ensure singleton
try:
    if AuthHelper.isInitialized() == False:
        auth_helper_instance = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])

        logger.info("Initialized AuthHelper class successfully :)")
    else:
        auth_helper_instance = AuthHelper.instance()
except Exception:
    msg = "Failed to initialize the AuthHelper class"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Neo4j connection initialization
####################################################################################################

# The neo4j_driver (from commons package) is a singleton module
# This neo4j_driver_instance will be used for application-specifc neo4j queries
# as well as being passed to the schema_manager
try:
    neo4j_driver_instance = neo4j_driver.instance(app.config['NEO4J_URI'],
                                                  app.config['NEO4J_USERNAME'],
                                                  app.config['NEO4J_PASSWORD'])
    logger.info("Initialized neo4j_driver module successfully :)")
except Exception:
    msg = "Failed to initialize the neo4j_driver module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Memcached client initialization
####################################################################################################

memcached_client_instance = None

if MEMCACHED_MODE:
    try:
        # Use client pool to maintain a pool of already-connected clients for improved performance
        # The uwsgi config launches the app across multiple threads (8) inside each process (32), making essentially 256 processes
        # Set the connect_timeout and timeout to avoid blocking the process when memcached is slow, defaults to "forever"
        # connect_timeout: seconds to wait for a connection to the memcached server
        # timeout: seconds to wait for send or reveive calls on the socket connected to memcached
        # Use the ignore_exc flag to treat memcache/network errors as cache misses on calls to the get* methods
        # Set the no_delay flag to sent TCP_NODELAY (disable Nagle's algorithm to improve TCP/IP networks and decrease the number of packets)
        # If you intend to use anything but str as a value, it is a good idea to use a serializer
        memcached_client_instance = PooledClient(app.config['MEMCACHED_SERVER'],
                                                 max_pool_size = 256,
                                                 connect_timeout = 1,
                                                 timeout = 30,
                                                 ignore_exc = True,
                                                 no_delay = True,
                                                 serde = serde.pickle_serde)

        # memcached_client_instance can be instantiated without connecting to the Memcached server
        # A version() call will throw error (e.g., timeout) when failed to connect to server
        # Need to convert the version in bytes to string
        logger.info(f'Connected to Memcached server {memcached_client_instance.version().decode()} successfully :)')
    except Exception:
        msg = 'Failed to connect to the Memcached server :('
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        # Turn off the caching
        MEMCACHED_MODE = False
"""
Close the current neo4j connection at the end of every request
"""
@app.teardown_appcontext
def close_neo4j_driver(error):
    if hasattr(g, 'neo4j_driver_instance'):
        # Close the driver instance
        neo4j_driver.close()
        # Also remove neo4j_driver_instance from Flask's application context
        g.neo4j_driver_instance = None



####################################################################################################
## Schema initialization
####################################################################################################

try:
    # The schema_manager is a singleton module
    # Pass in auth_helper_instance, neo4j_driver instance, and file_upload_helper instance
    schema_manager.initialize(app.config['SCHEMA_YAML_FILE'],
                              app.config['UUID_API_URL'],
                              app.config['INGEST_API_URL'],
                              app.config['SEARCH_API_URL'],
                              auth_helper_instance,
                              neo4j_driver_instance,
                              app.ubkg,
                              memcached_client_instance,
                              app.config['MEMCACHED_PREFIX'])

    logger.info("Initialized schema_manager module successfully :)")
# Use a broad catch-all here
except Exception as e:
    # Log the full stack trace, prepend a line with our message and Exception
    logger.exception(f"Failed to initialize the schema_manager module - {str(e)}")


####################################################################################################
## REFERENCE DOI Redirection
####################################################################################################

## Read tsv file with the REFERENCE entity redirects
## sets the reference_redirects dict which is used
## by the /redirect method below
# try:
#     # TODO: Need to get updated redirection info for sennet
#     reference_redirects = {}
#     url = app.config['REDIRECTION_INFO_URL']
#     response = requests.get(url)
#     resp_txt = response.content.decode('utf-8')
#     cr = csv.reader(resp_txt.splitlines(), delimiter='\t')

#     first = True
#     id_column = None
#     redir_url_column = None
#     for row in cr:
#         if first:
#             first = False
#             header = row
#             column = 0
#             for label in header:
#                 if label == 'sennet_id': id_column = column
#                 if label == 'data_information_page': redir_url_column = column
#                 column = column + 1
#             if id_column is None: raise Exception(f"Column sennet_id not found in {url}")
#             if redir_url_column is None: raise Exception (f"Column data_information_page not found in {url}")
#         else:
#             reference_redirects[row[id_column].upper().strip()] = row[redir_url_column]
#     rr = redirect('abc', code = 307)
#     print(rr)
# except Exception:
#     logger.exception("Failed to read tsv file with REFERENCE redirect information")


####################################################################################################
## Constants
####################################################################################################

# For now, don't use the constants from commons
# All lowercase for easy comparision
#
# Places where these constants are used should be evaluated for refactoring to directly reference the
# constants in SchemaConstants.  Constants defined here should be evaluated to move to SchemaConstants.
# All this should be done when the endpoints with changed code can be verified with solid tests.
ACCESS_LEVEL_PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
ACCESS_LEVEL_CONSORTIUM = SchemaConstants.ACCESS_LEVEL_CONSORTIUM
ACCESS_LEVEL_PROTECTED = SchemaConstants.ACCESS_LEVEL_PROTECTED
DATASET_STATUS_PUBLISHED = SchemaConstants.DATASET_STATUS_PUBLISHED
COMMA_SEPARATOR = ','


####################################################################################################
## API Endpoints
####################################################################################################

"""
The default route

Returns
-------
str
    A welcome message
"""
@app.route('/', methods = ['GET'])
def index():
    return "Hello! This is SenNet Entity API service :)"


"""
Delete ALL the following cached data from Memcached, Data Admin access is required in AWS API Gateway:
    - cached individual entity dict
    - cached IDs dict from uuid-api
    - cached yaml content from github raw URLs
    - cached TSV file content for reference DOIs redirect

Returns
-------
str
    A confirmation message
"""
@app.route('/flush-all-cache', methods = ['DELETE'])
def flush_all_cache():
    msg = ''

    if MEMCACHED_MODE:
        memcached_client_instance.flush_all()
        msg = 'All cached data (entities, IDs, yamls, tsv) has been deleted from Memcached'
    else:
        msg = 'No caching is being used because Memcached mode is not enabled at all'

    return msg


"""
Delete the cached data from Memcached for a given entity, Data Admin access is required in AWS API Gateway

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity (Donor/Dataset/Sample/Upload/Collection/Publication)

Returns
-------
str
    A confirmation message
"""
@app.route('/flush-cache/<id>', methods = ['DELETE'])
def flush_cache(id):
    msg = ''

    if MEMCACHED_MODE:
        delete_cache(id)
        msg = f'The cached data has been deleted from Memcached for entity {id}'
    else:
        msg = 'No caching is being used because Memcached mode is not enabled at all'

    return msg

"""
Show status of neo4j connection with the current VERSION and BUILD

Returns
-------
json
    A json containing the status details
"""
@app.route('/status', methods = ['GET'])
def get_status():

    try:
        file_version_content = (Path(__file__).absolute().parent.parent / 'VERSION').read_text().strip()
    except Exception as e:
        file_version_content = str(e)

    try:
        file_build_content = (Path(__file__).absolute().parent.parent / 'BUILD').read_text().strip()
    except Exception as e:
        file_build_content = str(e)

    status_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': file_version_content,
        'build': file_build_content,
        'neo4j_connection': False
    }

    # Don't use try/except here
    is_connected = app_neo4j_queries.check_connection(neo4j_driver_instance)

    if is_connected:
        status_data['neo4j_connection'] = True

    return jsonify(status_data)


"""
Currently for debugging purpose 
Essentially does the same as ingest-api's `/metadata/usergroups` using the deprecated commons method
Globus groups token is required by AWS API Gateway lambda authorizer

Returns
-------
json
    A json list of globus groups this user belongs to
"""
@app.route('/usergroups', methods = ['GET'])
def get_user_groups():
    token = get_user_token(request)
    groups_list = auth_helper_instance.get_user_groups_deprecated(token)
    return jsonify(groups_list)


"""
Retrieve the ancestor organ(s) of a given entity

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity (Dataset/Sample)

Returns
-------
json
    List of organs that are ancestors of the given entity
    - Only dataset entities can return multiple ancestor organs
      as Samples can only have one parent.
    - If no organ ancestors are found an empty list is returned
    - If requesting the ancestor organ of a Sample of type Organ or Source/Collection/Upload
      a 400 response is returned.
"""
@app.route('/entities/<id>/ancestor-organs', methods = ['GET'])
def get_ancestor_organs(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # A bit validation
    supported_entity_types = ['Sample']
    if normalized_entity_type not in supported_entity_types and \
            not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req(f"Unable to get the ancestor organs for this: {normalized_entity_type}, supported entity types: Sample, Dataset, Publication")

    if normalized_entity_type == 'Sample' and entity_dict['sample_category'].lower() == 'organ':
        abort_bad_req("Unable to get the ancestor organ of an organ.")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to SenNet-READ group
            token = get_user_token(request, non_public_access_required = True)
    else:
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    organs = app_neo4j_queries.get_ancestor_organs(neo4j_driver_instance, entity_dict['uuid'])

    # Skip executing the trigger method to get Sample.direct_ancestor
    properties_to_skip = ['direct_ancestor']
    complete_entities_list = schema_manager.get_complete_entities_list(token, organs, properties_to_skip)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)

def _get_entity_visibility(normalized_entity_type, entity_dict):
    if normalized_entity_type not in schema_manager.get_all_entity_types():
        logger.log( logging.ERROR
                    ,f"normalized_entity_type={normalized_entity_type}"
                     f" not recognized by schema_manager.get_all_entity_types().")
        abort_bad_req(f"'{normalized_entity_type}' is not a recognized entity type.")

    # Use the characteristics of the entity's data to classify the entity's visibility, so
    # it can be used along with the user's authorization to determine access.
    entity_visibility=DataVisibilityEnum.NONPUBLIC
    if normalized_entity_type == 'Dataset' and \
       entity_dict['status'].lower() == DATASET_STATUS_PUBLISHED:
        entity_visibility=DataVisibilityEnum.PUBLIC
    elif normalized_entity_type == 'Collection' and \
        'registered_doi' in entity_dict and \
        'doi_url' in entity_dict and \
        'contacts' in entity_dict and \
        'creators' in entity_dict and \
        len(entity_dict['contacts']) > 0 and \
        len(entity_dict['creators']) > 0:
            # Get the data_access_level for each Dataset in the Collection from Neo4j
            collection_dataset_statuses = schema_neo4j_queries.get_collection_datasets_statuses(neo4j_driver_instance
                                                                                                ,entity_dict['uuid'])

            # If the list of distinct statuses for Datasets in the Collection only has one entry, and
            # it is 'published', the Collection is public
            if len(collection_dataset_statuses) == 1 and \
                collection_dataset_statuses[0].lower() == SchemaConstants.DATASET_STATUS_PUBLISHED:
                entity_visibility=DataVisibilityEnum.PUBLIC
    elif normalized_entity_type == 'Upload':
        # Upload entities require authorization to access, so keep the
        # entity_visibility as non-public, as initialized outside block.
        pass
    elif normalized_entity_type in ['Source','Sample'] and \
         entity_dict['data_access_level'] == ACCESS_LEVEL_PUBLIC:
        entity_visibility = DataVisibilityEnum.PUBLIC
    return entity_visibility

"""
Retrieve the metadata information of a given entity by id

The gateway treats this endpoint as public accessible

Result filtering is supported based on query string
For example: /entities/<id>?property=data_access_level

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the properties or filtered property of the target entity
"""
@app.route('/entities/<id>', methods = ['GET'])
def get_entity_by_id(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    # To verify if a Collection is public, it is necessary to have its Datasets, which
    # are populated as triggered data.  So pull back the complete entity for
    # _get_entity_visibility() to check.
    complete_dict = schema_manager.get_complete_entity_result(token, entity_dict)

    # Determine if the entity is publicly visible base on its data, only.
    entity_scope = _get_entity_visibility(  normalized_entity_type=normalized_entity_type
                                            ,entity_dict=complete_dict)

    # Initialize the user as authorized if the data is public.  Otherwise, the
    # user is not authorized and credentials must be checked.
    if entity_scope == DataVisibilityEnum.PUBLIC:
        user_authorized = True
    else:
        # It's highly possible that there's no token provided
        user_token = get_user_token(request)

        # The user_token is flask.Response on error
        # Without token, the user can only access public collections, modify the collection result
        # by only returning public datasets attached to this collection
        if isinstance(user_token, Response):
            abort_forbidden(f"{normalized_entity_type} for {id} is not accessible without presenting a token.")
        else:
            # When the groups token is valid, but the user doesn't belong to HuBMAP-READ group
            # Or the token is valid but doesn't contain group information (auth token or transfer token)
            user_authorized = user_in_globus_read_group(request)

    # We'll need to return all the properties including those generated by
    # `on_read_trigger` to have a complete result e.g., the 'next_revision_uuid' and
    # 'previous_revision_uuid' being used below.
    if not user_authorized:
        abort_forbidden(f"The requested {normalized_entity_type} has non-public data."
                        f"  A Globus token with access permission is required.")

    # Also normalize the result based on schema
    final_result = schema_manager.normalize_object_result_for_response( provenance_type='ENTITIES'
                                                                        ,entity_dict=complete_dict
                                                                        ,properties_to_include=['protocol_url'])

    # Result filtering based on query string
    # The `data_access_level` property is available in all entities Source/Sample/Dataset
    # and this filter is being used by gateway to check the data_access_level for file assets
    # The `status` property is only available in Dataset and being used by search-api for revision
    result_filtering_accepted_property_keys = ['data_access_level', 'status']

    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            if property_key == 'status' and \
                    not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
                abort_bad_req(f"Only Dataset supports 'status' property key in the query string")

            # Response with the property value directly
            # Don't use jsonify() on string value
            return complete_dict[property_key]
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    else:
        # Response with the dict
        return jsonify(final_result)


"""
Retrive the full tree above the referenced entity and build the provenance document

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the provenance details associated with this entity
"""
@app.route('/entities/<id>/provenance', methods = ['GET'])
def get_entity_provenance(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']

    # A bit validation to prevent Lab or Collection being queried
    supported_entity_types = ['Source', 'Sample']
    if normalized_entity_type not in supported_entity_types and \
            not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req(f"Unable to get the provenance for this {normalized_entity_type}, supported entity types: {COMMA_SEPARATOR.join(supported_entity_types)}, Dataset, Publication")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to SenNet-READ group
            token = get_user_token(request, non_public_access_required = True)
    else:
        # The `data_access_level` of Source/Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Will just proceed to get the provenance information
    # Get the `depth` from query string if present and it's used by neo4j query
    # to set the maximum number of hops in the traversal
    depth = None
    if 'depth' in request.args:
        depth = int(request.args.get('depth'))

    # Convert neo4j json to dict
    neo4j_result = app_neo4j_queries.get_provenance(neo4j_driver_instance, uuid, depth)
    raw_provenance_dict = dict(neo4j_result['json'])

    raw_descendants_dict = None
    if bool(request.args):
        # The parsed query string value is a string 'true'
        return_descendants = request.args.get('return_descendants')

        if (return_descendants is not None) and (return_descendants.lower() == 'true'):
            neo4j_result_descendants = app_neo4j_queries.get_provenance(neo4j_driver_instance, uuid, depth, True)
            raw_descendants_dict = dict(neo4j_result_descendants['json'])

    # Normalize the raw provenance nodes based on the yaml schema
    normalized_provenance_dict = {
        'relationships': raw_provenance_dict['relationships'],
        'nodes': []
    }

    build_nodes(raw_provenance_dict, normalized_provenance_dict, token)
    provenance_json = provenance.get_provenance_history(uuid, normalized_provenance_dict, auth_helper_instance)

    if raw_descendants_dict:
        normalized_provenance_descendants_dict = {
            'relationships': raw_descendants_dict['relationships'],
            'nodes': []
        }

        build_nodes(raw_descendants_dict, normalized_provenance_descendants_dict, token)
        provenance_json_descendants = provenance.get_provenance_history(uuid, normalized_provenance_descendants_dict,
                                                                        auth_helper_instance)

        provenance_json = json.loads(provenance_json)
        provenance_json['descendants'] = json.loads(provenance_json_descendants)
        provenance_json = json.dumps(provenance_json)

    # Response with the provenance details
    return Response(response = provenance_json, mimetype = "application/json")


def build_nodes(raw_provenance_dict, normalized_provenance_dict, token):
    for node_dict in raw_provenance_dict['nodes']:
        # The schema yaml doesn't handle Lab nodes, just leave it as is
        if (node_dict['label'] == 'Entity') and (node_dict['entity_type'] != 'Lab'):
            # Generate trigger data
            # Skip some of the properties that are time-consuming to generate via triggers:
            # director_ancestor for Sample, and direct_ancestors for Dataset
            # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
            # checks when the target Dataset is public but the revisions are not public
            properties_to_skip = [
                'direct_ancestors',
                'direct_ancestor',
                'next_revision_uuid',
                'previous_revision_uuid',
                'next_revision_uuids',
                'previous_revision_uuids'
            ]

            # We'll need to return all the properties (except the ones to skip from above list)
            # including those generated by `on_read_trigger` to have a complete result
            # The 'on_read_trigger' doesn't really need a token
            complete_entity_dict = schema_manager.get_complete_entity_result(token, node_dict, properties_to_skip)

            # Filter out properties not defined or not to be exposed in the schema yaml
            normalized_entity_dict = schema_manager.normalize_object_result_for_response('ENTITIES', complete_entity_dict)

            # Now the node to be used by provenance is all regulated by the schema
            normalized_provenance_dict['nodes'].append(normalized_entity_dict)
        elif node_dict['label'] == 'Activity':
            # Normalize Activity nodes too
            normalized_activity_dict = schema_manager.normalize_activity_result_for_response(node_dict)
            normalized_provenance_dict['nodes'].append(normalized_activity_dict)
        else:
            # Skip Entity Lab nodes
            normalized_provenance_dict['nodes'].append(node_dict)


"""
Show all the supported entity types

The gateway treats this endpoint as public accessible

Returns
-------
json
    A list of all the available entity types defined in the schema yaml
"""
@app.route('/entity-types', methods = ['GET'])
def get_entity_types():
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    return jsonify(schema_manager.get_all_entity_types())

"""
Retrive all the entity nodes for a given entity type
Result filtering is supported based on query string
For example: /<entity_type>/entities?property=uuid

NOTE: this endpoint is NOT exposed via AWS API Gateway due to performance consideration
It's only used by search-api with making internal calls during index/reindex time bypassing AWS API Gateway

Parameters
----------
entity_type : str
    One of the supported entity types: Dataset, Collection, Sample, Source

Returns
-------
json
    All the entity nodes in a list of the target entity type
"""
@app.route('/<entity_type>/entities', methods = ['GET'])
def get_entities_by_type(entity_type):
    final_result = []

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to ensure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        abort_bad_req("Invalid entity type provided: " + entity_type)

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type, property_key)

            # Final result
            final_result = property_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        # Get back a list of entity dicts for the given entity type
        entities_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type)

        # We'll return all the properties but skip these time-consuming ones
        # Source doesn't need to skip any
        # Collection is not handled by this call
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Upload
            'datasets',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'previous_revision_uuid',
            'next_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]
        # Get user token from Authorization header.  Since this endpoint is not exposed through the AWS Gateway
        token = get_user_token(request)
        # Get back a list of entity dicts for the given entity type
        entities_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type)

        complete_entities_list = schema_manager.get_complete_entities_list(token, entities_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response( complete_entities_list
                                                                            ,properties_to_include=['protocol_url'])

    # Response with the final result
    return jsonify(final_result)

"""
Create an entity of the target type in neo4j

Response result filtering is supported based on query string
For example: /entities/<entity_type>?return_all_properties=true
Default to skip those time-consuming properties

Parameters
----------
entity_type : str
    One of the target entity types (case-insensitive since will be normalized): Dataset, Source, Sample, Upload, Collection

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/<entity_type>', methods = ['POST'])
def create_entity(entity_type):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to make sure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        abort_bad_req(f"Invalid entity type provided: {entity_type}")

    # Execute entity level validator defined in schema yaml before entity creation
    # Currently on Dataset and Upload creation require application header
    try:
        schema_manager.execute_entity_level_validator('before_entity_create_validator', normalized_entity_type, request)
    except schema_errors.MissingApplicationHeaderException as e:
        abort_bad_req(e)
    except schema_errors.InvalidApplicationHeaderException as e:
        abort_bad_req(e)

    verify_ubkg_properties(json_data_dict)

    # Validate request json against the yaml schema
    try:
        schema_manager.validate_json_data_against_schema('ENTITIES', json_data_dict, normalized_entity_type)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        abort_bad_req(str(e))

    # Execute property level validators defined in schema yaml before entity property creation
    # Use empty dict {} to indicate there's no existing_data_dict
    try:
        schema_manager.execute_property_level_validators('ENTITIES', 'before_property_create_validators', normalized_entity_type, request, {}, json_data_dict)
    # Currently only ValueError
    except ValueError as e:
        abort_bad_req(e)

    # Sample and Dataset: additional validation, create entity, after_create_trigger
    # Collection and Source: create entity
    if normalized_entity_type == 'Sample':
        # A bit more validation to ensure if `organ` code is set, the `sample_category` must be set to "organ"
        # Vise versa, if `sample_category` is set to "organ", `organ` code is required
        if ('sample_category' in json_data_dict) and (json_data_dict['sample_category'].lower() == 'organ'):
            if ('organ' not in json_data_dict) or (json_data_dict['organ'].strip() == ''):
                abort_bad_req("A valid organ code is required when the sample_category is organ")
        else:
            if 'organ' in json_data_dict:
                abort_bad_req("The sample_category must be organ when an organ code is provided")

        # A bit more validation for new sample to be linked to existing source entity
        direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
        # Check existence of the direct ancestor (either another Sample or Source)
        direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
        validate_constraints_by_entities(direct_ancestor_dict, json_data_dict, normalized_entity_type)
        json_data_dict['direct_ancestor_uuid'] = direct_ancestor_dict['uuid']

        check_multiple_organs_constraint(json_data_dict, direct_ancestor_dict)

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # `direct_ancestor_uuids` is required for creating new Dataset
        # Check existence of those direct ancestors

        direct_ancestor_uuids = []
        for direct_ancestor_uuid in json_data_dict['direct_ancestor_uuids']:
            direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
            validate_constraints_by_entities(direct_ancestor_dict, json_data_dict, normalized_entity_type)
            direct_ancestor_uuids.append(direct_ancestor_dict['uuid'])

        json_data_dict['direct_ancestor_uuids'] = direct_ancestor_uuids

        def check_previous_revision(previous_revision_uuid):
            previous_version_dict = query_target_entity(previous_revision_uuid, user_token)

            # Make sure the previous version entity is either a Dataset or Sample
            if previous_version_dict['entity_type'] not in ['Dataset', 'Sample']:
                abort_bad_req(f"The previous_revision_uuid specified for this dataset must be either a Dataset or Sample")

            # Also need to validate if the given 'previous_revision_uuid' has already had
            # an exisiting next revision
            # Only return a list of the uuids, no need to get back the list of dicts
            next_revisions_list = app_neo4j_queries.get_next_revisions(neo4j_driver_instance, previous_version_dict['uuid'], 'uuid')

            # As long as the list is not empty, tell the users to use a different 'previous_revision_uuid'
            if next_revisions_list:
                abort_bad_req(f"The previous_revision_uuid specified for this dataset has already had a next revision")

            # Only published datasets can have revisions made of them. Verify that that status of the Dataset specified
            # by previous_revision_uuid is published. Else, bad request error.
            if 'status' not in previous_version_dict or previous_version_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
                abort_bad_req(f"The previous_revision_uuid specified for this dataset must be 'Published' in order to create a new revision from it")


        # Also check existence of the previous revision dataset if specified
        if 'previous_revision_uuid' in json_data_dict:
            check_previous_revision(json_data_dict['previous_revision_uuid'])

        if 'previous_revision_uuids' in json_data_dict:
            for previous_revision_uuid in json_data_dict['previous_revision_uuids']:
                check_previous_revision(previous_revision_uuid)

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)
    else:
        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)

    # For Source: link to parent Lab node
    # For Sample: link to existing direct ancestor
    # For Dataset: link to direct ancestors
    # For Collection: link to member Datasets
    # For Upload: link to parent Lab node
    after_create(normalized_entity_type, user_token, merged_dict)

    # By default we'll return all the properties but skip these time-consuming ones
    # Source doesn't need to skip any
    properties_to_skip = []

    if normalized_entity_type == 'Sample':
        properties_to_skip = [
            'direct_ancestor'
        ]
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        properties_to_skip = [
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'previous_revision_uuid',
            'next_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]
    elif normalized_entity_type in ['Upload', 'Collection']:
        properties_to_skip = [
            'datasets',
            'entities'
        ]

    # Result filtering based on query string
    # Will return all properties by running all the read triggers
    # If the reuqest specifies `/entities/<entity_type>?return_all_properties=true`
    if bool(request.args):
        # The parased query string value is a string 'true'
        return_all_properties = request.args.get('return_all_properties')

        if (return_all_properties is not None) and (return_all_properties.lower() == 'true'):
            properties_to_skip = []

    # Generate the filtered or complete entity dict to send back
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_dict, properties_to_skip)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_object_result_for_response('ENTITIES', complete_dict)

    # Also index the new entity node in elasticsearch via search-api
    logger.log(logging.INFO
               ,f"Re-indexing for creation of {complete_dict['entity_type']}"
                f" with UUID {complete_dict['uuid']}")
    reindex_entity(complete_dict['uuid'], user_token)

    return jsonify(normalized_complete_dict)

"""
Create multiple samples from the same source entity

Parameters
----------
count : str
    The number of samples to be created

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/multiple-samples/<count>', methods = ['POST'])
def create_multiple_samples(count):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Normalize user provided entity_type
    normalized_entity_type = 'Sample'

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    try:
        schema_manager.validate_json_data_against_schema('ENTITIES', json_data_dict, normalized_entity_type)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        abort_bad_req(str(e))

    # `direct_ancestor_uuid` is required on create
    # Check existence of the direct ancestor (either another Sample or Source) and get the first 'direct_ancestor_uuid'
    direct_ancestor_uuid_dict = query_target_entity(json_data_dict['direct_ancestor_uuid'][0], user_token)


    # Generate 'before_create_triiger' data and create the entity details in Neo4j
    generated_ids_dict_list = create_multiple_samples_details(request, normalized_entity_type, user_token, json_data_dict, count)

    # Also index the each new Sample node in elasticsearch via search-api
    for id_dict in generated_ids_dict_list:
        reindex_entity(id_dict['uuid'], user_token)

    return jsonify(generated_ids_dict_list)


"""
Update the properties of a given activity, primarily the protocol_url and processing_information

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target activity 

Returns
-------
json
    All the updated properties of the target activity
"""


@app.route('/activity/<id>', methods=['PUT'])
def update_activity(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Get target entity and return as a dict if exists
    activity_dict = query_target_activity(id, user_token)

    normalized_dict = schema_manager.normalize_object_result_for_response('ACTIVITIES', json_data_dict)


    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    try:
        schema_manager.validate_json_data_against_schema('ACTIVITIES', normalized_dict, "Activity",
                                                         existing_entity_dict=activity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        abort_bad_req(str(e))

    # Execute property level validators defined in schema yaml before entity property update
    try:
        schema_manager.execute_property_level_validators('ACTIVITIES','before_property_update_validators', "Activity",
                                                         request, activity_dict, normalized_dict)
    except (schema_errors.MissingApplicationHeaderException,
            schema_errors.InvalidApplicationHeaderException,
            KeyError,
            ValueError) as e:
        abort_bad_req(e)

    # Generate 'before_update_trigger' data and update the entity details in Neo4j
    merged_updated_dict = update_object_details('ACTIVITIES', request, "Activity", user_token, normalized_dict,
                                                activity_dict)

    # We'll need to return all the properties including those
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_updated_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_object_result_for_response('ACTIVITIES', complete_dict)

    # Also reindex the updated entity node in elasticsearch via search-api
    # reindex_entity(activity_dict['uuid'], user_token)

    return jsonify(normalized_complete_dict)


@app.route('/entities/type/<type_a>/instanceof/<type_b>', methods=['GET'])
def get_entities_type_instanceof(type_a, type_b):
    try:
        instanceof: bool = schema_manager.entity_type_instanceof(type_a, type_b)
    except:
        abort_bad_req('Unable to process request')
    return make_response(jsonify({'instanceof': instanceof}), 200)

"""
Endpoint which sends the "visibility" of an entity using values from DataVisibilityEnum.
Not exposed through the gateway.  Used by services like search-api to, for example, determine if
a Collection can be in a public index while encapsulating the logic to determine that in this service.
Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target collection 
Returns
-------
json
    A value from DataVisibilityEnum
"""
@app.route('/visibility/<id>', methods = ['GET'])
def get_entity_visibility(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Get the generated complete entity result from cache if exists
    # Otherwise re-generate on the fly.  To verify if a Collection is public, it is
    # necessary to have its Datasets, which are populated as triggered data, so
    # pull back the complete entity
    complete_dict = schema_manager.get_complete_entity_result(token, entity_dict)

    # Determine if the entity is publicly visible base on its data, only.
    entity_scope = _get_entity_visibility(normalized_entity_type=normalized_entity_type, entity_dict=complete_dict)

    return jsonify(entity_scope.value)

"""
Update the properties of a given entity

Response result filtering is supported based on query string
For example: /entities/<id>?return_all_properties=true
Default to skip those time-consuming properties

Parameters
----------
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the updated properties of the target entity
"""
@app.route('/entities/<id>', methods = ['PUT'])
def update_entity(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Normalize user provided status
    if "status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["status"])
        json_data_dict["status"] = normalized_status

    # Normalize user provided status
    if "sub_status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["sub_status"])
        json_data_dict["sub_status"] = normalized_status

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(id, user_token)

    # Check that the user has the correct access to modify this entity
    validate_user_update_privilege(entity_dict, user_token)

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_dict['entity_type'])


    verify_ubkg_properties(json_data_dict)

    # Note, we don't support entity level validators on entity update via PUT
    # Only entity create via POST is supported at the entity level

    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    try:
        schema_manager.validate_json_data_against_schema('ENTITIES', json_data_dict, normalized_entity_type, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        abort_bad_req(str(e))

    # Execute property level validators defined in schema yaml before entity property update
    try:
        schema_manager.execute_property_level_validators('ENTITIES', 'before_property_update_validators', normalized_entity_type, request, entity_dict, json_data_dict)
    except (schema_errors.MissingApplicationHeaderException,
            schema_errors.InvalidApplicationHeaderException,
            KeyError,
            ValueError) as e:
        abort_bad_req(e)

    # Sample, Dataset, and Upload: additional validation, update entity, after_update_trigger
    # Collection and Source: update entity
    if normalized_entity_type == 'Sample':
        # A bit more validation for updating the sample and the linkage to existing source entity
        has_direct_ancestor_uuid = False
        if ('direct_ancestor_uuid' in json_data_dict) and json_data_dict['direct_ancestor_uuid']:
            has_direct_ancestor_uuid = True

            direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
            # Check existence of the source entity
            direct_ancestor_uuid_dict = query_target_entity(direct_ancestor_uuid, user_token)
            validate_constraints_by_entities(direct_ancestor_uuid_dict, json_data_dict, normalized_entity_type)
            # Also make sure it's either another Sample or a Source
            if direct_ancestor_uuid_dict['entity_type'] not in ['Source', 'Sample']:
                abort_bad_req(f"The uuid: {direct_ancestor_uuid} is not a Source neither a Sample, cannot be used as the direct ancestor of this Sample")

            check_multiple_organs_constraint(json_data_dict, direct_ancestor_uuid_dict, entity_dict['uuid'])

        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods 
        if has_direct_ancestor_uuid:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif normalized_entity_type in ['Dataset', 'Publication']:
        # A bit more validation if `direct_ancestor_uuids` provided
        has_direct_ancestor_uuids = False
        if ('direct_ancestor_uuids' in json_data_dict) and (json_data_dict['direct_ancestor_uuids']):
            has_direct_ancestor_uuids = True

            # Check existence of those source entities
            for direct_ancestor_uuids in json_data_dict['direct_ancestor_uuids']:
                direct_ancestor_uuids_dict = query_target_entity(direct_ancestor_uuids, user_token)
                validate_constraints_by_entities(direct_ancestor_uuids_dict, json_data_dict, normalized_entity_type)

        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        if has_direct_ancestor_uuids:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif normalized_entity_type == 'Upload':
        has_dataset_uuids_to_link = False
        if ('dataset_uuids_to_link' in json_data_dict) and (json_data_dict['dataset_uuids_to_link']):
            has_dataset_uuids_to_link = True

            # Check existence of those datasets to be linked
            # If one of the datasets to be linked appears to be already linked,
            # neo4j query won't create the new linkage due to the use of `MERGE`
            for dataset_uuid in json_data_dict['dataset_uuids_to_link']:
                dataset_dict = query_target_entity(dataset_uuid, user_token)
                # Also make sure it's a Dataset
                if dataset_dict['entity_type'] not in ['Dataset', 'Publication']:
                    abort_bad_req(f"The uuid: {dataset_uuid} is not a Dataset or Publication, cannot be linked to this Upload")

        has_dataset_uuids_to_unlink = False
        if ('dataset_uuids_to_unlink' in json_data_dict) and (json_data_dict['dataset_uuids_to_unlink']):
            has_dataset_uuids_to_unlink = True

            # Check existence of those datasets to be unlinked
            # If one of the datasets to be unlinked appears to be not linked at all,
            # the neo4j cypher will simply skip it because it won't match the "MATCH" clause
            # So no need to tell the end users that this dataset is not linked
            # Let alone checking the entity type to ensure it's a Dataset
            for dataset_uuid in json_data_dict['dataset_uuids_to_unlink']:
                dataset_dict = query_target_entity(dataset_uuid, user_token)

        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        if has_dataset_uuids_to_link or has_dataset_uuids_to_unlink:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif normalized_entity_type == 'Collection':
        entity_visibility = _get_entity_visibility(  normalized_entity_type=normalized_entity_type
                                                    ,entity_dict=entity_dict)
        # Prohibit update of an existing Collection if it meets criteria of being visible to public e.g. has DOI.
        if entity_visibility == DataVisibilityEnum.PUBLIC:
            logger.info(f"Attempt to update {normalized_entity_type} with id={id} which has visibility {entity_visibility}.")
            abort_bad_req(f"Cannot update {normalized_entity_type} due '{entity_visibility.value}' visibility.")

        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, user_token, json_data_dict,
                                                    entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        after_update(normalized_entity_type, user_token, merged_updated_dict)
    else:
        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, user_token, json_data_dict, entity_dict)

    # By default we'll return all the properties but skip these time-consuming ones
    # Source doesn't need to skip any
    properties_to_skip = []

    if normalized_entity_type == 'Sample':
        properties_to_skip = [
            'direct_ancestor'
        ]
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        properties_to_skip = [
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'previous_revision_uuid',
            'next_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]
    elif normalized_entity_type in ['Upload', 'Collection']:
        properties_to_skip = [
            'datasets',
            'entities'
        ]

    # Result filtering based on query string
    # Will return all properties by running all the read triggers
    # If the reuqest specifies `/entities/<id>?return_all_properties=true`
    if bool(request.args):
        # The parased query string value is a string 'true'
        return_all_properties = request.args.get('return_all_properties')

        if (return_all_properties is not None) and (return_all_properties.lower() == 'true'):
            properties_to_skip = []

    # Generate the filtered or complete entity dict to send back
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_updated_dict, properties_to_skip)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_object_result_for_response('ENTITIES', complete_dict)

    if 'protocol_url' in json_data_dict or (
            'ingest_metadata' in json_data_dict and 'dag_provenance_list' in json_data_dict['ingest_metadata']):
        # protocol_url = json_data_dict['protocol_url']
        activity_dict = query_activity_was_generated_by(id, user_token)
        # request.json = {'protocol_url': protocol_url}
        update_activity(activity_dict['uuid'])

    # How to handle reindex collection?
    # Also reindex the updated entity node in elasticsearch via search-api
    logger.log(logging.INFO
               ,"Re-indexing for modification of {entity_dict['entity_type']}"
                f" with UUID {entity_dict['uuid']}")
    reindex_entity(entity_dict['uuid'], user_token)

    return jsonify(normalized_complete_dict)

"""
Get all ancestors of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /ancestors/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity 

Returns
-------
json
    A list of all the ancestors of the target entity
"""
@app.route('/ancestors/<id>', methods = ['GET'])
def get_ancestors(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    # Collection doesn't have ancestors via Activity nodes
    if normalized_entity_type == 'Collection':
        abort_bad_req(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to SenNet-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Source and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        ancestors_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, ancestors_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all descendants of the given entity
Result filtering based on query string
For example: /descendants/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the descendants of the target entity
"""
@app.route('/descendants/<id>', methods = ['GET'])
def get_descendants(id):
    final_result = []

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Collection and Upload don't have descendants via Activity nodes
    # No need to check, it'll always return empty list

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        descendants_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, descendants_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)

"""
Get all parents of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /parents/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the parents of the target entity
"""
@app.route('/parents/<id>', methods = ['GET'])
def get_parents(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    # Collection doesn't have ancestors via Activity nodes
    if normalized_entity_type == 'Collection':
        abort_bad_req(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to SenNet-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Source and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        parents_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, parents_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)

"""
Get all chilren of the given entity
Result filtering based on query string
For example: /children/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the children of the target entity
"""
@app.route('/children/<id>', methods = ['GET'])
def get_children(id):
    final_result = []

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Collection and Upload don't have children via Activity nodes
    # No need to check, it'll always return empty list

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        children_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid',
            'next_revision_uuids',
            'previous_revision_uuids'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, children_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all siblings of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/<id>/siblings?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the siblings of the target entity
"""
@app.route('/entities/<id>/siblings', methods = ['GET'])
def get_siblings(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    # Collection doesn't have ancestors via Activity nodes
    if normalized_entity_type == 'Collection':
        abort_bad_req(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required=True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required=True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    status = None
    property_key = None
    include_revisions = None
    accepted_args = ['property', 'status', 'include-old-revisions']
    if bool(request.args):
        for arg_name in request.args.keys():
            if arg_name not in accepted_args:
                abort_bad_req(f"{arg_name} is an unrecognized argument")
        property_key = request.args.get('property')
        status = request.args.get('status')
        include_revisions = request.args.get('include-old-revisions')
        if status is not None:
            status = status.lower()
            if status not in SchemaConstants.ALLOWED_DATASET_STATUSES:
                abort_bad_req("Invalid Dataset Status. Must be 'new', 'qa', or 'published' Case-Insensitive")
        if property_key is not None:
            property_key = property_key.lower()
            result_filtering_accepted_property_keys = ['uuid']
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")
        if include_revisions is not None:
            include_revisions = include_revisions.lower()
            if include_revisions not in ['true', 'false']:
                abort_bad_req("Invalid 'include-old-revisions'. Accepted values are 'true' and 'false' Case-Insensitive")
            if include_revisions == 'true':
                include_revisions = True
            else:
                include_revisions = False
    sibling_list = app_neo4j_queries.get_siblings(neo4j_driver_instance, uuid, status, property_key, include_revisions)
    if property_key is not None:
        return jsonify(sibling_list)
    # Generate trigger data
    # Skip some of the properties that are time-consuming to generate via triggers
    # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
    # checks when the target Dataset is public but the revisions are not public
    properties_to_skip = [
        # Properties to skip for Sample
        'direct_ancestor',
        # Properties to skip for Dataset
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'next_revision_uuid',
        'previous_revision_uuid',
        'associated_collection',
        'creation_action',
        'local_directory_rel_path',
        'previous_revision_uuids',
        'next_revision_uuids'
    ]

    complete_entities_list = schema_manager.get_complete_entities_list(token, sibling_list, properties_to_skip)
    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all tuplets of the given entity: sibling entities sharing an parent activity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/{id}/tuplets?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the tuplets of the target entity
"""
@app.route('/entities/<id>/tuplets', methods = ['GET'])
def get_tuplets(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    # Collection doesn't have ancestors via Activity nodes
    if normalized_entity_type == 'Collection':
        abort_bad_req(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required=True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required=True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    status = None
    property_key = None
    accepted_args = ['property', 'status']
    if bool(request.args):
        for arg_name in request.args.keys():
            if arg_name not in accepted_args:
                abort_bad_req(f"{arg_name} is an unrecognized argument")
        property_key = request.args.get('property')
        status = request.args.get('status')
        if status is not None:
            status = status.lower()
            if status not in SchemaConstants.ALLOWED_DATASET_STATUSES:
                abort_bad_req("Invalid Dataset Status. Must be 'new', 'qa', or 'published' Case-Insensitive")
        if property_key is not None:
            property_key = property_key.lower()
            result_filtering_accepted_property_keys = ['uuid']
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")
    tuplet_list = app_neo4j_queries.get_tuplets(neo4j_driver_instance, uuid, status, property_key)
    if property_key is not None:
        return jsonify(tuplet_list)
    # Generate trigger data
    # Skip some of the properties that are time-consuming to generate via triggers
    # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
    # checks when the target Dataset is public but the revisions are not public
    properties_to_skip = [
        # Properties to skip for Sample
        'direct_ancestor',
        # Properties to skip for Dataset
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'next_revision_uuid',
        'previous_revision_uuid',
        'associated_collection',
        'creation_action',
        'local_directory_rel_path',
        'previous_revision_uuids',
        'next_revision-uuids'
    ]

    complete_entities_list = schema_manager.get_complete_entities_list(token, tuplet_list, properties_to_skip)
    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all previous revisions of the given entity
Result filtering based on query string
For example: /previous_revisions/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of entities that are the previous revisions of the target entity
"""
@app.route('/previous_revisions/<id>', methods = ['GET'])
def get_previous_revisions(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            # property_list = app_neo4j_queries.get_previous_revisions(neo4j_driver_instance, uuid, property_key)
            property_multi_list = app_neo4j_queries.get_previous_multi_revisions(neo4j_driver_instance, uuid, property_key)

            # Final result
            # final_result = property_list
            final_result = property_multi_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")

        return jsonify(final_result)
    # Return all the details if no property filtering
    else:
        # descendants_list = app_neo4j_queries.get_previous_revisions(neo4j_driver_instance, uuid)
        descendants_multi_list = app_neo4j_queries.get_previous_multi_revisions(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            'collections',
            'upload',
            'title',
            'direct_ancestors'
        ]

        final_results = []
        for multi_list in descendants_multi_list:
            complete_entities_list = schema_manager.get_complete_entities_list(user_token, multi_list, properties_to_skip)
            # Final result after normalization
            final_results.append(schema_manager.normalize_entities_list_for_response(complete_entities_list))

        return jsonify(final_results)


"""
Get all next revisions of the given entity
Result filtering based on query string
For example: /next_revisions/<id>?property=uuid

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of entities that are the next revisions of the target entity
"""
@app.route('/next_revisions/<id>', methods = ['GET'])
def get_next_revisions(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Make sure the id exists in uuid-api and
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                abort_bad_req(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            # property_list = app_neo4j_queries.get_next_revisions(neo4j_driver_instance, uuid, property_key)
            property_multi_list = app_neo4j_queries.get_next_multi_revisions(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_multi_list
        else:
            abort_bad_req("The specified query string is not supported. Use '?property=<key>' to filter the result")

        return jsonify(final_result)
    # Return all the details if no property filtering
    else:
        # descendants_list = app_neo4j_queries.get_next_revisions(neo4j_driver_instance, uuid)
        descendants_multi_list = app_neo4j_queries.get_next_multi_revisions(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            'collections',
            'upload',
            'title',
            'direct_ancestors'
        ]


        final_results = []
        for multi_list in descendants_multi_list:
            complete_entities_list = schema_manager.get_complete_entities_list(user_token, multi_list, properties_to_skip)
            # Final result after normalization
            final_results.append(schema_manager.normalize_entities_list_for_response(complete_entities_list))

        return jsonify(final_results)


"""
Redirect a request from a doi service for a dataset or collection

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of the target entity
"""
# To continue supporting the already published collection DOIs
@app.route('/collection/redirect/<id>', methods = ['GET'])
# New route
@app.route('/doi/redirect/<id>', methods = ['GET'])
def doi_redirect(id):
    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)

    entity_type = entity_dict['entity_type']

    # Only for collection
    if entity_type not in ['Collection', 'Dataset', 'Publication']:
        abort_bad_req("The target entity of the specified id must be a Collection or Dataset or Publication")

    uuid = entity_dict['uuid']

    # URL template
    redirect_url = app.config['DOI_REDIRECT_URL']

    if (redirect_url.lower().find('<entity_type>') == -1) or (redirect_url.lower().find('<identifier>') == -1):
        # Log the full stack trace, prepend a line with our message
        msg = "Incorrect configuration value for 'DOI_REDIRECT_URL'"
        logger.exception(msg)
        abort_internal_err(msg)

    rep_entity_type_pattern = re.compile(re.escape('<entity_type>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_entity_type_pattern.sub(entity_type.lower(), redirect_url)

    rep_identifier_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_identifier_pattern.sub(uuid, redirect_url)

    resp = Response("Page has moved", 307)
    resp.headers['Location'] = redirect_url

    return resp


"""
Redirection method created for REFERENCE organ DOI redirection, but can be for others if needed

The gateway treats this endpoint as public accessible

Parameters
----------
snid : str
    The SenNet ID (e.g. SNT123.ABCD.456)
"""
# @app.route('/redirect/<snid>', methods = ['GET'])
# def redirect(snid):
#     cid = snid.upper().strip()
#     if cid in reference_redirects:
#         redir_url = reference_redirects[cid]
#         resp = Response("page has moved", 307)
#         resp.headers['Location'] = redir_url
#         return resp
#     else:
#         return Response(f"{snid} not found.", 404)

"""
Get the Globus URL to the given Dataset or Upload

The gateway treats this endpoint as public accessible

It will provide a Globus URL to the dataset/upload directory in of three Globus endpoints based on the access
level of the user (public, consortium or protected), public only, of course, if no token is provided.
If a dataset/upload isn't found a 404 will be returned. There is a chance that a 500 can be returned, but not
likely under normal circumstances, only for a misconfigured or failing in some way endpoint. 

If the Auth token is provided but is expired or invalid a 401 is returned. If access to the dataset/upload 
is not allowed for the user (or lack of user) a 403 is returned.

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
Response
    200 with the Globus Application URL to the directory of dataset/upload
    404 Dataset/Upload not found
    403 Access Forbidden
    401 Unauthorized (bad or expired token)
    500 Unexpected server or other error
"""
# Thd old routes for backward compatibility - will be deprecated eventually
@app.route('/entities/dataset/globus-url/<id>', methods = ['GET'])
@app.route('/dataset/globus-url/<id>', methods = ['GET'])
# New route
@app.route('/entities/<id>/globus-url', methods = ['GET'])
def get_globus_url(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    # Then retrieve the allowable data access level (public, protected or consortium)
    # for the dataset and SenNet Component ID that the dataset belongs to
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset and Upload
    if normalized_entity_type not in ['Dataset', 'Publication', 'Upload']:
        abort_bad_req("The target entity of the specified id is not a Dataset, Publication nor a Upload")

    # Upload doesn't have this 'data_access_level' property, we treat it as 'protected'
    # For Dataset, if no access level is present, default to protected too
    if not 'data_access_level' in entity_dict or string_helper.isBlank(entity_dict['data_access_level']):
        entity_data_access_level = ACCESS_LEVEL_PROTECTED
    else:
        entity_data_access_level = entity_dict['data_access_level']

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = auth_helper_instance.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
        msg = f"The 'group_uuid' property is not set for {normalized_entity_type} with uuid: {uuid}"
        logger.exception(msg)
        abort_internal_err(msg)

    group_uuid = entity_dict['group_uuid']

    # Validate the group_uuid
    try:
        schema_manager.validate_entity_group_uuid(group_uuid)
    except schema_errors.NoDataProviderGroupException:
        msg = f"Invalid 'group_uuid': {group_uuid} for {normalized_entity_type} with uuid: {uuid}"
        logger.exception(msg)
        abort_internal_err(msg)

    group_name = groups_by_id_dict[group_uuid]['displayname']

    try:
        # Get user data_access_level based on token if provided
        # If no Authorization header, default user_info['data_access_level'] == 'public'
        # The user_info contains HIGHEST access level of the user based on the token
        # This call raises an HTTPException with a 401 if any auth issues encountered
        user_info = auth_helper_instance.getUserDataAccessLevel(request)
    # If returns HTTPException with a 401, expired/invalid token
    except HTTPException:
        abort_unauthorized("The provided token is invalid or expired")

    # The user is in the Globus group with full access to thie dataset,
    # so they have protected level access to it
    if ('hmgroupids' in user_info) and (group_uuid in user_info['hmgroupids']):
        user_data_access_level = ACCESS_LEVEL_PROTECTED
    else:
        if not 'data_access_level' in user_info:
            msg = f"Unexpected error, data access level could not be found for user trying to access {normalized_entity_type} id: {id}"
            logger.exception(msg)
            return abort_internal_err(msg)

        user_data_access_level = user_info['data_access_level'].lower()

    #construct the Globus URL based on the highest level of access that the user has
    #and the level of access allowed for the dataset
    #the first "if" checks to see if the user is a member of the Consortium group
    #that allows all access to this dataset, if so send them to the "protected"
    #endpoint even if the user doesn't have full access to all protected data
    globus_server_uuid = None
    dir_path = ''

    # Note: `entity_data_access_level` for Upload is always default to 'protected'
    # public access
    if entity_data_access_level == ACCESS_LEVEL_PUBLIC:
        globus_server_uuid = app.config['GLOBUS_PUBLIC_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PUBLIC_DATA_SUBDIR'])
        dir_path = dir_path +  access_dir + "/"
    # consortium access
    elif (entity_data_access_level == ACCESS_LEVEL_CONSORTIUM) and (not user_data_access_level == ACCESS_LEVEL_PUBLIC):
        globus_server_uuid = app.config['GLOBUS_CONSORTIUM_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['CONSORTIUM_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + group_name + "/"
    # protected access
    elif (entity_data_access_level == ACCESS_LEVEL_PROTECTED) and (user_data_access_level == ACCESS_LEVEL_PROTECTED):
        globus_server_uuid = app.config['GLOBUS_PROTECTED_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PROTECTED_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + group_name + "/"

    if globus_server_uuid is None:
        abort_forbidden("Access not granted")

    dir_path = dir_path + uuid + "/"
    dir_path = urllib.parse.quote(dir_path, safe='')

    #https://app.globus.org/file-manager?origin_id=28bbb03c-a87d-4dd7-a661-7ea2fb6ea631&origin_path=%2FIEC%20Testing%20Group%2F03584b3d0f8b46de1b629f04be156879%2F
    url = hm_file_helper.ensureTrailingSlashURL(app.config['GLOBUS_APP_BASE_URL']) + "file-manager?origin_id=" + globus_server_uuid + "&origin_path=" + dir_path

    return Response(url, 200)


"""
Retrive the latest (newest) revision of a Dataset

Public/Consortium access rules apply - if no token/consortium access then 
must be for a public dataset and the returned Dataset must be the latest public version.

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity 

Returns
-------
json
    The detail of the latest revision dataset if exists
    Otherwise an empty JSON object {}
"""
@app.route('/datasets/<id>/latest-revision', methods = ['GET'])
def get_dataset_latest_revision(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset or Publication")

    latest_revision_dict = {}

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required = True)

        latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid)
    else:
        # Default to the latest "public" revision dataset
        # when no token or not a valid SenNet-Read token
        latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid, public = True)

        # Send back the real latest revision dataset if a valid SenNet-Read token presents
        if user_in_globus_read_group(request):
            latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid)

    # We'll need to return all the properties including those
    # generated by `on_read_trigger` to have a complete result
    # E.g., the 'previous_revision_uuid'
    # Here we skip the 'next_revision_uuid' property becase when the "public" latest revision dataset
    # is not the real latest revision, we don't want the users to see it
    properties_to_skip = [
        'next_revision_uuid',
        'next_revision_uuids'
    ]

    # On entity retrieval, the 'on_read_trigger' doesn't really need a token
    complete_dict = schema_manager.get_complete_entity_result(token, latest_revision_dict, properties_to_skip)

    # Also normalize the result based on schema
    final_result = schema_manager.normalize_object_result_for_response('ENTITIES', complete_dict)

    # Response with the dict
    return jsonify(final_result)


"""
Retrive the calculated revision number of a Dataset

The calculated revision is number is based on the [:REVISION_OF] relationships 
to the oldest dataset in a revision chain. 
Where the oldest dataset = 1 and each newer version is incremented by one (1, 2, 3 ...)

Public/Consortium access rules apply, if is for a non-public dataset 
and no token or a token without membership in SenNet-Read group is sent with the request 
then a 403 response should be returned.

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target entity 

Returns
-------
int
    The calculated revision number
"""
@app.route('/datasets/<id>/revision', methods = ['GET'])
def get_dataset_revision_number(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset or Publication")

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    revision_number = app_neo4j_queries.get_dataset_revision_number(neo4j_driver_instance, entity_dict['uuid'])

    # Response with the integer
    return jsonify(revision_number)


"""
Retract a published dataset with a retraction reason and sub status

Takes as input a json body with required fields "retracted_reason" and "sub_status".
Authorization handled by gateway. Only token of SenNet-Data-Admin group can use this call. 

Technically, the same can be achieved by making a PUT call to the generic entity update endpoint
with using a SenNet-Data-Admin group token. But doing this is strongly discouraged because we'll
need to add more validators to ensure when "retracted_reason" is provided, there must be a 
"sub_status" filed and vise versa. So consider this call a special use case of entity update.

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target dataset 

Returns
-------
dict
    The updated dataset details
"""
@app.route('/datasets/<id>/retract', methods=['PUT'])
def retract_dataset(id):
    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Normalize user provided status
    if "sub_status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["sub_status"])
        json_data_dict["sub_status"] = normalized_status

    # Use beblow application-level validations to avoid complicating schema validators
    # The 'retraction_reason' and `sub_status` are the only required/allowed fields. No other fields allowed.
    # Must enforce this rule otherwise we'll need to run after update triggers if any other fields
    # get passed in (which should be done using the generic entity update call)
    if 'retraction_reason' not in json_data_dict:
        abort_bad_req("Missing required field: retraction_reason")

    if 'sub_status' not in json_data_dict:
        abort_bad_req("Missing required field: sub_status")

    if len(json_data_dict) > 2:
        abort_bad_req("Only retraction_reason and sub_status are allowed fields")

    # Must be a SenNet-Data-Admin group token
    token = get_user_token(request)

    # Retrieves the neo4j data for a given entity based on the id supplied.
    # The normalized entity-type from this entity is checked to be a dataset
    # If the entity is not a dataset and the dataset is not published, cannot retract
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # A bit more application-level validation
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset or Publication")

    # Validate request json against the yaml schema
    # The given value of `sub_status` is being validated at this step
    try:
        schema_manager.validate_json_data_against_schema('ENTITIES', json_data_dict, normalized_entity_type, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        abort_bad_req(str(e))

    # Execute property level validators defined in schema yaml before entity property update
    try:
        schema_manager.execute_property_level_validators('ENTITIES', 'before_property_update_validators', normalized_entity_type, request, entity_dict, json_data_dict)
    except (schema_errors.MissingApplicationHeaderException,
            schema_errors.InvalidApplicationHeaderException,
            KeyError,
            ValueError) as e:
        abort_bad_req(e)

    # No need to call after_update() afterwards because retraction doesn't call any after_update_trigger methods
    merged_updated_dict = update_object_details('ENTITIES', request, normalized_entity_type, token, json_data_dict, entity_dict)

    complete_dict = schema_manager.get_complete_entity_result(token, merged_updated_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_object_result_for_response('ENTITIES', complete_dict)

    # Also reindex the updated entity node in elasticsearch via search-api
    reindex_entity(entity_dict['uuid'], token)

    return jsonify(normalized_complete_dict)

"""
Retrieve a list of all revisions of a dataset from the id of any dataset in the chain. 
E.g: If there are 5 revisions, and the id for revision 4 is given, a list of revisions
1-5 will be returned in reverse order (newest first). Non-public access is only required to 
retrieve information on non-published datasets. Output will be a list of dictionaries. Each dictionary
contains the dataset revision number and its uuid. Optionally, the full dataset can be included for each.

By default, only the revision number and uuid is included. To include the full dataset, the query 
parameter "include_dataset" can be given with the value of "true". If this parameter is not included or 
is set to false, the dataset will not be included. For example, to include the full datasets for each revision,
use '/datasets/<id>/revisions?include_dataset=true'. To omit the datasets, either set include_dataset=false, or
simply do not include this parameter. 

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target dataset 

Returns
-------
list
    The list of revision datasets
"""
@app.route('/entities/<id>/revisions', methods=['GET'])
@app.route('/datasets/<id>/revisions', methods=['GET'])
def get_revisions_list(id):
    # By default, do not return dataset. Only return dataset if return_dataset is true
    show_dataset = False
    if bool(request.args):
        include_dataset = request.args.get('include_dataset')
        if (include_dataset is not None) and (include_dataset.lower() == 'true'):
            show_dataset = True
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity is not a Dataset. Found entity type:" + normalized_entity_type)

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    # Get the all the sorted (DESC based on creation timestamp) revisions
    sorted_revisions_list = app_neo4j_queries.get_sorted_revisions(neo4j_driver_instance, entity_dict['uuid'])

    # Skip some of the properties that are time-consuming to generate via triggers
    properties_to_skip = [
        'direct_ancestors',
        'collections',
        'upload',
        'title'
    ]
    complete_revisions_list = schema_manager.get_complete_entities_list(token, sorted_revisions_list, properties_to_skip)
    normalized_revisions_list = schema_manager.normalize_entities_list_for_response(complete_revisions_list)

    # Only check the very last revision (the first revision dict since normalized_revisions_list is already sorted DESC)
    # to determine if send it back or not
    if not user_in_globus_read_group(request):
        latest_revision = normalized_revisions_list[0]

        if latest_revision['status'].lower() != DATASET_STATUS_PUBLISHED:
            normalized_revisions_list.pop(0)

            # Also hide the 'next_revision_uuid' of the second last revision from response
            if 'next_revision_uuid' in normalized_revisions_list[0]:
                normalized_revisions_list[0].pop('next_revision_uuid')

    # Now all we need to do is to compose the result list
    results = []
    revision_number = len(normalized_revisions_list)
    for revision in normalized_revisions_list:
        result = {
            'revision_number': revision_number,
            'uuid': revision['uuid']
        }
        if show_dataset:
            result['dataset'] = revision
        results.append(result)
        revision_number -= 1

    return jsonify(results)


"""
Retrieve a list of all multi revisions of a dataset from the id of any dataset in the chain. 
E.g: If there are 5 revisions, and the id for revision 4 is given, a list of revisions
1-5 will be returned in reverse order (newest first). Non-public access is only required to 
retrieve information on non-published datasets. Output will be a list of dictionaries. Each dictionary
contains the dataset revision number and its list of uuids. Optionally, the full dataset can be included for each.

By default, only the revision number and uuids are included. To include the full dataset, the query 
parameter "include_dataset" can be given with the value of "true". If this parameter is not included or 
is set to false, the dataset will not be included. For example, to include the full datasets for each revision,
use '/datasets/<id>/multi-revisions?include_dataset=true'. To omit the datasets, either set include_dataset=false, or
simply do not include this parameter. 

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of target dataset 

Returns
-------
list
    The list of revision datasets
"""
@app.route('/entities/<id>/multi-revisions', methods=['GET'])
@app.route('/datasets/<id>/multi-revisions', methods=['GET'])
def get_multi_revisions_list(id):
    # By default, do not return dataset. Only return dataset if include_dataset is true
    property_key = 'uuid'
    if bool(request.args):
        include_dataset = request.args.get('include_dataset')
        if (include_dataset is not None) and (include_dataset.lower() == 'true'):
            property_key = None
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity is not a Dataset. Found entity type:" + normalized_entity_type)

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    # Get the all the sorted (DESC based on creation timestamp) revisions
    sorted_revisions_list = app_neo4j_queries.get_sorted_multi_revisions(neo4j_driver_instance, entity_dict['uuid'],
                                                                         fetch_all=user_in_globus_read_group(request),
                                                                         property_key=property_key)

    # Skip some of the properties that are time-consuming to generate via triggers
    properties_to_skip = [
        'direct_ancestors',
        'collections',
        'upload',
        'title'
    ]

    normalized_revisions_list = []
    sorted_revisions_list_merged = sorted_revisions_list[0] + sorted_revisions_list[1][::-1]

    if property_key is None:
        for revision in sorted_revisions_list_merged:
            complete_revision_list = schema_manager.get_complete_entities_list(token, revision, properties_to_skip)
            normal = schema_manager.normalize_entities_list_for_response(complete_revision_list)
            normalized_revisions_list.append(normal)
    else:
        normalized_revisions_list = sorted_revisions_list_merged

    # Now all we need to do is to compose the result list
    results = []
    revision_number = len(normalized_revisions_list)
    for revision in normalized_revisions_list:
        result = {
            'revision_number': revision_number,
            'uuids': revision
        }
        results.append(result)
        revision_number -= 1

    return jsonify(results)


"""
Get all organs associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the organs associated with the target dataset
"""
@app.route('/datasets/<id>/organs', methods=['GET'])
def get_associated_organs_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset or Publication")

    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    associated_organs = app_neo4j_queries.get_associated_organs_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated organs, then there are no associated
    # Organs and a 404 will be returned.
    if len(associated_organs) < 1:
        abort_not_found("the dataset does not have any associated organs")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_organs)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all samples associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the samples associated with the target dataset
"""
@app.route('/datasets/<id>/samples', methods=['GET'])
def get_associated_samples_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset")

    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or the user token has the correct access level
    associated_samples = app_neo4j_queries.get_associated_samples_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated_samples, then there are no associated
    # samples and a 404 will be returned.
    if len(associated_samples) < 1:
        abort_not_found("the dataset does not have any associated samples")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_samples)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all sources associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The SenNet ID (e.g. SNT123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the sources associated with the target dataset
"""
@app.route('/datasets/<id>/sources', methods=['GET'])
def get_associated_sources_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        abort_bad_req("The entity of given id is not a Dataset")

    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to SenNet-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or the user token has the correct access level
    associated_sources = app_neo4j_queries.get_associated_sources_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated_sources, then there are no associated
    # sources and a 404 will be returned.
    if len(associated_sources) < 1:
        abort_not_found("the dataset does not have any associated sources")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_sources)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get the complete provenance info for all datasets

Authentication
-------
No token is required, however if a token is given it must be valid or an error will be raised. If no token with HuBMAP
Read Group access is given, only datasets designated as "published" will be returned

Query Parameters
-------
    format : string
        Designates the output format of the returned data. Accepted values are "json" and "tsv". If none provided, by 
        default will return a tsv.
    group_uuid : string
        Filters returned datasets by a given group uuid. 
    organ : string
        Filters returned datasets related to a samples of the given organ. Accepts 2 character organ codes. These codes
        must match the organ types yaml at https://raw.githubusercontent.com/sennetconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml
        or an error will be raised
    has_rui_info : string
        Accepts strings "true" or "false. Any other value will result in an error. If true, only datasets connected to 
        an sample that contain rui info will be returned. If false, only datasets that are NOT connected to samples
        containing rui info will be returned. By default, no filtering is performed. 
    dataset_status : string
        Filters results by dataset status. Accepted values are "Published", "QA", and "NEW". If a user only has access
        to published datasets and enters QA or New, an error will be raised. By default, no filtering is performed 

Returns
-------
json
    an array of each datatset's provenance info
tsv
    a text file of tab separated values where each row is a dataset and the columns include all its prov info
"""
@app.route('/datasets/prov-info', methods=['GET'])
def get_prov_info():
    # String constants
    HEADER_DATASET_UUID = 'dataset_uuid'
    HEADER_DATASET_SENNET_ID = 'dataset_sennet_id'
    HEADER_DATASET_STATUS = 'dataset_status'
    HEADER_DATASET_GROUP_NAME = 'dataset_group_name'
    HEADER_DATASET_GROUP_UUID = 'dataset_group_uuid'
    HEADER_DATASET_DATE_TIME_CREATED = 'dataset_date_time_created'
    HEADER_DATASET_CREATED_BY_EMAIL = 'dataset_created_by_email'
    HEADER_DATASET_DATE_TIME_MODIFIED = 'dataset_date_time_modified'
    HEADER_DATASET_MODIFIED_BY_EMAIL = 'dataset_modified_by_email'
    HEADER_DATASET_LAB_ID = 'lab_id_or_name'
    HEADER_DATASET_DATASET_TYPE = 'dataset_dataset_type'
    HEADER_DATASET_PORTAL_URL = 'dataset_portal_url'
    HEADER_FIRST_SAMPLE_SENNET_ID = 'first_sample_sennet_id'
    HEADER_FIRST_SAMPLE_UUID = 'first_sample_uuid'
    HEADER_FIRST_SAMPLE_CATEGORY = 'first_sample_category'
    HEADER_FIRST_SAMPLE_PORTAL_URL = 'first_sample_portal_url'
    HEADER_ORGAN_SENNET_ID = 'organ_sennet_id'
    HEADER_ORGAN_UUID = 'organ_uuid'
    HEADER_ORGAN_TYPE = 'organ_type'
    HEADER_SOURCE_SENNET_ID = 'source_sennet_id'
    HEADER_SOURCE_UUID = 'source_uuid'
    HEADER_SOURCE_GROUP_NAME = 'source_group_name'
    HEADER_RUI_LOCATION_SENNET_ID = 'rui_location_sennet_id'
    HEADER_RUI_LOCATION_UUID = 'rui_location_uuid'
    HEADER_SAMPLE_METADATA_SENNET_ID = 'sample_metadata_sennet_id'
    HEADER_SAMPLE_METADATA_UUID = 'sample_metadata_uuid'
    HEADER_PROCESSED_DATASET_UUID = 'processed_dataset_uuid'
    HEADER_PROCESSED_DATASET_SENNET_ID = 'processed_dataset_sennet_id'
    HEADER_PROCESSED_DATASET_STATUS = 'processed_dataset_status'
    HEADER_PROCESSED_DATASET_PORTAL_URL = 'processed_dataset_portal_url'
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, data_as_val=True, val_key='rui_code').organ_types()
    HEADER_PREVIOUS_VERSION_SENNET_IDS = 'previous_version_sennet_ids'

    headers = [
        HEADER_DATASET_UUID, HEADER_DATASET_SENNET_ID, HEADER_DATASET_STATUS, HEADER_DATASET_GROUP_NAME,
        HEADER_DATASET_GROUP_UUID, HEADER_DATASET_DATE_TIME_CREATED, HEADER_DATASET_CREATED_BY_EMAIL,
        HEADER_DATASET_DATE_TIME_MODIFIED, HEADER_DATASET_MODIFIED_BY_EMAIL, HEADER_DATASET_LAB_ID,
        HEADER_DATASET_DATASET_TYPE, HEADER_DATASET_PORTAL_URL, HEADER_FIRST_SAMPLE_SENNET_ID,
        HEADER_FIRST_SAMPLE_UUID, HEADER_FIRST_SAMPLE_CATEGORY,
        HEADER_FIRST_SAMPLE_PORTAL_URL, HEADER_ORGAN_SENNET_ID, HEADER_ORGAN_UUID,
        HEADER_ORGAN_TYPE, HEADER_SOURCE_SENNET_ID, HEADER_SOURCE_UUID,
        HEADER_SOURCE_GROUP_NAME, HEADER_RUI_LOCATION_SENNET_ID,
        HEADER_RUI_LOCATION_UUID, HEADER_SAMPLE_METADATA_SENNET_ID,
        HEADER_SAMPLE_METADATA_UUID, HEADER_PROCESSED_DATASET_UUID, HEADER_PROCESSED_DATASET_SENNET_ID,
        HEADER_PROCESSED_DATASET_STATUS, HEADER_PROCESSED_DATASET_PORTAL_URL, HEADER_PREVIOUS_VERSION_SENNET_IDS
    ]
    published_only = True

    # Token is not required, but if an invalid token is provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    if user_in_globus_read_group(request):
        published_only = False

    # Processing and validating query parameters
    accepted_arguments = ['format', 'organ', 'has_rui_info', 'dataset_status', 'group_uuid']
    return_json = False
    param_dict = {}
    if bool(request.args):
        for argument in request.args:
            if argument not in accepted_arguments:
                abort_bad_req(f"{argument} is an unrecognized argument.")
        return_format = request.args.get('format')
        if return_format is not None:
            if return_format.lower() not in ['json', 'tsv']:
                abort_bad_req(
                    "Invalid Format. Accepted formats are json and tsv. If no format is given, TSV will be the default")
            if return_format.lower() == 'json':
                return_json = True
        group_uuid = request.args.get('group_uuid')
        if group_uuid is not None:
            groups_by_id_dict = auth_helper_instance.get_globus_groups_info()['by_id']
            if group_uuid not in groups_by_id_dict:
                abort_bad_req(
                    f"Invalid Group UUID.")
            if not groups_by_id_dict[group_uuid]['data_provider']:
                abort_bad_req(f"Invalid Group UUID. Group must be a data provider")
            param_dict['group_uuid'] = group_uuid
        organ = request.args.get('organ')
        if organ is not None:
            validate_organ_code(organ)
            param_dict['organ'] = organ
        has_rui_info = request.args.get('has_rui_info')
        if has_rui_info is not None:
            if has_rui_info.lower() not in ['true', 'false']:
                abort_bad_req("Invalid value for 'has_rui_info'. Only values of true or false are acceptable")
            param_dict['has_rui_info'] = has_rui_info
        dataset_status = request.args.get('dataset_status')
        if dataset_status is not None:
            if dataset_status.lower() not in ['new', 'qa', 'published']:
                abort_bad_req("Invalid Dataset Status. Must be 'new', 'qa', or 'published' Case-Insensitive")
            if published_only and dataset_status.lower() != 'published':
                abort_bad_req(f"Invalid Dataset Status. No auth token given or token is not a member of HuBMAP-Read"
                                  " Group. If no token with HuBMAP-Read Group access is given, only datasets marked "
                                  "'Published' are available. Try again with a proper token, or change/remove "
                                  "dataset_status")
            if not published_only:
                param_dict['dataset_status'] = dataset_status

    # Instantiation of the list dataset_prov_list
    dataset_prov_list = []

    # Call to app_neo4j_queries to prepare and execute the database query
    prov_info = app_neo4j_queries.get_prov_info(neo4j_driver_instance, param_dict, published_only)

    # Each dataset's provinence info is placed into a dictionary
    for dataset in prov_info:
        internal_dict = collections.OrderedDict()
        internal_dict[HEADER_DATASET_UUID] = dataset['uuid']
        internal_dict[HEADER_DATASET_SENNET_ID] = dataset['sennet_id']
        internal_dict[HEADER_DATASET_STATUS] = dataset['status']
        internal_dict[HEADER_DATASET_GROUP_NAME] = dataset['group_name']
        internal_dict[HEADER_DATASET_GROUP_UUID] = dataset['group_uuid']
        internal_dict[HEADER_DATASET_DATE_TIME_CREATED] = datetime.fromtimestamp(int(dataset['created_timestamp']/1000.0))
        internal_dict[HEADER_DATASET_CREATED_BY_EMAIL] = dataset['created_by_user_email']
        internal_dict[HEADER_DATASET_DATE_TIME_MODIFIED] = datetime.fromtimestamp(int(dataset['last_modified_timestamp']/1000.0))
        internal_dict[HEADER_DATASET_MODIFIED_BY_EMAIL] = dataset['last_modified_user_email']
        internal_dict[HEADER_DATASET_LAB_ID] = dataset['lab_dataset_id']
        internal_dict[HEADER_DATASET_DATASET_TYPE] = dataset['dataset_type']

        internal_dict[HEADER_DATASET_PORTAL_URL] = app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace('<identifier>', dataset['uuid'])

        # first_sample properties are retrieved from its own dictionary
        if dataset['first_sample'] is not None:
            first_sample_sennet_id_list = []
            first_sample_uuid_list = []
            first_sample_category_list = []
            first_sample_portal_url_list = []
            for item in dataset['first_sample']:
                first_sample_sennet_id_list.append(item['sennet_id'])
                first_sample_uuid_list.append(item['uuid'])
                first_sample_category_list.append(item['sample_category'])
                first_sample_portal_url_list.append(app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'sample').replace('<identifier>', item['uuid']))
            internal_dict[HEADER_FIRST_SAMPLE_SENNET_ID] = first_sample_sennet_id_list
            internal_dict[HEADER_FIRST_SAMPLE_UUID] = first_sample_uuid_list
            internal_dict[HEADER_FIRST_SAMPLE_CATEGORY] = first_sample_category_list
            internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = first_sample_portal_url_list
            if return_json is False:
                internal_dict[HEADER_FIRST_SAMPLE_SENNET_ID] = ",".join(first_sample_sennet_id_list)
                internal_dict[HEADER_FIRST_SAMPLE_UUID] = ",".join(first_sample_uuid_list)
                internal_dict[HEADER_FIRST_SAMPLE_CATEGORY] = ",".join(first_sample_category_list)
                internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = ",".join(first_sample_portal_url_list)

        # distinct_organ properties are retrieved from its own dictionary
        if dataset['distinct_organ'] is not None:
            distinct_organ_sennet_id_list = []
            distinct_organ_uuid_list = []
            distinct_organ_type_list = []

            for item in dataset['distinct_organ']:
                distinct_organ_sennet_id_list.append(item['sennet_id'])
                distinct_organ_uuid_list.append(item['uuid'])
                for organ_type in ORGAN_TYPES:
                    if ORGAN_TYPES[organ_type]['rui_code'] == item['organ']:
                        distinct_organ_type_list.append(ORGAN_TYPES[organ_type]['term'])
                        break
            internal_dict[HEADER_ORGAN_SENNET_ID] = distinct_organ_sennet_id_list
            internal_dict[HEADER_ORGAN_UUID] = distinct_organ_uuid_list
            internal_dict[HEADER_ORGAN_TYPE] = distinct_organ_type_list
            if return_json is False:
                internal_dict[HEADER_ORGAN_SENNET_ID] = ",".join(distinct_organ_sennet_id_list)
                internal_dict[HEADER_ORGAN_UUID] = ",".join(distinct_organ_uuid_list)
                internal_dict[HEADER_ORGAN_TYPE] = ",".join(distinct_organ_type_list)

        # distinct_source properties are retrieved from its own dictionary
        if dataset['distinct_source'] is not None:
            distinct_source_sennet_id_list = []
            distinct_source_uuid_list = []
            distinct_source_group_name_list = []
            for item in dataset['distinct_source']:
                distinct_source_sennet_id_list.append(item['sennet_id'])
                distinct_source_uuid_list.append(item['uuid'])
                distinct_source_group_name_list.append(item['group_name'])
            internal_dict[HEADER_SOURCE_SENNET_ID] = distinct_source_sennet_id_list
            internal_dict[HEADER_SOURCE_UUID] = distinct_source_uuid_list
            internal_dict[HEADER_SOURCE_GROUP_NAME] = distinct_source_group_name_list
            if return_json is False:
                internal_dict[HEADER_SOURCE_SENNET_ID] = ",".join(distinct_source_sennet_id_list)
                internal_dict[HEADER_SOURCE_UUID] = ",".join(distinct_source_uuid_list)
                internal_dict[HEADER_SOURCE_GROUP_NAME] = ",".join(distinct_source_group_name_list)

        # distinct_rui_sample properties are retrieved from its own dictionary
        if dataset['distinct_rui_sample'] is not None:
            rui_location_sennet_id_list = []
            rui_location_uuid_list = []
            for item in dataset['distinct_rui_sample']:
                rui_location_sennet_id_list.append(item['sennet_id'])
                rui_location_uuid_list.append(item['uuid'])
            internal_dict[HEADER_RUI_LOCATION_SENNET_ID] = rui_location_sennet_id_list
            internal_dict[HEADER_RUI_LOCATION_UUID] = rui_location_uuid_list
            if return_json is False:
                internal_dict[HEADER_RUI_LOCATION_SENNET_ID] = ",".join(rui_location_sennet_id_list)
                internal_dict[HEADER_RUI_LOCATION_UUID] = ",".join(rui_location_uuid_list)

        # distinct_metasample properties are retrieved from its own dictionary
        if dataset['distinct_metasample'] is not None:
            metasample_sennet_id_list = []
            metasample_uuid_list = []
            for item in dataset['distinct_metasample']:
                metasample_sennet_id_list.append(item['sennet_id'])
                metasample_uuid_list.append(item['uuid'])
            internal_dict[HEADER_SAMPLE_METADATA_SENNET_ID] = metasample_sennet_id_list
            internal_dict[HEADER_SAMPLE_METADATA_UUID] = metasample_uuid_list
            if return_json is False:
                internal_dict[HEADER_SAMPLE_METADATA_SENNET_ID] = ",".join(metasample_sennet_id_list)
                internal_dict[HEADER_SAMPLE_METADATA_UUID] = ",".join(metasample_uuid_list)

        # processed_dataset properties are retrived from its own dictionary
        if dataset['processed_dataset'] is not None:
            processed_dataset_uuid_list = []
            processed_dataset_sennet_id_list = []
            processed_dataset_status_list = []
            processed_dataset_portal_url_list = []
            for item in dataset['processed_dataset']:
                processed_dataset_uuid_list.append(item['uuid'])
                processed_dataset_sennet_id_list.append(item['sennet_id'])
                processed_dataset_status_list.append(item['status'])
                processed_dataset_portal_url_list.append(app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace('<identifier>', item['uuid']))
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = processed_dataset_uuid_list
            internal_dict[HEADER_PROCESSED_DATASET_SENNET_ID] = processed_dataset_sennet_id_list
            internal_dict[HEADER_PROCESSED_DATASET_STATUS] = processed_dataset_status_list
            internal_dict[HEADER_PROCESSED_DATASET_PORTAL_URL] = processed_dataset_portal_url_list
            if return_json is False:
                internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_uuid_list)
                internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_sennet_id_list)
                internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_status_list)
                internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_portal_url_list)


        if dataset['previous_version_sennet_ids'] is not None:
            previous_version_sennet_ids_list = []
            for item in dataset['previous_version_sennet_ids']:
                previous_version_sennet_ids_list.append(item)
            internal_dict[HEADER_PREVIOUS_VERSION_SENNET_IDS] = previous_version_sennet_ids_list
            if return_json is False:
                internal_dict[HEADER_PREVIOUS_VERSION_SENNET_IDS] = ",".join(previous_version_sennet_ids_list)

        # Each dataset's dictionary is added to the list to be returned
        dataset_prov_list.append(internal_dict)

    # if return_json is true, this dictionary is ready to be returned already
    if return_json:
        return jsonify(dataset_prov_list)

    # if return_json is false, the data must be converted to be returned as a tsv
    else:
        new_tsv_file = StringIO()
        writer = csv.DictWriter(new_tsv_file, fieldnames=headers, delimiter='\t')
        writer.writeheader()
        writer.writerows(dataset_prov_list)
        new_tsv_file.seek(0)
        output = Response(new_tsv_file, mimetype='text/tsv')
        output.headers['Content-Disposition'] = 'attachment; filename=prov-info.tsv'
        return output


"""
Get the complete provenance info for a given dataset

Authentication
-------
No token is required, however if a token is given it must be valid or an error will be raised. If no token with HuBMAP
Read Group access is given, only datasets designated as "published" will be returned

Query Parameters
-------
format : string
        Designates the output format of the returned data. Accepted values are "json" and "tsv". If none provided, by 
        default will return a tsv.

Path Parameters
-------
id : string
    A HuBMAP_ID or UUID for a dataset. If an invalid dataset id is given, an error will be raised    

Returns
-------
json
    an array of each datatset's provenance info
tsv
    a text file of tab separated values where each row is a dataset and the columns include all its prov info
"""
@app.route('/datasets/<id>/prov-info', methods=['GET'])
def get_prov_info_for_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if normalized_entity_type != 'Dataset':
        abort_bad_req("The entity of given id is not a Dataset")

    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required=True)

    return_json = False
    dataset_prov_list = []
    include_samples = []
    if bool(request.args):
        return_format = request.args.get('format')
        if (return_format is not None) and (return_format.lower() == 'json'):
            return_json = True
        include_samples_req = request.args.get('include_samples')
        if (include_samples_req is not None):
            include_samples = include_samples_req.lower().split(',')

    HEADER_DATASET_UUID = 'dataset_uuid'
    HEADER_DATASET_SENNET_ID = 'dataset_sennet_id'
    HEADER_DATASET_STATUS = 'dataset_status'
    HEADER_DATASET_GROUP_NAME = 'dataset_group_name'
    HEADER_DATASET_GROUP_UUID = 'dataset_group_uuid'
    HEADER_DATASET_DATE_TIME_CREATED = 'dataset_date_time_created'
    HEADER_DATASET_CREATED_BY_EMAIL = 'dataset_created_by_email'
    HEADER_DATASET_DATE_TIME_MODIFIED = 'dataset_date_time_modified'
    HEADER_DATASET_MODIFIED_BY_EMAIL = 'dataset_modified_by_email'
    HEADER_DATASET_LAB_ID = 'lab_id_or_name'
    HEADER_DATASET_DATASET_TYPE = 'dataset_dataset_type'
    HEADER_DATASET_PORTAL_URL = 'dataset_portal_url'
    HEADER_FIRST_SAMPLE_SENNET_ID = 'first_sample_sennet_id'
    HEADER_FIRST_SAMPLE_UUID = 'first_sample_uuid'
    HEADER_FIRST_SAMPLE_CATEGORY = 'first_sample_category'
    HEADER_FIRST_SAMPLE_PORTAL_URL = 'first_sample_portal_url'
    HEADER_ORGAN_SENNET_ID = 'organ_sennet_id'
    HEADER_ORGAN_UUID = 'organ_uuid'
    HEADER_ORGAN_TYPE = 'organ_type'
    HEADER_SOURCE_SENNET_ID = 'source_sennet_id'
    HEADER_SOURCE_UUID = 'source_uuid'
    HEADER_SOURCE_GROUP_NAME = 'source_group_name'
    HEADER_RUI_LOCATION_SENNET_ID = 'rui_location_sennet_id'
    HEADER_RUI_LOCATION_UUID = 'rui_location_uuid'
    HEADER_SAMPLE_METADATA_SENNET_ID = 'sample_metadata_sennet_id'
    HEADER_SAMPLE_METADATA_UUID = 'sample_metadata_uuid'
    HEADER_PROCESSED_DATASET_UUID = 'processed_dataset_uuid'
    HEADER_PROCESSED_DATASET_SENNET_ID = 'processed_dataset_sennet_id'
    HEADER_PROCESSED_DATASET_STATUS = 'processed_dataset_status'
    HEADER_PROCESSED_DATASET_PORTAL_URL = 'processed_dataset_portal_url'
    HEADER_DATASET_SAMPLES = "dataset_samples"
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, data_as_val=True, val_key='rui_code').organ_types()

    headers = [
        HEADER_DATASET_UUID, HEADER_DATASET_SENNET_ID, HEADER_DATASET_STATUS, HEADER_DATASET_GROUP_NAME,
        HEADER_DATASET_GROUP_UUID, HEADER_DATASET_DATE_TIME_CREATED, HEADER_DATASET_CREATED_BY_EMAIL,
        HEADER_DATASET_DATE_TIME_MODIFIED, HEADER_DATASET_MODIFIED_BY_EMAIL, HEADER_DATASET_LAB_ID,
        HEADER_DATASET_DATASET_TYPE, HEADER_DATASET_PORTAL_URL, HEADER_FIRST_SAMPLE_SENNET_ID,
        HEADER_FIRST_SAMPLE_UUID, HEADER_FIRST_SAMPLE_CATEGORY,
        HEADER_FIRST_SAMPLE_PORTAL_URL, HEADER_ORGAN_SENNET_ID, HEADER_ORGAN_UUID,
        HEADER_ORGAN_TYPE, HEADER_SOURCE_SENNET_ID, HEADER_SOURCE_UUID,
        HEADER_SOURCE_GROUP_NAME, HEADER_RUI_LOCATION_SENNET_ID,
        HEADER_RUI_LOCATION_UUID, HEADER_SAMPLE_METADATA_SENNET_ID,
        HEADER_SAMPLE_METADATA_UUID, HEADER_PROCESSED_DATASET_UUID, HEADER_PROCESSED_DATASET_SENNET_ID,
        HEADER_PROCESSED_DATASET_STATUS, HEADER_PROCESSED_DATASET_PORTAL_URL
    ]

    sennet_ids = schema_manager.get_sennet_ids(id)

    # Get the target uuid if all good
    uuid = sennet_ids['hm_uuid']
    dataset = app_neo4j_queries.get_individual_prov_info(neo4j_driver_instance, uuid)
    if dataset is None:
        abort_bad_req("Query For this Dataset Returned no Records. Make sure this is a Primary Dataset")
    internal_dict = collections.OrderedDict()
    internal_dict[HEADER_DATASET_SENNET_ID] = dataset['sennet_id']
    internal_dict[HEADER_DATASET_UUID] = dataset['uuid']
    internal_dict[HEADER_DATASET_STATUS] = dataset['status']
    internal_dict[HEADER_DATASET_GROUP_NAME] = dataset['group_name']
    internal_dict[HEADER_DATASET_GROUP_UUID] = dataset['group_uuid']
    internal_dict[HEADER_DATASET_DATE_TIME_CREATED] = datetime.fromtimestamp(int(dataset['created_timestamp'] / 1000.0))
    internal_dict[HEADER_DATASET_CREATED_BY_EMAIL] = dataset['created_by_user_email']
    internal_dict[HEADER_DATASET_DATE_TIME_MODIFIED] = datetime.fromtimestamp(
        int(dataset['last_modified_timestamp'] / 1000.0))
    internal_dict[HEADER_DATASET_MODIFIED_BY_EMAIL] = dataset['last_modified_user_email']
    internal_dict[HEADER_DATASET_LAB_ID] = dataset['lab_dataset_id']
    internal_dict[HEADER_DATASET_DATASET_TYPE] = dataset['dataset_type']

    internal_dict[HEADER_DATASET_PORTAL_URL] = app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace(
        '<identifier>', dataset['uuid'])
    if dataset['first_sample'] is not None:
        first_sample_sennet_id_list = []
        first_sample_uuid_list = []
        first_sample_category_list = []
        first_sample_portal_url_list = []
        for item in dataset['first_sample']:
            first_sample_sennet_id_list.append(item['sennet_id'])
            first_sample_uuid_list.append(item['uuid'])
            first_sample_category_list.append(item['sample_category'])
            first_sample_portal_url_list.append(
                app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'sample').replace('<identifier>', item['uuid']))
        internal_dict[HEADER_FIRST_SAMPLE_SENNET_ID] = first_sample_sennet_id_list
        internal_dict[HEADER_FIRST_SAMPLE_UUID] = first_sample_uuid_list
        internal_dict[HEADER_FIRST_SAMPLE_CATEGORY] = first_sample_category_list
        internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = first_sample_portal_url_list
        if return_json is False:
            internal_dict[HEADER_FIRST_SAMPLE_SENNET_ID] = ",".join(first_sample_sennet_id_list)
            internal_dict[HEADER_FIRST_SAMPLE_UUID] = ",".join(first_sample_uuid_list)
            internal_dict[HEADER_FIRST_SAMPLE_CATEGORY] = ",".join(first_sample_category_list)
            internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = ",".join(first_sample_portal_url_list)
    if dataset['distinct_organ'] is not None:
        distinct_organ_sennet_id_list = []
        distinct_organ_uuid_list = []
        distinct_organ_type_list = []
        for item in dataset['distinct_organ']:
            distinct_organ_sennet_id_list.append(item['sennet_id'])
            distinct_organ_uuid_list.append(item['uuid'])
            for organ_type in ORGAN_TYPES:
                if ORGAN_TYPES[organ_type]['rui_code'] == item['organ']:
                    distinct_organ_type_list.append(ORGAN_TYPES[organ_type]['term'])
                    break
        internal_dict[HEADER_ORGAN_SENNET_ID] = distinct_organ_sennet_id_list
        internal_dict[HEADER_ORGAN_UUID] = distinct_organ_uuid_list
        internal_dict[HEADER_ORGAN_TYPE] = distinct_organ_type_list
        if return_json is False:
            internal_dict[HEADER_ORGAN_SENNET_ID] = ",".join(distinct_organ_sennet_id_list)
            internal_dict[HEADER_ORGAN_UUID] = ",".join(distinct_organ_uuid_list)
            internal_dict[HEADER_ORGAN_TYPE] = ",".join(distinct_organ_type_list)
    if dataset['distinct_source'] is not None:
        distinct_source_sennet_id_list = []
        distinct_source_uuid_list = []
        distinct_source_group_name_list = []
        for item in dataset['distinct_source']:
            distinct_source_sennet_id_list.append(item['sennet_id'])
            distinct_source_uuid_list.append(item['uuid'])
            distinct_source_group_name_list.append(item['group_name'])
        internal_dict[HEADER_SOURCE_SENNET_ID] = distinct_source_sennet_id_list
        internal_dict[HEADER_SOURCE_UUID] = distinct_source_uuid_list
        internal_dict[HEADER_SOURCE_GROUP_NAME] = distinct_source_group_name_list
        if return_json is False:
            internal_dict[HEADER_SOURCE_SENNET_ID] = ",".join(distinct_source_sennet_id_list)
            internal_dict[HEADER_SOURCE_UUID] = ",".join(distinct_source_uuid_list)
            internal_dict[HEADER_SOURCE_GROUP_NAME] = ",".join(distinct_source_group_name_list)
    if dataset['distinct_rui_sample'] is not None:
        rui_location_sennet_id_list = []
        rui_location_uuid_list = []
        for item in dataset['distinct_rui_sample']:
            rui_location_sennet_id_list.append(item['sennet_id'])
            rui_location_uuid_list.append(item['uuid'])
        internal_dict[HEADER_RUI_LOCATION_SENNET_ID] = rui_location_sennet_id_list
        internal_dict[HEADER_RUI_LOCATION_UUID] = rui_location_uuid_list
        if return_json is False:
            internal_dict[HEADER_RUI_LOCATION_SENNET_ID] = ",".join(rui_location_sennet_id_list)
            internal_dict[HEADER_RUI_LOCATION_UUID] = ",".join(rui_location_uuid_list)
    if dataset['distinct_metasample'] is not None:
        metasample_sennet_id_list = []
        metasample_uuid_list = []
        for item in dataset['distinct_metasample']:
            metasample_sennet_id_list.append(item['sennet_id'])
            metasample_uuid_list.append(item['uuid'])
        internal_dict[HEADER_SAMPLE_METADATA_SENNET_ID] = metasample_sennet_id_list
        internal_dict[HEADER_SAMPLE_METADATA_UUID] = metasample_uuid_list
        if return_json is False:
            internal_dict[HEADER_SAMPLE_METADATA_SENNET_ID] = ",".join(metasample_sennet_id_list)
            internal_dict[HEADER_SAMPLE_METADATA_UUID] = ",".join(metasample_uuid_list)

    # processed_dataset properties are retrived from its own dictionary
    if dataset['processed_dataset'] is not None:
        processed_dataset_uuid_list = []
        processed_dataset_sennet_id_list = []
        processed_dataset_status_list = []
        processed_dataset_portal_url_list = []
        for item in dataset['processed_dataset']:
            processed_dataset_uuid_list.append(item['uuid'])
            processed_dataset_sennet_id_list.append(item['sennet_id'])
            processed_dataset_status_list.append(item['status'])
            processed_dataset_portal_url_list.append(
                app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace('<identifier>',
                                                                                           item['uuid']))
        internal_dict[HEADER_PROCESSED_DATASET_UUID] = processed_dataset_uuid_list
        internal_dict[HEADER_PROCESSED_DATASET_SENNET_ID] = processed_dataset_sennet_id_list
        internal_dict[HEADER_PROCESSED_DATASET_STATUS] = processed_dataset_status_list
        internal_dict[HEADER_PROCESSED_DATASET_PORTAL_URL] = processed_dataset_portal_url_list
        if return_json is False:
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_uuid_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_sennet_id_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_status_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_portal_url_list)

    if include_samples:
        headers.append(HEADER_DATASET_SAMPLES)
        dataset_samples = app_neo4j_queries.get_all_dataset_samples(neo4j_driver_instance, uuid)
        logger.debug(f"dataset_samples={str(dataset_samples)}")
        if 'all' in include_samples:
            internal_dict[HEADER_DATASET_SAMPLES] = dataset_samples
        else:
            requested_samples = {}
            for uuid in dataset_samples.keys():
                if dataset_samples[uuid]['sample_category'] in include_samples:
                    requested_samples[uuid] = dataset_samples[uuid]
            internal_dict[HEADER_DATASET_SAMPLES] = requested_samples

    dataset_prov_list.append(internal_dict)


    if return_json:
        return jsonify(dataset_prov_list[0])
    else:
        new_tsv_file = StringIO()
        writer = csv.DictWriter(new_tsv_file, fieldnames=headers, delimiter='\t')
        writer.writeheader()
        writer.writerows(dataset_prov_list)
        new_tsv_file.seek(0)
        output = Response(new_tsv_file, mimetype='text/tsv')
        output.headers['Content-Disposition'] = 'attachment; filename=prov-info.tsv'
        return output


"""
Get the information needed to generate the sankey on software-docs as a json.

Authentication
-------
No token is required or checked. The information returned is what is displayed in the public sankey

Query Parameters
-------
N/A

Path Parameters
-------
N/A

Returns
-------
json
    a json array. Each item in the array corresponds to a dataset. Each dataset has the values: dataset_group_name, 
    organ_type, dataset_data_types, and dataset_status, each of which is a string. 

"""
@app.route('/datasets/sankey_data', methods=['GET'])
def sankey_data():
    # String constants
    HEADER_DATASET_GROUP_NAME = 'dataset_group_name'
    HEADER_ORGAN_TYPE = 'organ_type'
    HEADER_DATASET_DATASET_TYPE = 'dataset_type'
    HEADER_DATASET_STATUS = 'dataset_status'
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, data_as_val=True, val_key='rui_code').organ_types()
    with open('sankey_mapping.json') as f:
        mapping_dict = json.load(f)

    # Instantiation of the list dataset_prov_list
    dataset_sankey_list = []

    # Call to app_neo4j_queries to prepare and execute the database query
    sankey_info = app_neo4j_queries.get_sankey_info(neo4j_driver_instance)
    for dataset in sankey_info:
        internal_dict = collections.OrderedDict()
        internal_dict[HEADER_DATASET_GROUP_NAME] = dataset[HEADER_DATASET_GROUP_NAME]
        # TODO: Need to update this code once Ontology Organ Types endpoint is update
        for organ_type in ORGAN_TYPES:
            if ORGAN_TYPES[organ_type]['rui_code'] == dataset[HEADER_ORGAN_TYPE]:
                internal_dict[HEADER_ORGAN_TYPE] = ORGAN_TYPES[organ_type]['term']
                break

        internal_dict[HEADER_DATASET_DATASET_TYPE] = dataset[HEADER_DATASET_DATASET_TYPE]

        # Replace applicable Group Name and Data type with the value needed for the sankey via the mapping_dict
        internal_dict[HEADER_DATASET_STATUS] = dataset['dataset_status']
        if internal_dict[HEADER_DATASET_GROUP_NAME] in mapping_dict.keys():
            internal_dict[HEADER_DATASET_GROUP_NAME] = mapping_dict[internal_dict[HEADER_DATASET_GROUP_NAME]]

        # Each dataset's dictionary is added to the list to be returned
        dataset_sankey_list.append(internal_dict)
    return jsonify(dataset_sankey_list)


"""
Get the complete provenance info for all samples

Authentication
-------
Token that is part of the HuBMAP Read-Group is required.

Query Parameters
-------
    group_uuid : string
        Filters returned samples by a given group uuid. 

Returns
-------
json
    an array of each datatset's provenance info
"""
@app.route('/samples/prov-info', methods=['GET'])
def get_sample_prov_info():
    # String Constants
    HEADER_SAMPLE_UUID = "sample_uuid"
    HEADER_SAMPLE_LAB_ID = "lab_id_or_name"
    HEADER_SAMPLE_GROUP_NAME = "sample_group_name"
    HEADER_SAMPLE_CREATED_BY_EMAIL = "sample_created_by_email"
    HEADER_SAMPLE_HAS_METADATA = "sample_has_metadata"
    HEADER_SAMPLE_HAS_RUI_INFO = "sample_has_rui_info"
    HEADER_SAMPLE_DIRECT_ANCESTOR_ID = "sample_ancestor_id"
    HEADER_SAMPLE_DIRECT_ANCESTOR_ENTITY_TYPE = "sample_ancestor_entity"
    HEADER_SAMPLE_SENNET_ID = "sample_sennet_id"
    HEADER_SAMPLE_CATEGORY = "sample_category"
    HEADER_SOURCE_UUID = "source_uuid"
    HEADER_SOURCE_SENNET_ID = "source_sennet_id"
    HEADER_SOURCE_HAS_METADATA = "source_has_metadata"
    HEADER_ORGAN_UUID = "organ_uuid"
    HEADER_ORGAN_TYPE = "organ_type"
    HEADER_ORGAN_SENNET_ID = "organ_sennet_id"
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, data_as_val=True, val_key='rui_code').organ_types()

    # Processing and validating query parameters
    accepted_arguments = ['group_uuid']
    param_dict = {}  # currently the only filter is group_uuid, but in case this grows, we're using a dictionary
    if bool(request.args):
        for argument in request.args:
            if argument not in accepted_arguments:
                abort_bad_req(f"{argument} is an unrecognized argument.")
        group_uuid = request.args.get('group_uuid')
        if group_uuid is not None:
            groups_by_id_dict = auth_helper_instance.get_globus_groups_info()['by_id']
            if group_uuid not in groups_by_id_dict:
                abort_bad_req(f"Invalid Group UUID.")
            if not groups_by_id_dict[group_uuid]['data_provider']:
                abort_bad_req(f"Invalid Group UUID. Group must be a data provider")
            param_dict['group_uuid'] = group_uuid

    # Instantiation of the list sample_prov_list
    sample_prov_list = []

    # Call to app_neo4j_queries to prepare and execute database query
    prov_info = app_neo4j_queries.get_sample_prov_info(neo4j_driver_instance, param_dict)

    for sample in prov_info:

        # For cases where there is no sample of type organ above a given sample in the provenance, we check to see if
        # the given sample is itself an organ.
        organ_uuid = None
        organ_type = None
        organ_sennet_id = None
        if sample['organ_uuid'] is not None:
            organ_uuid = sample['organ_uuid']
            for organ_type in ORGAN_TYPES:
                if ORGAN_TYPES[organ_type]['rui_code'] == sample['organ_organ_type']:
                    organ_type = ORGAN_TYPES[organ_type]['term']
                    break
            organ_sennet_id = sample['organ_sennet_id']
        else:
            if sample['sample_sample_category'] == Ontology.ops().specimen_categories().ORGAN:
                organ_uuid = sample['sample_uuid']
                for organ_type in ORGAN_TYPES:
                    if ORGAN_TYPES[organ_type]['rui_code'] == sample['sample_organ']:
                        organ_type = ORGAN_TYPES[organ_type]['term']
                        break
                organ_sennet_id = sample['sample_sennet_id']


        sample_has_metadata = False
        if sample['sample_metadata'] is not None:
            sample_has_metadata = True

        sample_has_rui_info = False
        if sample['sample_rui_info'] is not None:
            sample_has_rui_info = True

        source_has_metadata = False
        if sample['source_metadata'] is not None:
            source_has_metadata = True

        internal_dict = collections.OrderedDict()
        internal_dict[HEADER_SAMPLE_UUID] = sample['sample_uuid']
        internal_dict[HEADER_SAMPLE_LAB_ID] = sample['lab_sample_id']
        internal_dict[HEADER_SAMPLE_GROUP_NAME] = sample['sample_group_name']
        internal_dict[HEADER_SAMPLE_CREATED_BY_EMAIL] = sample['sample_created_by_email']
        internal_dict[HEADER_SAMPLE_HAS_METADATA] = sample_has_metadata
        internal_dict[HEADER_SAMPLE_HAS_RUI_INFO] = sample_has_rui_info
        internal_dict[HEADER_SAMPLE_DIRECT_ANCESTOR_ID] = sample['sample_ancestor_id']
        internal_dict[HEADER_SAMPLE_CATEGORY] = sample['sample_sample_category']
        internal_dict[HEADER_SAMPLE_SENNET_ID] = sample['sample_sennet_id']
        internal_dict[HEADER_SAMPLE_DIRECT_ANCESTOR_ENTITY_TYPE] = sample['sample_ancestor_entity']
        internal_dict[HEADER_SOURCE_UUID] = sample['source_uuid']
        internal_dict[HEADER_SOURCE_HAS_METADATA] = source_has_metadata
        internal_dict[HEADER_SOURCE_SENNET_ID] = sample['source_sennet_id']
        internal_dict[HEADER_ORGAN_UUID] = organ_uuid
        internal_dict[HEADER_ORGAN_TYPE] = organ_type
        internal_dict[HEADER_ORGAN_SENNET_ID] = organ_sennet_id

        # Each sample's dictionary is added to the list to be returned
        sample_prov_list.append(internal_dict)
    return jsonify(sample_prov_list)


"""
Retrieves and validates constraints based on definitions within lib.constraints

Authentication
-------
No token is required

Query Paramters
-------
N/A

Request Body
-------
Requires a json list in the request body matching the following example
Example:
            [{
<required>      "ancestors": {
<required>            "entity_type": "sample",
<optional>            "sub_type": ["organ"],
<optional>            "sub_type_val": ["BD"],
                 },
<required>      "descendants": {
<required>           "entity_type": "sample",
<optional>           "sub_type": ["suspension"]
                 }
             }]
Returns
--------
JSON                   
"""
@app.route('/constraints', methods=['POST'])
def validate_constraints():
    # Always expect a json body
    require_json(request)
    is_match = request.values.get('match')
    order = request.values.get('order')
    use_case = request.values.get('filter')
    report_type = request.values.get('report_type')

    entry_json = request.get_json()
    results = []
    final_result = rest_ok({}, True)

    index = 0
    for constraint in entry_json:
        index += 1
        if order == 'descendants':
            result = get_constraints_by_descendant(constraint, bool(is_match), use_case)
        else:
            result = get_constraints_by_ancestor(constraint, bool(is_match), use_case)

        if result.get('code') is not StatusCodes.OK:
            final_result = rest_bad_req({}, True)

        if report_type == 'ln_err':
            if result.get('code') is not StatusCodes.OK:
                results.append(_ln_err({'msg': result.get('name'), 'data': result.get('description')}, index))
        else:
            results.append(result)

    final_result['description'] = results
    return full_response(final_result)


####################################################################################################
## Internal Functions
####################################################################################################

"""
Parase the token from Authorization header

Parameters
----------
request : falsk.request
    The flask http request object
non_public_access_required : bool
    If a non-public access token is required by the request, default to False

Returns
-------
str
    The token string if valid
"""
def get_user_token(request, non_public_access_required = False):
    # Get user token from Authorization header
    # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
    try:
        user_token = auth_helper_instance.getAuthorizationTokens(request.headers)
    except Exception:
        msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        abort_internal_err(msg)

    # Further check the validity of the token if required non-public access
    if non_public_access_required:
        # When the token is a flask.Response instance,
        # it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed
        if isinstance(user_token, Response):
            # We wrap the message in a json and send back to requester as 401 too
            # The Response.data returns binary string, need to decode
            abort_unauthorized(user_token.get_data().decode())

        # By now the token is already a valid token
        # But we also need to ensure the user belongs to SenNet-Read group
        # in order to access the non-public entity
        # Return a 403 response if the user doesn't belong to SenNet-READ group
        if not user_in_globus_read_group(request):
            abort_forbidden("Access not granted")

    return user_token

"""
Check if the user with token is in the SenNet-READ group

Parameters
----------
request : falsk.request
    The flask http request object that containing the Authorization header
    with a valid Globus groups token for checking group information

Returns
-------
bool
    True if the user belongs to SenNet-READ group, otherwise False
"""
def user_in_globus_read_group(request):
    if 'Authorization' not in request.headers:
        return False

    try:
        user_token = get_user_token(request)
        read_privs = auth_helper_instance.has_read_privs(user_token)
        if isinstance(read_privs, Response):
            msg = read_privs.get_data().decode()
            logger.exception(msg)
            return False

    except Exception as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)

        # If the token is not a groups token, no group information available
        # The commons.sn_auth.AuthCache would return a Response with 500 error message
        # We treat such cases as the user not in the SenNet-READ group
        return False

    return read_privs


"""
Validate the provided token when Authorization header presents

Parameters
----------
request : flask.request object
    The Flask http request object
"""
def validate_token_if_auth_header_exists(request):
    # No matter if token is required or not, when an invalid token provided,
    # we need to tell the client with a 401 error
    # HTTP header names are case-insensitive
    # request.headers.get('Authorization') returns None if the header doesn't exist
    if request.headers.get('Authorization') is not None:
        user_token = get_user_token(request)

        # When the Authoriztion header provided but the user_token is a flask.Response instance,
        # it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed
        if isinstance(user_token, Response):
            # We wrap the message in a json and send back to requester as 401 too
            # The Response.data returns binary string, need to decode
            abort_unauthorized(user_token.get_data().decode())

        # Also check if the parased token is invalid or expired
        # Set the second paremeter as False to skip group check
        user_info = auth_helper_instance.getUserInfo(user_token, False)

        if isinstance(user_info, Response):
            abort_unauthorized(user_info.get_data().decode())


"""
Get the token for internal use only

Returns
-------
str
    The token string 
"""
def get_internal_token():
    return auth_helper_instance.getProcessSecret()

"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
user_token: str
    The user's globus groups token
json_data_dict: dict
    The json request dict from user input

Returns
-------
dict
    A dict of all the newly created entity detials
"""
def create_entity_details(request, normalized_entity_type, user_token, json_data_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Create new ids for the new entity
    try:
        new_ids_dict_list = schema_manager.create_sennet_ids(normalized_entity_type, json_data_dict, user_token, user_info_dict)
        new_ids_dict = new_ids_dict_list[0]
    # When group_uuid is provided by user, it can be invalid
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"

        logger.exception(msg)
        abort_bad_req(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        abort_forbidden(msg)
    except schema_errors.MultipleDataProviderGroupException:
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        abort_bad_req(msg)
    except KeyError as e:
        logger.exception(e)
        abort_bad_req(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new SenNet ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_sennet_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            abort_bad_req(e.response.text)
        if status_code == 404:
            abort_not_found(e.response.text)
        else:
            abort_internal_err(e.response.text)

    # Merge all the above dictionaries and pass to the trigger methods
    new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict}

    try:
        # Use {} since no existing dict
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_type, user_token, {}, new_data_dict)
    # If one of the before_create_trigger methods fails, we can't create the entity
    except schema_errors.BeforeCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
        logger.exception(msg)
        abort_internal_err(msg)
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"

        logger.exception(msg)
        abort_bad_req(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        abort_forbidden(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        abort_bad_req(msg)
    # If something wrong with file upload
    except schema_errors.FileUploadException as e:
        logger.exception(e)
        abort_internal_err(e)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        abort_bad_req(e)
    except Exception as e:
        logger.exception(e)
        abort_internal_err(e)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key
    # in the trigger method, e.g., Source.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values('ENTITIES', merged_dict, normalized_entity_type)

    # Create new entity
    try:
        # Important: `entity_dict` is the resulting neo4j dict, Python list and dicts are stored
        # as string expression literals in it. That's why properties like entity_dict['direct_ancestor_uuid']
        # will need to use ast.literal_eval() in the schema_triggers.py
        entity_dict = app_neo4j_queries.create_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict)
    except TransactionError:
        msg = "Failed to create the new " + normalized_entity_type
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)


    # Important: use `entity_dict` instead of `filtered_merged_dict` to keep consistent with the stored
    # string expression literals of Python list/dict being used with entity update, e.g., `image_files`
    # Important: the same property keys in entity_dict will overwrite the same key in json_data_dict
    # and this is what we wanted. Adding json_data_dict back is to include those `transient` properties
    # provided in the JSON input but not stored in neo4j, and will be needed for after_create_trigger/after_update_trigger,
    # e.g., `previous_revision_uuid`, `direct_ancestor_uuid`
    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**json_data_dict, **entity_dict, **user_info_dict}

    # Note: return merged_final_dict instead of entity_dict because
    # it contains all the user json data that the generated that entity_dict may not have
    return merged_final_dict


"""
Create multiple sample nodes and relationships with the source entity node

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
user_token: str
    The user's globus groups token
json_data_dict: dict
    The json request dict from user input
count : int
    The number of samples to create

Returns
-------
list
    A list of all the newly generated ids via uuid-api
"""
def create_multiple_samples_details(request, normalized_entity_type, user_token, json_data_dict, count):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Create new ids for the new entity
    try:
        new_ids_dict_list = schema_manager.create_sennet_ids(normalized_entity_type, json_data_dict, user_token, user_info_dict, count)
    # When group_uuid is provided by user, it can be invalid
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"

        logger.exception(msg)
        abort_bad_req(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        abort_forbidden(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        abort_bad_req(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        abort_bad_req(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new SenNet ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_sennet_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            abort_bad_req(e.response.text)
        if status_code == 404:
            abort_not_found(e.response.text)
        else:
            abort_internal_err(e.response.text)

    # Use the same json_data_dict and user_info_dict for each sample
    # Only difference is the `uuid` and `sennet_id` that are generated
    # Merge all the dictionaries and pass to the trigger methods
    new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict_list[0]}

    # Instead of calling generate_triggered_data() for each sample, we'll just call it on the first sample
    # since all other samples will share the same generated data except `uuid` and `sennet_id`
    # A bit performance improvement
    try:
        # Use {} since no existing dict
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_type, user_token, {}, new_data_dict)
    # If one of the before_create_trigger methods fails, we can't create the entity
    except schema_errors.BeforeCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
        logger.exception(msg)
        abort_internal_err(msg)
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"

        logger.exception(msg)
        abort_bad_req(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        abort_forbidden(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        abort_bad_req(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        abort_bad_req(e)
    except Exception as e:
        logger.exception(e)
        abort_internal_err(e)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key
    # in the trigger method, e.g., Source.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values('ENTITIES', merged_dict, normalized_entity_type)

    samples_dict_list = []
    for new_ids_dict in new_ids_dict_list:
        # Just overwrite the `uuid` and `sennet_id` that are generated
        # All other generated properties will stay the same across all samples
        sample_dict = {**filtered_merged_dict, **new_ids_dict}
        # Add to the list
        samples_dict_list.append(sample_dict)

    # Generate property values for the only one Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_entity_type, user_token, user_info_dict)

    # Create new sample nodes and needed relationships as well as activity node in one transaction
    try:
        # No return value
        app_neo4j_queries.create_multiple_samples(neo4j_driver_instance, samples_dict_list, activity_data_dict, json_data_dict['direct_ancestor_uuid'][0])
    except TransactionError:
        msg = "Failed to create multiple samples"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)

    # Return the generated ids for UI
    return new_ids_dict_list


"""
Create multiple component datasets from a single Multi-Assay ancestor

Input
-----
json
    A json object with the fields: 
        creation_action
         - type: str
         - description: the action event that will describe the activity node. Allowed valuese are: "Multi-Assay Split"
        group_uuid
         - type: str
         - description: the group uuid for the new component datasets
        direct_ancestor_uuid
         - type: str
         - description: the uuid for the parent multi assay dataset
        datasets
         - type: dict
         - description: the datasets to be created. Only difference between these and normal datasets are the field "dataset_link_abs_dir"

Returns
--------
json array
    List of the newly created datasets represented as dictionaries. 
"""
@app.route('/datasets/components', methods=['POST'])
def multiple_components():
    if READ_ONLY_MODE:
        abort_forbidden("Access not granted when entity-api in READ-ONLY mode")
    # If an invalid token provided, we need to tell the client with a 401 error, rather
    # than a 500 error later if the token is not good.
    validate_token_if_auth_header_exists(request)
    # Get user token from Authorization header
    user_token = get_user_token(request)
    try:
        schema_validators.validate_application_header_before_entity_create("Dataset", request)
    except Exception as e:
        abort_bad_req(str(e))
    require_json(request)

    ######### validate top level properties ########

    # Verify that each required field is in the json_data_dict, and that there are no other fields
    json_data_dict = request.get_json()
    required_fields = ['creation_action', 'group_uuid', 'direct_ancestor_uuids', 'datasets']
    for field in required_fields:
        if field not in json_data_dict:
            raise abort_bad_req(f"Missing required field {field}")
    for field in json_data_dict:
        if field not in required_fields:
            raise abort_bad_req(f"Request body contained unexpected field {field}")

    # validate creation_action
    allowable_creation_actions = ['Multi-Assay Split']
    if json_data_dict.get('creation_action') not in allowable_creation_actions:
        abort_bad_req(f"creation_action {json_data_dict.get('creation_action')} not recognized. Allowed values are: {COMMA_SEPARATOR.join(allowable_creation_actions)}")

    # While we accept a list of direct_ancestor_uuids, we currently only allow a single direct ancestor so verify that there is only 1
    direct_ancestor_uuids = json_data_dict.get('direct_ancestor_uuids')
    if direct_ancestor_uuids is None or not isinstance(direct_ancestor_uuids, list) or len(direct_ancestor_uuids) !=1:
        abort_bad_req(f"Required field 'direct_ancestor_uuids' must be a list. This list may only contain 1 item: a string representing the uuid of the direct ancestor")

    # validate existence of direct ancestors.
    for direct_ancestor_uuid in direct_ancestor_uuids:
        direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
        if direct_ancestor_dict.get('entity_type').lower() != "dataset":
            abort_bad_req(f"Direct ancestor is of type: {direct_ancestor_dict.get('entity_type')}. Must be of type 'dataset'.")

    # validate that there is at least one component dataset
    if len(json_data_dict.get('datasets')) < 1:
        abort_bad_req(f"'datasets' field must contain 2 component datasets.")

    # Validate all datasets using existing schema with triggers and validators
    for dataset in json_data_dict.get('datasets'):
        # dataset_link_abs_dir is not part of the entity creation, will not be stored in neo4j and does not require
        # validation. Remove it here and add it back after validation. We do the same for creating the entities. Doing
        # this makes it easier to keep the dataset_link_abs_dir with the associated dataset instead of adding additional lists and keeping track of which value is tied to which dataset
        dataset_link_abs_dir = dataset.pop('dataset_link_abs_dir', None)
        if not dataset_link_abs_dir:
            abort_bad_req(f"Missing required field in datasets: dataset_link_abs_dir")
        dataset['group_uuid'] = json_data_dict.get('group_uuid')
        dataset['direct_ancestor_uuids'] = direct_ancestor_uuids
        try:
            schema_manager.validate_json_data_against_schema('ENTITIES', dataset, 'Dataset')
        except schema_errors.SchemaValidationException as e:
            # No need to log validation errors
            abort_bad_req(str(e))
        # Execute property level validators defined in the schema yaml before entity property creation
        # Use empty dict {} to indicate there's no existing_data_dict
        try:
            schema_manager.execute_property_level_validators('ENTITIES', 'before_property_create_validators', "Dataset", request, {}, dataset)
        # Currently only ValueError
        except ValueError as e:
            abort_bad_req(e)

        # Add back in dataset_link_abs_dir
        dataset['dataset_link_abs_dir'] = dataset_link_abs_dir

    dataset_list = create_multiple_component_details(request, "Dataset", user_token, json_data_dict.get('datasets'), json_data_dict.get('creation_action'))

    # We wait until after the new datasets are linked to their ancestor before performing the remaining post-creation
    # linkeages. This way, in the event of unforseen errors, we don't have orphaned nodes.
    for dataset in dataset_list:
        schema_triggers.set_status_history('status', 'Dataset', user_token, dataset, {})

    properties_to_skip = [
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'previous_revision_uuids',
        'next_revision_uuids',
        'previous_revision_uuid',
        'next_revision_uuid'
    ]

    if bool(request.args):
        # The parsed query string value is a string 'true'
        return_all_properties = request.args.get('return_all_properties')

        if (return_all_properties is not None) and (return_all_properties.lower() == 'true'):
            properties_to_skip = []

    normalized_complete_entity_list = []
    for dataset in dataset_list:
        # Remove dataset_link_abs_dir once more before entity creation
        dataset_link_abs_dir = dataset.pop('dataset_link_abs_dir', None)
        # Generate the filtered or complete entity dict to send back
        complete_dict = schema_manager.get_complete_entity_result(user_token, dataset, properties_to_skip)

        # Will also filter the result based on schema
        normalized_complete_dict = schema_manager.normalize_object_result_for_response(provenance_type='ENTITIES', entity_dict=complete_dict)


        # Also index the new entity node in elasticsearch via search-api
        logger.log(logging.INFO
                   ,f"Re-indexing for creation of {complete_dict['entity_type']}"
                    f" with UUID {complete_dict['uuid']}")
        reindex_entity(complete_dict['uuid'], user_token)
        # Add back in dataset_link_abs_dir one last time
        normalized_complete_dict['dataset_link_abs_dir'] = dataset_link_abs_dir
        normalized_complete_entity_list.append(normalized_complete_dict)

    return jsonify(normalized_complete_entity_list)



"""
Create multiple dataset nodes and relationships with the source entity node

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus groups token
json_data_dict_list: list
    List of datasets objects as dictionaries
creation_action : str
    The creation action for the new activity node.

Returns
-------
list
    A list of all the newly created datasets with generated fields represented as dictionaries
"""
def create_multiple_component_details(request, normalized_entity_type, user_token, json_data_dict_list, creation_action):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)
    direct_ancestor = json_data_dict_list[0].get('direct_ancestor_uuids')[0]
    # Create new ids for the new entity
    try:
        # we only need the json data from one of the datasets. The info will be the same for both, so we just grab the first in the list
        new_ids_dict_list = schema_manager.create_sennet_ids(normalized_entity_type, json_data_dict_list[0], user_token, user_info_dict, len(json_data_dict_list))
    # When group_uuid is provided by user, it can be invalid
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        abort_bad_req(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new SenNet ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_sennet_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            abort_bad_req(e.response.text)
        if status_code == 404:
            abort_not_found(e.response.text)
        else:
            abort_internal_err(e.response.text)
    datasets_dict_list = []
    for i in range(len(json_data_dict_list)):
        # Remove dataset_link_abs_dir once more before entity creation
        dataset_link_abs_dir = json_data_dict_list[i].pop('dataset_link_abs_dir', None)
        # Combine each id dict into each dataset in json_data_dict_list
        new_data_dict = {**json_data_dict_list[i], **user_info_dict, **new_ids_dict_list[i]}
        try:
            # Use {} since no existing dict
            generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_type, user_token, {}, new_data_dict)
            # If one of the before_create_trigger methods fails, we can't create the entity
        except schema_errors.BeforeCreateTriggerException:
            # Log the full stack trace, prepend a line with our message
            msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
            logger.exception(msg)
            abort_internal_err(msg)
        except schema_errors.NoDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            if 'group_uuid' in json_data_dict_list[i]:
                msg = "Invalid 'group_uuid' value, can't create the entity"
            else:
                msg = "The user does not have the correct Globus group associated with, can't create the entity"

            logger.exception(msg)
            abort_bad_req(msg)
        except schema_errors.UnmatchedDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user does not belong to the given Globus group, can't create the entity"
            logger.exception(msg)
            abort_forbidden(msg)
        except schema_errors.MultipleDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
            logger.exception(msg)
            abort_bad_req(msg)
        except KeyError as e:
            # Log the full stack trace, prepend a line with our message
            logger.exception(e)
            abort_bad_req(e)
        except Exception as e:
            logger.exception(e)
            abort_internal_err(e)
        merged_dict = {**json_data_dict_list[i], **generated_before_create_trigger_data_dict}

        # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
        # and properties with None value
        # Meaning the returned target property key is different from the original key
        # in the trigger method, e.g., Donor.image_files_to_add
        filtered_merged_dict = schema_manager.remove_transient_and_none_values('ENTITIES', merged_dict, normalized_entity_type)
        dataset_dict = {**filtered_merged_dict, **new_ids_dict_list[i]}
        dataset_dict['dataset_link_abs_dir'] = dataset_link_abs_dir
        datasets_dict_list.append(dataset_dict)

    activity_data_dict = schema_manager.generate_activity_data(normalized_entity_type, user_token, user_info_dict, creation_action)
    # activity_data_dict['creation_action'] = creation_action
    try:
        created_datasets = app_neo4j_queries.create_multiple_datasets(neo4j_driver_instance, datasets_dict_list, activity_data_dict, direct_ancestor)
    except TransactionError:
        msg = "Failed to create multiple samples"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)


    return created_datasets


"""
Execute 'after_create_triiger' methods

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
user_token: str
    The user's globus groups token
merged_data_dict: dict
    The merged dict that contains the entity dict newly created and 
    information from user request json that are not stored in Neo4j
"""
def after_create(normalized_entity_type, user_token, merged_data_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} since no new dict
        schema_manager.generate_triggered_data('after_create_trigger', normalized_entity_type, user_token, merged_data_dict, {})
    except schema_errors.AfterCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity has been created, but failed to execute one of the 'after_create_trigger' methods"
        logger.exception(msg)
        abort_internal_err(msg)
    except Exception as e:
        logger.exception(e)
        abort_internal_err(e)


"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
user_token: str
    The user's globus groups token
json_data_dict: dict
    The json request dict
existing_entity_dict: dict
    Dict of the exiting entity information

Returns
-------
dict
    A dict of all the updated entity detials
"""
def update_object_details(provenance_type, request, normalized_entity_type, user_token, json_data_dict, existing_entity_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Merge user_info_dict and the json_data_dict for passing to the trigger methods
    new_data_dict = {**user_info_dict, **json_data_dict}

    try:
        generated_before_update_trigger_data_dict = schema_manager.generate_triggered_data('before_update_trigger', normalized_entity_type, user_token, existing_entity_dict, new_data_dict)
    # If something wrong with file upload
    except schema_errors.FileUploadException as e:
        logger.exception(e)
        abort_internal_err(e)
    # If one of the before_update_trigger methods fails, we can't update the entity
    except schema_errors.BeforeUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_update_trigger' methods, can't update the entity"
        logger.exception(msg)
        abort_internal_err(msg)
    except Exception as e:
        logger.exception(e)
        abort_internal_err(e)

    # Merge dictionaries
    merged_dict = {**json_data_dict, **generated_before_update_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key 
    # in the trigger method, e.g., Source.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values(provenance_type, merged_dict, normalized_entity_type)

    # By now the filtered_merged_dict contains all user updates and all triggered data to be added to the entity node
    # Any properties in filtered_merged_dict that are not on the node will be added.
    # Any properties not in filtered_merged_dict that are on the node will be left as is.
    # Any properties that are in both filtered_merged_dict and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.

    # Update the exisiting entity
    try:
        updated_entity_dict = app_neo4j_queries.update_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict, existing_entity_dict['uuid'])
    except TransactionError:
        msg = "Failed to update the entity with id " + id
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)

    # Important: use `updated_entity_dict` instead of `filtered_merged_dict` to keep consistent with the stored
    # string expression literals of Python list/dict being used with entity update, e.g., `image_files`
    # Important: the same property keys in entity_dict will overwrite the same key in json_data_dict
    # and this is what we wanted. Adding json_data_dict back is to include those `transient` properties
    # provided in the JSON input but not stored in neo4j, and will be needed for after_create_trigger/after_update_trigger,
    # e.g., `previous_revision_uuid`, `direct_ancestor_uuid`
    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**json_data_dict, **updated_entity_dict, **user_info_dict}

    # Use merged_final_dict instead of merged_dict because
    # merged_dict only contains properties to be updated, not all properties
    return merged_final_dict

"""
Execute 'after_update_triiger' methods

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Source
user_token: str
    The user's globus groups token
entity_dict: dict
    The entity dict newly updated
"""
def after_update(normalized_entity_type, user_token, entity_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} sicne no new dict
        schema_manager.generate_triggered_data('after_update_trigger', normalized_entity_type, user_token, entity_dict, {})
    except schema_errors.AfterUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity information has been updated, but failed to execute one of the 'after_update_trigger' methods"
        logger.exception(msg)
        abort_internal_err(msg)
    except Exception as e:
        logger.exception(e)
        abort_internal_err(e)


"""
Get target entity dict

Parameters
----------
id : str
    The uuid or sennet_id of target activity
user_token: str
    The user's globus groups token from the incoming request

Returns
-------
dict
    A dictionary of activity details returned from neo4j
"""
def query_target_activity(id, user_token):
    try:
        """
        The dict returned by uuid-api that contains all the associated ids, e.g.:
        {
            "ancestor_id": "23c0ffa90648358e06b7ac0c5673ccd2",
            "ancestor_ids":[
                "23c0ffa90648358e06b7ac0c5673ccd2"
            ],
            "email": "marda@ufl.edu",
            "uuid": "1785aae4f0fb8f13a56d79957d1cbedf",
            "sennet_id": "SNT966.VNKN.965",
            "time_generated": "2020-10-19 15:52:02",
            "type": "SOURCE",
            "user_id": "694c6f6a-1deb-41a6-880f-d1ad8af3705f"
        }
        """
        sennet_ids = schema_manager.get_sennet_ids(id)

        # Get the target uuid if all good
        uuid = sennet_ids['uuid']
        entity_dict = app_neo4j_queries.get_activity(neo4j_driver_instance, uuid)

        # The uuid exists via uuid-api doesn't mean it's also in Neo4j
        if not entity_dict:
            abort_not_found(f"Activity of id: {id} not found in Neo4j")

        return entity_dict
    except requests.exceptions.RequestException as e:
        # Due to the use of response.raise_for_status() in schema_manager.get_sennet_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            abort_bad_req(e.response.text)
        if status_code == 404:
            abort_not_found(e.response.text)
        else:
            abort_internal_err(e.response.text)


"""
Get target entity dict

Parameters
----------
id : str
    The uuid or sennet_id of target entity
user_token: str
    The user's globus nexus token from the incoming request

Returns
-------
dict
    A dictionary of entity details returned from neo4j
"""


def query_target_entity(id, user_token):
    entity_dict = None
    current_datetime = datetime.now()

    # Use the cached data if found and still valid
    # Otherwise, make a fresh query and add to cache
    if entity_dict is None:
        try:
            """
            The dict returned by uuid-api that contains all the associated ids, e.g.:
            {
                "ancestor_id": "940f409ea5b96ff8d98a87d185cc28e2",
                "ancestor_ids": [
                    "940f409ea5b96ff8d98a87d185cc28e2"
                ],
                "email": "jamie.l.allen@vanderbilt.edu",
                "sn_uuid": "be5a8f1654364c9ea0ca3071ba48f260",
                "sennet_id": "SN272.FXQF.697",
                "submission_id": "VAN0032-RK-2-43",
                "time_generated": "2020-11-09 19:55:09",
                "type": "SAMPLE",
                "user_id": "83ae233d-6d1d-40eb-baa7-b6f636ab579a"
            }
            """
            # Get cached ids if exist otherwise retrieve from UUID-API
            sennet_ids = schema_manager.get_sennet_ids(id)

            # Get the target uuid if all good
            uuid = sennet_ids['uuid']
            entity_dict = app_neo4j_queries.get_entity(neo4j_driver_instance, uuid)

            # The uuid exists via uuid-api doesn't mean it's also in Neo4j
            if not entity_dict:
                abort_not_found(f"Entity of id: {id} not found in Neo4j")

        except requests.exceptions.RequestException as e:
            # Due to the use of response.raise_for_status() in schema_manager.get_sennet_ids()
            # we can access the status codes from the exception
            status_code = e.response.status_code

            if status_code == 400:
                abort_bad_req(e.response.text)
            if status_code == 404:
                abort_not_found(e.response.text)
            else:
                abort_internal_err(e.response.text)
    else:
        logger.info(f'Using the cache data of entity {id} at time {current_datetime}')

    # Final return
    return entity_dict

"""
Get target entity dict

Parameters
----------
id : str
    The uuid or sennet_id of target entity
user_token: str
    The user's globus nexus token from the incoming request

Returns
-------
dict
    A dictionary of activity details returned from neo4j
"""
def query_activity_was_generated_by(id, user_token):
    try:
        sennet_ids = schema_manager.get_sennet_ids(id)

        # Get the target uuid if all good
        uuid = sennet_ids['uuid']
        activity_dict = app_neo4j_queries.get_activity_was_generated_by(neo4j_driver_instance, uuid)

        # The uuid exists via uuid-api doesn't mean it's also in Neo4j
        if not activity_dict:
            abort_not_found(f"Activity connected to id: {id} not found in Neo4j")

        return activity_dict
    except requests.exceptions.RequestException as e:
        # Due to the use of response.raise_for_status() in schema_manager.get_sennet_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            abort_bad_req(e.response.text)
        if status_code == 404:
            abort_not_found(e.response.text)
        else:
            abort_internal_err(e.response.text)

"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def require_json(request):
    if not request.is_json:
        abort_bad_req("A json body and appropriate Content-Type header are required")


"""
Make a call to search-api to reindex this entity node in elasticsearch

Parameters
----------
uuid : str
    The uuid of the target entity
user_token: str
    The user's globus groups token
"""
def reindex_entity(uuid, user_token):
    try:
        logger.info(f"Making a call to search-api to reindex uuid: {uuid}")

        headers = create_request_headers(user_token)

        response = requests.put(app.config['SEARCH_API_URL'] + "/reindex/" + uuid, headers = headers)
        # The reindex takes time, so 202 Accepted response status code indicates that
        # the request has been accepted for processing, but the processing has not been completed
        if response.status_code == 202:
            logger.info(f"The search-api has accepted the reindex request for uuid: {uuid}")
        else:
            logger.error(f"The search-api failed to initialize the reindex for uuid: {uuid}")
    except Exception:
        msg = f"Failed to send the reindex request to search-api for entity with uuid: {uuid}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)

"""
Create a dict of HTTP Authorization header with Bearer token for making calls to uuid-api

Parameters
----------
user_token: str
    The user's globus groups token

Returns
-------
dict
    The headers dict to be used by requests
"""
def create_request_headers(user_token):
    auth_header_name = 'Authorization'
    auth_scheme = 'Bearer'

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme + ' ' + user_token
    }

    return headers_dict

"""
Ensure the access level dir with leading and trailing slashes

Parameters
----------
dir_name : str
    The name of the sub directory corresponding to each access level

Returns
-------
str 
    One of the formatted dir path string: /public/, /protected/, /consortium/
"""
def access_level_prefix_dir(dir_name):
    if string_helper.isBlank(dir_name):
        return ''

    return hm_file_helper.ensureTrailingSlashURL(hm_file_helper.ensureBeginningSlashURL(dir_name))


"""
Check if a user has valid access to update a given entity

Parameters
----------
entity : str
    The entity that is attempting to be updated
user_token : str 
    The token passed in via the request header that will be used to authenticate

"""


def validate_user_update_privilege(entity, user_token):
    # A user has update privileges if they are a data admin or are in the same group that registered the entity
    is_admin = auth_helper_instance.has_data_admin_privs(user_token)
    if isinstance(is_admin, Response):
        abort(is_admin)

    user_write_groups: List[dict] = auth_helper_instance.get_user_write_groups(user_token)
    if isinstance(user_write_groups, Response):
        abort(user_write_groups)

    user_group_uuids = [d['uuid'] for d in user_write_groups]
    if entity['group_uuid'] not in user_group_uuids and is_admin is False:
        abort_forbidden(f"User does not have write privileges for this entity. "
                        f"Reach out to the help desk to request access to group: {entity['group_uuid']}.")


"""
Formats error into dict

error : str
    the detail of the error
    
row : int
    the row number where the error occurred
    
column : str
    the column in the csv/tsv where the error occurred

Returns
-------
 dict 
"""
def _ln_err(error, row: int = None, column: str = None):
    return {
        'column': column,
        'error': error,
        'row': row
    }

"""
Ensures that two given entity dicts as ancestor and descendant pass constraint validation.

ancestor: dict
descendant: dict
descendant_entity_type: str (dicts sometimes do not include immutable keys like entity_type, pass it here.)

Returns constraint test full matches if successful. Raises abort_bad_req if failed.
"""
def validate_constraints_by_entities(ancestor, descendant, descendant_entity_type=None):

    def get_sub_type(obj):
        sub_type = obj.get('sample_category') if obj.get('sample_category') is not None else obj.get('source_type')
        try:
            sub_type = [obj.get('dataset_type')] if sub_type is None else [sub_type]
            if type(sub_type) is not list:
                sub_type = ast.literal_eval(sub_type)
        except Exception as ec:
            logger.error(str(ec))
        return sub_type

    def get_sub_type_val(obj):
        sub_type_val = obj.get('organ')
        return [sub_type_val] if sub_type_val is not None else None

    def get_entity_type(obj, default_type):
        return obj.get('entity_type') if obj.get('entity_type') is not None else default_type

    constraint = build_constraint(
        build_constraint_unit(ancestor.get('entity_type'),
                              sub_type=get_sub_type(ancestor), sub_type_val=get_sub_type_val(ancestor)),
        [build_constraint_unit(get_entity_type(descendant, descendant_entity_type),
                               sub_type=get_sub_type(descendant), sub_type_val=get_sub_type_val(descendant))]
    )

    result = get_constraints_by_ancestor(constraint, True)
    if result.get('code') is not StatusCodes.OK:
        abort_bad_req(f"Invalid entity constraints for ancestor of type {ancestor.get('entity_type')}. Valid descendants include: {result.get('description')}")
    return result

"""
Ensures that a given organ code matches what is found on the organ_types yaml document

organ_code : str

Returns nothing. Raises abort_bad_req is organ code not found on organ_types.yaml 
"""


def validate_organ_code(organ_code):
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, data_as_val=True, val_key='rui_code').organ_types()

    for organ_type in ORGAN_TYPES:
        if equals(ORGAN_TYPES[organ_type]['rui_code'], organ_code):
            return

    abort_bad_req(f"Invalid Organ. Organ must be 2 digit, case-insensitive code")


def verify_ubkg_properties(json_data_dict):
    SOURCE_TYPES = Ontology.ops(as_data_dict=True).source_types()
    SAMPLE_CATEGORIES = Ontology.ops(as_data_dict=True).specimen_categories()
    ORGAN_TYPES = Ontology.ops(as_data_dict=True, key='rui_code').organ_types()
    DATASET_TYPE = Ontology.ops(as_data_dict=True).dataset_types()

    if 'source_type' in json_data_dict:
        compare_property_against_ubkg(SOURCE_TYPES, json_data_dict, 'source_type')

    if 'sample_category' in json_data_dict:
        compare_property_against_ubkg(SAMPLE_CATEGORIES, json_data_dict, 'sample_category')

    if 'organ' in json_data_dict:
        compare_property_against_ubkg(ORGAN_TYPES, json_data_dict, 'organ')

    # If the proposed Dataset dataset_type ends with something in square brackets, anything inside
    # those square brackets are acceptable at the end of the string.  Simply validate the start.
    if 'dataset_type' in json_data_dict:
        dataset_type_dict = {'dataset_type': re.sub(pattern='(\S)\s\[.*\]$', repl=r'\1',
                                                    string=json_data_dict['dataset_type'])}
        compare_property_against_ubkg(DATASET_TYPE, dataset_type_dict, 'dataset_type')


def compare_property_list_against_ubkg(ubkg_dict, json_data_dict, field):
    good_fields = []
    passes_ubkg_validation = True
    for ubkg_field in ubkg_dict:
        for item in json_data_dict[field]:
            if equals(item, ubkg_dict[ubkg_field]):
                good_fields.append(ubkg_dict[ubkg_field])

    if len(good_fields) != len(json_data_dict[field]):
        match_note = f"Mathing include: {', '.join(good_fields)}. " if len(good_fields) > 0 else ''
        ubkg_validation_message = f"Some or all values in '{field}' does not match any allowable property. " \
                                  f"{match_note}" \
                                  "Please check proper spelling."
        abort_unacceptable(ubkg_validation_message)

    json_data_dict[field] = good_fields


def compare_property_against_ubkg(ubkg_dict, json_data_dict, field):
    passes_ubkg_validation = False

    for ubkg_field in ubkg_dict:
        if equals(json_data_dict[field], ubkg_dict[ubkg_field]):
            json_data_dict[field] = ubkg_dict[ubkg_field]
            passes_ubkg_validation = True
            break

    if not passes_ubkg_validation:
        ubkg_validation_message = f"Value listed in '{field}' does not match any allowable property. " \
                                  "Please check proper spelling."
        abort_unacceptable(ubkg_validation_message)


def check_multiple_organs_constraint(current_entity: dict, ancestor_entity: dict, case_uuid: str = None):
    """
    Validates that the Organ of the Sample (to be POST or PUT) does not violate allowable multiple organs constraints.

    Parameters
    ----------
    current_entity: the Sample entity to validate
    ancestor_entity: the ancestor Source
    case_uuid: an uuid to exclude from the count on check of ancestor given the organ
    :return:
    """
    if equals(ancestor_entity['entity_type'], Ontology.ops().entities().SOURCE):
        if equals(current_entity['sample_category'], Ontology.ops().specimen_categories().ORGAN):
            organ_code = current_entity['organ']
            if organ_code not in app.config['MULTIPLE_ALLOWED_ORGANS']:
                count = app_neo4j_queries.get_source_organ_count(neo4j_driver_instance, ancestor_entity['uuid'],
                                                                 organ_code, case_uuid=case_uuid)
                if count >= 1:
                    organ_codes = Ontology.ops(as_data_dict=True, val_key='term', key='rui_code').organ_types()
                    organ = organ_codes[organ_code]
                    abort_bad_req(f"Cannot add another organ of type {organ} ({organ_code}) to Source {ancestor_entity['sennet_id']}. "
                                  f"A {organ} Sample exists already on this Source.")


"""
Validates the given metadata via the pathname returned by the Ingest API

pathname : str

Returns Boolean whether validation was passed or not. 
"""


def validate_metadata(data, user_token):
    try:
        logger.info(f"Making a call to ingest-api to validate metadata")

        headers = create_request_headers(user_token)

        response = requests.post(app.config['INGEST_API_URL'] + "/metadata/validate", headers=headers, data=data)
        if response.status_code == 200:
            # compare the two metadata
            json_data_dict = request.get_json()
            request_metadata = json_data_dict['metadata']

            response_dict = response.json()
            response_metadata = response_dict['metadata'][0]

            # Delete these because they would have been appended during the Portal-UI processe ...
            # So remove before comparing.
            del request_metadata['pathname']
            if request_metadata.get('file_row') is not None:
                del request_metadata['file_row']

            return request_metadata.items() == response_metadata.items()

        else:
            logger.error(response.text)

    except Exception:
        msg = f"Failed to send the validate metadata request to ingest-api"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        abort_internal_err(msg)

    return False


"""
Delete the cached data of all possible keys used for the given entity id

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity (Donor/Dataset/Sample/Upload/Collection/Publication)
"""
def delete_cache(id):
    if MEMCACHED_MODE:
        # First delete the target entity cache
        entity_dict = query_target_entity(id, get_internal_token())
        entity_uuid = entity_dict['uuid']

        # If the target entity is Sample (`direct_ancestor`) or Dataset/Publication (`direct_ancestors`)
        # Delete the cache of all the direct descendants (children)
        child_uuids = schema_neo4j_queries.get_children(neo4j_driver_instance, entity_uuid , 'uuid')

        # If the target entity is Collection, delete the cache for each of its associated
        # Datasets and Publications (via [:IN_COLLECTION] relationship) as well as just Publications (via [:USES_DATA] relationship)
        collection_dataset_uuids = schema_neo4j_queries.get_collection_associated_datasets(neo4j_driver_instance, entity_uuid , 'uuid')

        # If the target entity is Upload, delete the cache for each of its associated Datasets (via [:IN_UPLOAD] relationship)
        upload_dataset_uuids = schema_neo4j_queries.get_upload_datasets(neo4j_driver_instance, entity_uuid , 'uuid')

        # If the target entity is Datasets/Publication, delete the associated Collections cache, Upload cache
        collection_uuids = schema_neo4j_queries.get_dataset_collections(neo4j_driver_instance, entity_uuid , 'uuid')
        collection_dict = schema_neo4j_queries.get_publication_associated_collection(neo4j_driver_instance, entity_uuid)
        upload_dict = schema_neo4j_queries.get_dataset_upload(neo4j_driver_instance, entity_uuid)

        # We only use uuid in the cache key acorss all the cache types
        uuids_list = [entity_uuid] + child_uuids + collection_dataset_uuids + upload_dataset_uuids + collection_uuids

        # It's possible no linked collection or upload
        if collection_dict:
            uuids_list.append(collection_dict['uuid'])

        if upload_dict:
            uuids_list.append(upload_dict['uuid'])

        schema_manager.delete_memcached_cache(uuids_list)

####################################################################################################
## For local development/testing
####################################################################################################

if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5002")
        print(f"Flask app.run() done")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")
    except SystemExit as se:
        logger.exception(se,stack_info=True)
        print(f"SystemExit exception with code {se.code}.")

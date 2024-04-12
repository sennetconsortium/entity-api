from enum import Enum


class SchemaConstants(object):
    # Expire the request cache after the time-to-live (seconds), default 4 hours
    REQUEST_CACHE_TTL = 14400
    MEMCACHED_TTL = 7200

    # Constants used by validators
    INGEST_API_APP = 'ingest-api'
    COMPONENT_DATASET = 'component-dataset'
    INGEST_PIPELINE_APP = 'ingest-pipeline'
    INGEST_PORTAL_APP = 'portal-ui'
    # HTTP header names are case-insensitive
    SENNET_APP_HEADER = 'X-SenNet-Application'
    INTERNAL_TRIGGER = 'X-Internal-Trigger'
    DATASET_STATUS_PUBLISHED = 'published'

    # Used by triggers, all lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    DOI_BASE_URL = 'https://doi.org/'

    ALLOWED_SINGLE_CREATION_ACTIONS = ['central process', 'lab process']
    ALLOWED_MULTI_CREATION_ACTIONS = ['multi-assay split']

    ALLOWED_DATASET_STATUSES = ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid', 'submitted', 'incomplete']
    ALLOWED_UPLOAD_STATUSES = ['new', 'valid', 'invalid', 'error', 'reorganized', 'processing', 'submitted', 'incomplete']


# Define an enumeration to classify an entity's visibility, which can be combined with
# authorization info when verify operations on a request.
class DataVisibilityEnum(Enum):
    PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
    # Since initial release just requires public/non-public, add
    # another entry indicating non-public.
    NONPUBLIC = 'nonpublic'

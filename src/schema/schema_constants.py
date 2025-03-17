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

    ALLOWED_SINGLE_CREATION_ACTIONS = ['central process', 'lab process', 'external process']
    ALLOWED_MULTI_CREATION_ACTIONS = ['multi-assay split']

    ALLOWED_DATASET_STATUSES = ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid', 'submitted', 'incomplete']
    ALLOWED_UPLOAD_STATUSES = ['new', 'valid', 'invalid', 'error', 'reorganized', 'processing', 'submitted', 'incomplete']

    # Used to validate the X-SenNet-Application header
    ALLOWED_APPLICATIONS = [INGEST_API_APP, INGEST_PIPELINE_APP, INGEST_PORTAL_APP]


# Define an enumeration to classify an entity's visibility, which can be combined with
# authorization info when verify operations on a request.
class DataVisibilityEnum(Enum):
    PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
    # Since initial release just requires public/non-public, add
    # another entry indicating non-public.
    NONPUBLIC = 'nonpublic'


# Define an enumeration to classify metadata scope which can be returned.
class MetadataScopeEnum(Enum):
    # Legacy notion of complete metadata for an entity includes generated
    # data populated by triggers.
    COMPLETE = 'complete_metadata'
    # Index metadata is for storage in Open Search documents, and should not
    # include data which must be generated and then removed, nor any data which
    # is not stored in an index document.
    INDEX = 'index_metadata'


# Define an enumeration of accepted trigger types.
class TriggerTypeEnum(Enum):
    ON_READ = 'on_read_trigger'
    ON_INDEX = 'on_index_trigger'
    BEFORE_CREATE = 'before_create_trigger'
    BEFORE_UPDATE = 'before_update_trigger'
    AFTER_CREATE = 'after_create_trigger'
    AFTER_UPDATE = 'after_update_trigger'
    ON_BULK_READ = 'on_bulk_read_trigger'

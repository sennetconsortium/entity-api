class SchemaConstants(object):
    # Expire the request cache after the time-to-live (seconds), default 4 hours
    REQUEST_CACHE_TTL = 14400

    # Constants used by validators
    INGEST_API_APP = 'ingest-api'
    # HTTP header names are case-insensitive
    HUBMAP_APP_HEADER = 'X-SenNet-Application'
    DATASET_STATUS_PUBLISHED = 'published'

    # Used by triggers, all lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    # Yaml file to parse organ description
    ORGAN_TYPES_YAML = 'https://raw.githubusercontent.com/sennetconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml'
    ASSAY_TYPES_YAML = 'https://raw.githubusercontent.com/sennetconsortium/search-api/master/src/search-schema/data/definitions/enums/assay_types.yaml'

    # For generating Sample.tissue_type
    TISSUE_TYPES_YAML = 'https://raw.githubusercontent.com/sennetconsortium/search-api/master/src/search-schema/data/definitions/enums/tissue_sample_types.yaml'
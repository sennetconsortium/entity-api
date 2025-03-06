import copy
import logging
from atlas_consortia_commons.string import equals

from lib.ontology import Ontology
from schema import schema_manager, schema_neo4j_queries
from schema.schema_triggers import get_organ_hierarchy

logger = logging.getLogger(__name__)

class BulkTriggersManager:
    def __init__(self):
        self.groups = {}
        self.references = {}
        self.list_indexes = {}

    def get_group_by_key(self, key):
        return self.groups[key]

    def set_item_to_group_by_key(self, key, item):
        if key not in self.groups:
            self.groups[key] = set()
        self.groups[key].add(item)

    def set_reference(self, key, item):
        self.references[key] = item

    def build_lists_index_references(self, list = []):
        i = 0
        for r in list:
            self.list_indexes[r['uuid']] = i
            i = i + 1

        return self.list_indexes

    def get_trigger_method_name(self, key):
        bulk_meta_references = self.references[key]
        return bulk_meta_references[1]

    def get_property_key(self, key):
        bulk_meta_references = self.references[key]
        return bulk_meta_references[0]






def get_bulk_origin_samples(user_token, bulk_trigger_manager:BulkTriggersManager, storage_key, entities_list):
    """Trigger event method to grab the ancestor of entities where entity type is Sample and the sample_category is Organ.

    Parameters
    ----------
    user_token: str
        The user's globus nexus token
    storage_key : str
        The reference key for finding what needs handling for bulk
    entities_list : list
        List of entities to be bulk processed

    Returns
    -------
    list
        The processed list of entities
    """
    # The origin_sample is the sample that `sample_category` is "organ" and the `organ` code is set at the same time


    property_key = bulk_trigger_manager.get_property_key(storage_key)
    curr_uuid = None # tracking for error handling
    try:

        def _get_organ_hierarchy(entity_dict):
            organ_hierarchy_key, organ_hierarchy_value = get_organ_hierarchy(property_key='organ_hierarchy',
                                                                             normalized_type=Ontology.ops().entities().SAMPLE,
                                                                             user_token=user_token,
                                                                             existing_data_dict=entity_dict,
                                                                             new_data_dict={})
            entity_dict[organ_hierarchy_key] = organ_hierarchy_value

        uuids = []
        for uuid in bulk_trigger_manager.get_group_by_key(storage_key):
            curr_uuid = uuid
            if curr_uuid in bulk_trigger_manager.list_indexes:
                index = bulk_trigger_manager.list_indexes[uuid]
                existing_data_dict = entities_list[index]
                # handle the ones that are sample_category of Organs, they are the origin_samples of themselves
                if equals(existing_data_dict.get("sample_category"), Ontology.ops().specimen_categories().ORGAN):
                    _get_organ_hierarchy(existing_data_dict)
                    existing_data_dict[property_key] = [copy.deepcopy(existing_data_dict)]
                # otherwise store for later cypher query
                elif existing_data_dict['entity_type'] in ["Sample", "Dataset", "Publication"]:
                    uuids.append(existing_data_dict['uuid'])
            else:
                uuids.append(curr_uuid)

        if len(uuids) > 0:
            # let's do a query for the rest that were not Samples or sample_category of Organs
            origin_samples_results = schema_neo4j_queries.get_origin_samples(schema_manager.get_neo4j_driver_instance(),
                                                                             uuids)
            for r in origin_samples_results:
                curr_uuid = r['uuid']
                if curr_uuid in bulk_trigger_manager.list_indexes:
                    for origin_sample in r['result']:
                        _get_organ_hierarchy(origin_sample)

                    index = bulk_trigger_manager.list_indexes[curr_uuid]
                    entities_list[index][property_key] = r['result']


    except Exception as e:
        logger.error(f"No origin sample found for an entity with UUID {curr_uuid} / {storage_key}")

    return entities_list
import copy
import logging
from atlas_consortia_commons.string import equals

from lib.ontology import Ontology
from schema import schema_manager, schema_neo4j_queries
from schema.schema_triggers import get_organ_hierarchy

logger = logging.getLogger(__name__)


class BulkTriggersManager:
    def __init__(self):
        self.groups = (
            {}
        )  # the keys of which are in the form property_key_trigger_name. Value is a python set of uuids requiring the same trigger
        self.references = (
            {}
        )  # the keys of which are in the form property_key_trigger_name. Value is a list with [property_key, trigger_name]
        self.list_indexes = (
            {}
        )  # the keys of which are entity uuids, value is the index that this entity is at in entities_list of the bulk trigger. Important for constant time finding of entities

    def get_group_by_key(self, key: str):
        return self.groups[key]

    def set_item_to_group_by_key(self, key: str, item: str):
        if key not in self.groups:
            self.groups[key] = set()
        self.groups[key].add(item)

    def set_reference(self, key, item):
        self.references[key] = item

    def build_lists_index_references(self, entities_list: list = []):
        i = 0
        for r in entities_list:
            self.list_indexes[r["uuid"]] = i
            i = i + 1

        return self.list_indexes

    def _get_from_references(self, key: str, index: int):
        bulk_meta_references = self.references.get(key, [])
        return bulk_meta_references[index] if len(bulk_meta_references) > index else None

    def get_trigger_method_name(self, key: str) -> str:
        return self._get_from_references(key, 1)

    def get_property_key(self, key: str) -> str:
        return self._get_from_references(key, 0)


def get_bulk_origin_samples(
    user_token, bulk_trigger_manager: BulkTriggersManager, storage_key, entities_list
):
    """Trigger event method to grab the ancestor of entities where entity type is Sample and the sample_category is Organ.

    Parameters
    ----------
    user_token: str
        The user's globus nexus token
    bulk_trigger_manager : BulkTriggersManager
        Instance of helper class for managing bulk triggers
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
    curr_uuid = None  # tracking for error handling
    try:

        def _get_organ_hierarchy(entity_dict):
            organ_hierarchy_key, organ_hierarchy_value = get_organ_hierarchy(
                property_key="organ_hierarchy",
                normalized_type=Ontology.ops().entities().SAMPLE,
                user_token=user_token,
                existing_data_dict=entity_dict,
                new_data_dict={},
            )
            entity_dict[organ_hierarchy_key] = organ_hierarchy_value
            return entity_dict

        uuids = []
        for uuid in bulk_trigger_manager.get_group_by_key(storage_key):
            curr_uuid = uuid
            if curr_uuid in bulk_trigger_manager.list_indexes:
                index = bulk_trigger_manager.list_indexes[uuid]
                existing_data_dict = entities_list[index]
                # handle the ones that are sample_category of Organs, they are the origin_samples of themselves
                if equals(
                    existing_data_dict.get("sample_category"),
                    Ontology.ops().specimen_categories().ORGAN,
                ):
                    entities_list[index][property_key] = [
                        _get_organ_hierarchy(copy.deepcopy(existing_data_dict))
                    ]
                # otherwise store for later cypher query
                elif existing_data_dict["entity_type"] in ["Sample", "Dataset", "Publication"]:
                    uuids.append(existing_data_dict["uuid"])
            else:
                uuids.append(curr_uuid)

        if len(uuids) > 0:
            # let's do a query for the rest that were not Samples or sample_category of Organs
            origin_samples_results = schema_neo4j_queries.get_origin_samples(
                schema_manager.get_neo4j_driver_instance(), uuids
            )
            for r in origin_samples_results:
                curr_uuid = r["uuid"]
                if curr_uuid in bulk_trigger_manager.list_indexes:
                    for origin_sample in r["result"]:
                        _get_organ_hierarchy(origin_sample)

                    index = bulk_trigger_manager.list_indexes[curr_uuid]
                    entities_list[index][property_key] = r["result"]

    except Exception as e:
        logger.error(f"No origin sample found for an entity with UUID {curr_uuid} / {storage_key}")

    return entities_list

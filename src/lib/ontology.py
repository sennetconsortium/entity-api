from __future__ import annotations

from atlas_consortia_commons.ubkg.ubkg_sdk import UbkgSDK
from flask import current_app


# Custom accessors etc. can be added to the Ontology class
class Ontology(UbkgSDK):
    @staticmethod
    def modify_entities_cache():
        cache = current_app.ubkg.get_cache()
        entities = current_app.ubkg.entities
        key = f"VALUESET_{entities}"
        if key in cache:
            for e in cache[key]:
                if e["term"] == "Publication Entity":
                    e["term"] = "Publication"

    @classmethod
    def organs_by_organ_uberon(cls: Ontology) -> dict:
        return cls.ops(
            as_data_dict=True, prop_callback=None, data_as_val=True, key="organ_uberon"
        ).organ_types()

    
    @classmethod
    def dataset_types_hierarchy(cls: Ontology, dataset_type: str = None) -> dict:
        def prop_callback(dict):
            return dict['dataset_type']['name']
        
        def val_callback(dict):
            if 'modalities' not in dict:
                return []
       
            list_of_facets = []
            for modality in dict['modalities']:
                for analyte in modality['analytes']:
                    list_of_facets.append({
                        "modality": modality['name'],
                        "analyte": analyte['name'],
                        "dataset_type": dict['dataset_type']['name']
                    })
            return list_of_facets
        
        all_facets = cls.ops(
            as_data_dict=True, prop_callback=prop_callback, val_callback=val_callback, data_as_val=False, key="dataset_types", val_key="dataset_types"
        ).transform_ontology(current_app.ubkg.dataset_types_hierarchy, 'DatasetTypesHierarchy')

        if dataset_type is not None and dataset_type in all_facets:
            matrix = all_facets[dataset_type]
        else:
            matrix = list(all_facets.values())
        
        return [item for sublist in matrix for item in sublist]
    
    @classmethod
    def dataset_type_hierarchy(cls: Ontology) -> dict:
        def prop_callback(dict):
            return dict['dataset_type']['name']
        
        def val_callback(dict):
            modalities = []
            if 'modalities' in dict:
                for modality in dict['modalities']:
                    modalities.append(modality['name'])
            return modalities
        
        return cls.ops(
            as_data_dict=True, prop_callback=prop_callback, val_callback=val_callback, data_as_val=False, key="dataset_type", val_key="dataset_modalities"
        ).transform_ontology(current_app.ubkg.dataset_types_hierarchy, 'DatasetTypesHierarchy')

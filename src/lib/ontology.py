from atlas_consortia_commons.ubkg.ubkg_sdk import UbkgSDK
from flask import current_app


def enum_val_lower(val):
    return val.value.lower()


class Ontology(UbkgSDK):
    @staticmethod
    def assay_types_ext():
        Ontology.ops().Ops.key = 'data_type'
        Ontology.ops().Ops.url_params = '&dataset_provider=external'
        return Ontology.ops().transform_ontology(current_app.ubkg.assay_types, 'AssayTypesExt')

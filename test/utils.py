from dataclasses import dataclass, fields

from lib.ontology import enum_val_lower


@dataclass
class SpecimenCategories:
    BLOCK: str = 'Block'
    ORGAN: str = 'Organ'
    SECTION: str = 'Section'
    SUSPENSION: str = 'Suspension'


@dataclass
class Entities:
    DATASET: str = 'Dataset'
    PUBLICATION_ENTITY: str = 'Publication Entity'
    SAMPLE: str = 'Sample'
    SOURCE: str = 'Source'


def entities(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(Entities)]
    if as_arr and cb == str:
        return [e.default for e in fields(Entities)]
    if as_data_dict:
        return {e.name: e.default for e in fields(Entities)}
    return Entities
    
def specimen_categories(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(SpecimenCategories)]
    if as_arr and cb == str:
        return [e.default for e in fields(SpecimenCategories)]
    if as_data_dict:
        return {e.name: e.default for e in fields(SpecimenCategories)}
    return SpecimenCategories

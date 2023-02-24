from lib.ontology import get_entities, Entities, SpecimenCategory, Organs, DataTypes


def build_constraint(ancestor: dict, descendants: list[dict]) -> dict:
    return {
        'ancestors': ancestor,
        'descendants': descendants
    }


def build_constraint_unit(entity: Entities, sub_type=None, sub_type_val=None) -> dict:
    constraint: dict = {
        'entity_type': entity,
        'sub_type': sub_type,
        'sub_type_val': sub_type_val
    }
    return constraint


def build_search_constraint_unit(keyword, value) -> dict:
    constraint: dict = {
        'keyword': keyword,
        'value': value
    }
    return constraint


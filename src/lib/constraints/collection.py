from lib.constraints.base import (
    build_constraint,
    build_constraint_unit,
    build_search_constraint_unit,
)
from lib.ontology import Ontology


# can be the descendant of / --->
def build_all_collection_constraints(entity):

    descendant = build_constraint_unit(entity)
    ancestor = build_constraint_unit(Ontology.ops().entities().SOURCE)
    ancestor2 = build_constraint_unit(Ontology.ops().entities().SAMPLE)
    ancestor3 = build_constraint_unit(Ontology.ops().entities().DATASET)

    return [build_constraint([ancestor, ancestor2, ancestor3], [descendant])]


def build_collection_search_constraints(entity):
    descendant = build_constraint_unit(entity)
    ancestor = build_search_constraint_unit(
        "entity_type.keyword", Ontology.ops().entities().DATASET
    )
    ancestor2 = build_search_constraint_unit(
        "entity_type.keyword", Ontology.ops().entities().SAMPLE
    )
    ancestor3 = build_search_constraint_unit(
        "entity_type.keyword", Ontology.ops().entities().SOURCE
    )

    return [build_constraint([ancestor, ancestor2, ancestor3], [descendant])]

from lib.constraints.base import build_constraint, build_constraint_unit, build_search_constraint_unit
from lib.ontology import Ontology


# can be the descendant of / --->
def build_all_epicollection_constraints(entity):

    ancestor = build_constraint_unit(Ontology.ops().entities().DATASET)
    descendant = build_constraint_unit(entity)

    return [
        build_constraint(ancestor, [descendant])
    ]

def build_epicollection_search_constraints(entity):
    descendant = build_constraint_unit(entity)
    ancestor = build_search_constraint_unit('entity_type.keyword', Ontology.ops().entities().DATASET)

    return [
        build_constraint([ancestor], [descendant])
    ]

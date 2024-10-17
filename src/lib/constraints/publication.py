from lib.constraints.base import build_constraint, build_constraint_unit
from lib.ontology import Ontology


# can be the descendant of / --->
def build_all_publication_constraints(entity):

    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(Ontology.ops().entities().DATASET)

    return [
        build_constraint(ancestor, [descendant])
    ]
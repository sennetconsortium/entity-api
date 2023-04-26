from lib.constraints.base import build_constraint, build_constraint_unit
from lib.ontology import Ontology


# can be the descendant of / --->
def build_all_source_constraints(entity):

    # Sample organ ---> Source
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(Ontology.entities().SAMPLE, [Ontology.specimen_categories().ORGAN])

    return [
        build_constraint(ancestor, [descendant])
    ]
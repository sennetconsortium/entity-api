from lib.ontology import Entities, SpecimenCategory
from lib.constraints.base import build_constraint, build_constraint_unit


def build_all_source_constraints(entity):
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategory.ORGAN])

    return [
        build_constraint(ancestor, [descendant])
    ]
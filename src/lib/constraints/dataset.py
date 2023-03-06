from lib.constraints.base import build_constraint, build_constraint_unit, build_search_constraint_unit
from lib.ontology import Entities, SpecimenCategory, Organs


# can be the descendant of / --->
def build_all_dataset_constraints(entity):

    # Dataset ---> Dataset
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(entity)
    return [
        build_constraint(ancestor, [descendant])
    ]


def build_dataset_search_constraints(entity):
    descendant = build_constraint_unit(entity)
    ancestor1 = build_search_constraint_unit('entity_type.keyword', Entities.DATASET)
    ancestor2 = build_search_constraint_unit('sample_category.keyword', SpecimenCategory.BLOCK)
    ancestor3 = build_search_constraint_unit('sample_category.keyword', SpecimenCategory.SECTION)
    ancestor4 = build_search_constraint_unit('sample_category.keyword', SpecimenCategory.SUSPENSION)

    return [
        build_constraint([ancestor1, ancestor2, ancestor3, ancestor4], [descendant])
    ]

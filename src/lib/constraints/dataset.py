from lib.constraints.base import (
    build_constraint,
    build_constraint_unit,
    build_search_constraint_unit,
)
from lib.ontology import Ontology


# can be the descendant of / --->
def build_all_dataset_constraints(entity):

    # Dataset, Publication ---> Dataset
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(entity)
    descendant2 = build_constraint_unit(Ontology.ops().entities().PUBLICATION)
    return [build_constraint(ancestor, [descendant, descendant2])]


def build_dataset_search_constraints(entity):
    descendant = build_constraint_unit(entity)
    SpecimenCategories = Ontology.ops().specimen_categories()
    ancestor1 = build_search_constraint_unit(
        "entity_type.keyword", Ontology.ops().entities().DATASET
    )
    ancestor2 = build_search_constraint_unit("sample_category.keyword", SpecimenCategories.BLOCK)
    ancestor3 = build_search_constraint_unit("sample_category.keyword", SpecimenCategories.SECTION)
    ancestor4 = build_search_constraint_unit(
        "sample_category.keyword", SpecimenCategories.SUSPENSION
    )

    return [build_constraint([ancestor1, ancestor2, ancestor3, ancestor4], [descendant])]

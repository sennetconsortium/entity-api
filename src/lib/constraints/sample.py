from lib.constraints.base import build_constraint, build_constraint_unit
from lib.ontology import Ontology

# can be the descendant of / --->


def build_sample_organ_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    SpecimenCategories = Ontology.ops().specimen_categories()
    Entities = Ontology.ops().entities()

    # Sample suspension ---> Sample organ of blood
    ancestor = build_constraint_unit(
        entity, [SpecimenCategories.ORGAN], [Ontology.ops().organ_types().BLOOD]
    )
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.SUSPENSION])
    constraints.append(build_constraint(ancestor, [descendant]))

    # Sample block ---> Sample organ
    ancestor = build_constraint_unit(entity, [SpecimenCategories.ORGAN])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.BLOCK])
    constraints.append(build_constraint(ancestor, [descendant]))

    return constraints


def build_sample_block_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    SpecimenCategories = Ontology.ops().specimen_categories()
    Entities = Ontology.ops().entities()

    # Sample block, section, suspension; Dataset ---> Sample block
    ancestor = build_constraint_unit(entity, [SpecimenCategories.BLOCK])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.BLOCK])
    descendant2 = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.SECTION])
    descendant3 = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.SUSPENSION])
    descendant4 = build_constraint_unit(
        Entities.DATASET, [Ontology.ops().dataset_types().LIGHT_SHEET]
    )
    constraints.append(
        build_constraint(ancestor, [descendant, descendant2, descendant3, descendant4])
    )

    return constraints


def build_sample_section_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    SpecimenCategories = Ontology.ops().specimen_categories()
    Entities = Ontology.ops().entities()

    # Dataset ---> Sample section
    ancestor = build_constraint_unit(entity, [SpecimenCategories.SECTION])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.SUSPENSION])
    descendant2 = build_constraint_unit(Entities.DATASET)
    constraints.append(build_constraint(ancestor, [descendant, descendant2]))

    return constraints


def build_sample_suspension_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    SpecimenCategories = Ontology.ops().specimen_categories()
    Entities = Ontology.ops().entities()

    # Sample suspension; Dataset ---> Sample suspension
    ancestor = build_constraint_unit(entity, [SpecimenCategories.SUSPENSION])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategories.SUSPENSION])
    descendant2 = build_constraint_unit(Entities.DATASET)
    constraints.append(build_constraint(ancestor, [descendant, descendant2]))

    return constraints


def build_all_sample_constraints(entity):
    constraints = build_sample_organ_constraints(entity)
    constraints = build_sample_block_constraints(entity, constraints)
    constraints = build_sample_section_constraints(entity, constraints)
    constraints = build_sample_suspension_constraints(entity, constraints)

    return constraints

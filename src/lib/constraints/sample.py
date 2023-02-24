from lib.ontology import get_entities, Entities, SpecimenCategory, Organs, DataTypes
from lib.constraints.base import build_constraint, build_constraint_unit


# can be the descendant of / --->

def build_sample_organ_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    # Sample block ---> Sample organ
    ancestor = build_constraint_unit(entity, [SpecimenCategory.ORGAN])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategory.BLOCK])
    constraints.append(build_constraint(ancestor, [descendant]))

    # Sample suspension ---> Sample organ of blood
    ancestor = build_constraint_unit(entity, [SpecimenCategory.ORGAN], [Organs.BLOOD])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategory.SUSPENSION])
    constraints.append(build_constraint(ancestor, [descendant]))

    return constraints


def build_sample_block_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    # Sample block, section, suspension; Dataset ---> Sample block
    ancestor = build_constraint_unit(entity, [SpecimenCategory.BLOCK])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategory.BLOCK,
                                                         SpecimenCategory.SECTION,
                                                         SpecimenCategory.SUSPENSION])
    descendant2 = build_constraint_unit(Entities.DATASET, [DataTypes.LIGHTSHEET])
    constraints.append(build_constraint(ancestor, [descendant, descendant2]))

    return constraints


def build_sample_section_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    # Dataset ---> Sample section
    ancestor = build_constraint_unit(entity, [SpecimenCategory.SECTION])
    descendant = build_constraint_unit(Entities.DATASET)
    constraints.append(build_constraint(ancestor, [descendant]))

    return constraints


def build_sample_suspension_constraints(entity, constraints=None):
    if constraints is None:
        constraints = []

    # Sample suspension; Dataset ---> Sample suspension
    ancestor = build_constraint_unit(entity, [SpecimenCategory.SUSPENSION])
    descendant = build_constraint_unit(Entities.SAMPLE, [SpecimenCategory.SUSPENSION])
    descendant2 = build_constraint_unit(Entities.DATASET)
    constraints.append(build_constraint(ancestor, [descendant, descendant2]))

    return constraints


def build_all_sample_constraints(entity):
    constraints = build_sample_organ_constraints(entity)
    constraints = build_sample_block_constraints(entity, constraints)
    constraints = build_sample_section_constraints(entity, constraints)
    constraints = build_sample_suspension_constraints(entity, constraints)

    return constraints

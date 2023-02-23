from lib.constraints.base import build_constraint, build_constraint_unit


def build_all_dataset_constraints(entity):
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(entity)
    return [
        build_constraint(ancestor, [descendant])
    ]
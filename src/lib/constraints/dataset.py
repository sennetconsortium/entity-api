from lib.constraints.base import build_constraint, build_constraint_unit


# can be the descendant of / --->
def build_all_dataset_constraints(entity):

    # Dataset ---> Dataset
    ancestor = build_constraint_unit(entity)
    descendant = build_constraint_unit(entity)
    return [
        build_constraint(ancestor, [descendant])
    ]
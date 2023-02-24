from lib.constraints.base import *
from lib.constraints.source import *
from lib.constraints.sample import *
from lib.constraints.dataset import *
from lib.rest import *


def build_source_constraints(entity):
    return build_all_source_constraints(entity)


def build_sample_constraints(entity):
    return build_all_sample_constraints(entity)


def build_dataset_constraints(entity):
    return build_all_dataset_constraints(entity)


def build_constraints():
    entities = get_entities()
    all_constraints: dict = {}
    try:
        for entity in entities:
            func = f"build_{entity}_constraints"
            all_constraints[entity] = locals()[func](entity)
    except Exception as e:
        print(f"{e}")

    return all_constraints


def determine_constraint_from_entity(constraint_unit) -> dict:
    entity_type = constraint_unit.get('entity_type')
    sub_type = constraint_unit.get('sub_type')
    error = None
    constraints = []
    if entity_type == Entities.SAMPLE:
        try:
            func = f"build_sample_{sub_type[0]}_constraints"
            constraints = globals()[func](entity_type)
        except Exception as e:
            error = f"No `sub_type` found with value `{sub_type[0]}`"
    elif entity_type == Entities.SOURCE:
        constraints = build_source_constraints(entity_type)
    elif entity_type == Entities.DATASET:
        constraints = build_dataset_constraints(entity_type)
    else:
        error = f"No `entity_type` found with value `{entity_type}`"

    return {
        'constraints': constraints,
        'error': error
    }


def validate_constraint_unit_to_entry_unit(entry_units, const_units):
    match = False
    for entry_unit in entry_units:

        sub_type = entry_unit.get('sub_type')
        if sub_type is not None:
            sub_type.sort()

        sub_type_val = entry_unit.get('sub_type_val')
        if sub_type_val is not None:
            sub_type_val.sort()

        if entry_unit in const_units:
            match = True
            break
    return match


def validate_constraint(entry, is_match=False) -> dict:
    entry_ancestor = entry.get('ancestor')
    results = []

    if entry_ancestor is not None:
        constraints = determine_constraint_from_entity(entry_ancestor)
        if constraints.get('error') is not None:
            results.append(rest_response(StatusCodes.BAD_REQUEST, 'Bad Request', constraints.get('error')))

        for constraint in constraints.get('constraints'):
            const_ancestor = constraint.get('ancestor')

            if entry_ancestor.items() <= const_ancestor.items():
                const_descendants = constraint.get('descendants')

                if is_match:
                    entry_descendants = entry.get('descendants')
                    if validate_constraint_unit_to_entry_unit(entry_descendants, const_descendants):
                        results.append(rest_response(StatusCodes.OK, 'OK', const_descendants))
                        break
                    else:
                        results.append(rest_response(StatusCodes.NOT_FOUND, 'Match not found', const_descendants))
                else:
                    results.append(rest_response(StatusCodes.OK, 'OK', const_descendants))
                    break

            else:
                results.append(rest_response(StatusCodes.NOT_FOUND, 'No matching constraints on given ancestor', const_ancestor))

    return results[len(results) - 1]



"""
[
    {
        "ancestor": {
            "entity_type": "sample",
            "sub_type": ["suspension"],
            "sub_type_val": null
        },
        "descendants": [
          	{
              "entity_type": "sample",
              "sub_type": ["suspension"],
              "sub_type_val": null
        	}
        ]
    }
]
"""
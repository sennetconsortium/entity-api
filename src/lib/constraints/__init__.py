from lib.constraints.base import *
from lib.constraints.source import *
from lib.constraints.sample import *
from lib.constraints.dataset import *
from lib.rest import *


def build_source_constraints(entity) -> list:
    return build_all_source_constraints(entity)


def build_sample_constraints(entity) -> list:
    return build_all_sample_constraints(entity)


def build_dataset_constraints(entity) -> list:
    return build_all_dataset_constraints(entity)


def determine_constraint_from_entity(constraint_unit, use_case=None) -> dict:
    entity_type = constraint_unit.get('entity_type')
    sub_type = constraint_unit.get('sub_type')
    error = None
    constraints = []
    entities = get_entities()

    if entity_type not in entities:
        error = f"No `entity_type` found with value `{entity_type}`"
    else:
        try:
            _sub_type = f"{sub_type[0]}_" if sub_type is not None else ''
            _use_case = f"{use_case}_" if use_case is not None else ''
            func = f"build_{entity_type}_{_sub_type}{_use_case}constraints"
            constraints = globals()[func](entity_type)
        except Exception as e:
            error = f"Constraints could not be found with the combination of `sub_type`: `{sub_type[0]}` and `filter` as {use_case}"

    return {
        'constraints': constraints,
        'error': error
    }


def validate_constraint_units_to_entry_units(entry_units, const_units) -> bool:
    match = False
    const_units = get_constraint_unit_as_list(const_units)
    entry_units = get_constraint_unit_as_list(entry_units)
    for entry_unit in entry_units:

        sub_type = entry_unit.get('sub_type')
        if sub_type is not None:
            sub_type.sort()

        sub_type_val = entry_unit.get('sub_type_val')
        if sub_type_val is not None:
            sub_type_val.sort()

        if entry_unit in const_units:
            match = True
        else:
            match = False
            break

    return match


def get_constraints_by_descendant(entry, is_match=False, use_case=None) -> dict:
    return get_constraints(entry, 'descendants', 'ancestors', is_match, use_case)


def get_constraints_by_ancestor(entry, is_match=False, use_case=None) -> dict:
    return get_constraints(entry, 'ancestors', 'descendants', is_match, use_case)


def get_constraint_unit(entry):
    if type(entry) is list and len(entry) > 0:
        return entry[0]
    elif type(entry) is dict:
        return entry
    else:
        return None


def get_constraint_unit_as_list(entry):
    if type(entry) is list:
        return entry
    elif type(entry) is dict:
        return [entry]
    else:
        return []


def validate_exclusions(entry, constraint, key):
    entry_key = get_constraint_unit_as_list(entry.get(key))
    const_key = get_constraint_unit_as_list(constraint.get(key))

    if len(const_key) > 0 and const_key[0] == "!":
        const_key.pop(0)
        if any(x in entry_key for x in const_key):
            return False
        else:
            return True
    else:
        return False


def get_constraints(entry, key1, key2, is_match=False, use_case=None) -> dict:
    entry_key1 = get_constraint_unit(entry.get(key1))
    msg = f"Missing `{key1}` in request. Use orders=ancestors|descendants request param to specify. Default: ancestors"
    result = rest_bad_req(msg) if is_match else rest_ok("Nothing to validate.")

    if entry_key1 is not None:
        constraints = determine_constraint_from_entity(entry_key1, use_case)
        if constraints.get('error') is not None:
            result = rest_bad_req(constraints.get('error'))

        for constraint in constraints.get('constraints'):
            const_key1 = get_constraint_unit(constraint.get(key1))

            if entry_key1.items() <= const_key1.items() or validate_exclusions(entry_key1, const_key1, 'sub_type_val'):
                const_key2 = constraint.get(key2)

                if is_match:
                    entry_key2 = entry.get(key2)
                    if entry_key2 is not None and validate_constraint_units_to_entry_units(entry_key2, const_key2):
                        result = rest_ok(const_key2)
                    else:
                        result = rest_response(StatusCodes.NOT_FOUND, f"Match not found. Valid `{key2}` in description.", const_key2)
                else:
                    result = rest_ok(const_key2)
                break
            else:
                result = rest_response(StatusCodes.NOT_FOUND, f"No matching constraints on given `{key1}`", None)

    return result

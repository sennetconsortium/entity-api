from lib.constraints.base import *
from lib.constraints.source import *
from lib.constraints.sample import *
from lib.constraints.dataset import *
from lib.constraints.publication import *
from lib.constraints.collection import *
from lib.constraints.epicollection import *
from deepdiff import DeepDiff

from atlas_consortia_commons.rest import rest_ok, rest_response, StatusCodes, rest_bad_req
from atlas_consortia_commons.object import enum_val_lower
from lib.ontology import Ontology


def build_source_constraints(entity) -> list:
    return build_all_source_constraints(entity)


def build_sample_constraints(entity) -> list:
    return build_all_sample_constraints(entity)


def build_dataset_constraints(entity) -> list:
    return build_all_dataset_constraints(entity)


def build_publication_constraints(entity) -> list:
    return build_all_publication_constraints(entity)


def build_collection_constraints(entity) -> list:
    return build_all_collection_constraints(entity)


def build_epicollection_constraints(entity) -> list:
    return build_all_epicollection_constraints(entity)


def determine_constraint_from_entity(constraint_unit, use_case=None) -> dict:
    entity_type = constraint_unit.get("entity_type", "")
    entity_type = entity_type.lower()
    sub_type = constraint_unit.get("sub_type")
    error = None
    constraints = []
    entities = Ontology.ops(as_arr=True, cb=enum_val_lower).entities()
    # Need to manually add Epicollection
    entities.append("collection")
    entities.append("epicollection")
    if entity_type not in entities:
        error = f"No `entity_type` found with value `{entity_type}`"
    else:
        try:
            _sub_type = f"{sub_type[0].replace(' ', '_')}_" if sub_type is not None else ""
            _use_case = f"{use_case}_" if use_case is not None else ""
            func = f"build_{entity_type}_{_sub_type}{_use_case}constraints"
            constraints = globals()[func.lower()](entity_type)
        except Exception as e:
            filter_err = f" and `filter` as {use_case}" if use_case is not None else ""
            error = f"Constraints could not be found with `sub_type`: `{sub_type[0]}`{filter_err}."
            if not use_case:
                func = f"build_{entity_type}_constraints"
                constraints = globals()[func.lower()](entity_type)
    return {"constraints": constraints, "error": error}


def validate_constraint_units_to_entry_units(entry_units, const_units) -> bool:
    all_match = []
    const_units = get_constraint_unit_as_list(const_units)
    entry_units = get_constraint_unit_as_list(entry_units)
    for entry_unit in entry_units:

        sub_type = entry_unit.get("sub_type")
        if sub_type is not None:
            sub_type.sort()

        sub_type_val = entry_unit.get("sub_type_val")
        if sub_type_val is not None:
            sub_type_val.sort()

        match = False
        for const_unit in const_units:
            if (
                DeepDiff(
                    entry_unit, const_unit, ignore_string_case=True, exclude_types=[type(None)]
                )
                == {}
            ):
                match = True
                break

        all_match.append(match)

    return False not in all_match


def get_constraints_by_descendant(entry, is_match=False, use_case=None) -> dict:
    return get_constraints(entry, "descendants", "ancestors", is_match, use_case)


def get_constraints_by_ancestor(entry, is_match=False, use_case=None) -> dict:
    return get_constraints(entry, "ancestors", "descendants", is_match, use_case)


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


# Validates based on exclusions. Example constraint:
# build_constraint_unit(entity, [SpecimenCategory.BLOCK], ['!', Organs.BLOOD])
def validate_exclusions(entry, constraint, key) -> bool:
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
    result = rest_bad_req(msg, True) if is_match else rest_ok("Nothing to validate.", True)

    if entry_key1 is not None:
        report = determine_constraint_from_entity(entry_key1, use_case)
        constraints = report.get("constraints")
        if report.get("error") is not None and not constraints:
            result = rest_bad_req(report.get("error"), True)

        for constraint in constraints:
            const_key1 = get_constraint_unit(constraint.get(key1))

            if (
                DeepDiff(
                    entry_key1, const_key1, ignore_string_case=True, exclude_types=[type(None)]
                )
                == {}
            ):  # or validate_exclusions(entry_key1, const_key1, 'sub_type_val'):
                const_key2 = constraint.get(key2)

                if is_match:
                    entry_key2 = entry.get(key2)
                    v = validate_constraint_units_to_entry_units(entry_key2, const_key2)
                    if entry_key2 is not None and v:
                        result = rest_ok(const_key2, True)
                    else:
                        entity_type = entry_key1.get("entity_type")
                        entity_type = (
                            entity_type.title() if entity_type is not None else entity_type
                        )
                        sub_type = entry_key1.get("sub_type")
                        sub_type = ", ".join(sub_type) if sub_type is not None else ""
                        msg = (
                            f"This `{entity_type}` `{sub_type}` cannot be associated with the provided `{key1}` due to entity constraints. "
                            f"Click the link to view valid entity types that can be `{key2}`"
                        )
                        result = rest_response(StatusCodes.NOT_FOUND, msg, const_key2, True)
                else:
                    result = rest_ok(const_key2, True)
                break

            else:
                result = rest_response(
                    StatusCodes.NOT_FOUND, f"No matching constraints on given `{key1}`", None, True
                )

    return result

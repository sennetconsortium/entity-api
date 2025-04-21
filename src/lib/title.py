import re

from lib.ontology import Ontology


def make_phrase_from_separator_delineated_str(separated_phrase: str, separator: str, new_separator: str = ", ") -> str:
    """
    Given a string which contains multiple items, each separated by the substring specified by
    the 'separator' argument, and possibly also ending with 'separator',
    - remove the last instance of 'separator'
    - replaced the remaining last instance of 'separator' with ", and"
    - replace all remaining instances of 'separator' with the substring specified in the 'new_separator' argument

    Parameters
    ----------
    separated_phrase : str
        A string which contains multiple items, each separated by the substring specified by
        the 'separator' argument, and possibly also ending with 'separator'
    separator : str
        A string which is used to separate items during computation.  This should be something which
        is statistically improbable to occur within items, such as a comma or a common word.
    new_separator: str
        The replacement for occurrences of 'separator', such as a comma or a comma followed by a space. Defaults to ', '.

    Returns
    -------
    str: A version of the 'separated_phase' argument revised per the method description
    """
    # Remove the last separator
    if re.search(rf"{separator}$", separated_phrase):
        separated_phrase = re.sub(pattern=rf"(.*)({separator})$", repl=r"\1", string=separated_phrase)

    # Replace the last separator with the word 'and' for inclusion in the Dataset title
    separated_phrase = re.sub(pattern=rf"(.*)({separator})(.*?)$", repl=r"\1, and \3", string=separated_phrase)

    # Replace all remaining separator with commas
    descriptions = separated_phrase.rsplit(separator)
    return new_separator.join(descriptions)


def get_attributes_from_source_metadata(source_type: str, source_metadata: dict) -> dict:
    """
    Given a string of metadata for a Source which was returned from Neo4j, and a list of desired attribute names to
    extract from that metadata, return a dictionary containing lower-case version of each attribute found.

    Parameters
    ----------
    source_type : str
        The type of source
    source_metadata : dict
        A Python dict returned from Neo4j, containing metadata for a Source.

    Returns
    -------
    dict: A dict keyed using elements of attribute_key_list which were found in the Source metadata, containing
        a lower-case version of the value stored in Neo4j
    """
    source_grouping_concepts_dict = dict()
    source_types = Ontology.ops().source_types()

    if source_type in [source_types.HUMAN, source_types.HUMAN_ORGANOID]:
        # human and human organoid source
        if "organ_donor_data" in source_metadata:
            source_metadata = source_metadata["organ_donor_data"]

        if "living_donor_data" in source_metadata:
            source_metadata = source_metadata["living_donor_data"]

        for data in source_metadata:
            if "grouping_concept_preferred_term" in data:
                if data["grouping_concept_preferred_term"].lower() == "age":
                    # The actual value of age stored in "data_value" instead of "preferred_term"
                    source_grouping_concepts_dict["age"] = data["data_value"]
                    units = data["units"].lower()
                    if units[-1] == "s":
                        # Make sure the units are singular
                        units = units[0:-1]
                    source_grouping_concepts_dict["age_units"] = units
                elif data["grouping_concept_preferred_term"].lower() == "race":
                    source_grouping_concepts_dict["race"] = data["preferred_term"].lower()
                elif data["grouping_concept_preferred_term"].lower() == "sex":
                    source_grouping_concepts_dict["sex"] = data["preferred_term"].lower()

    else:
        # mouse and mouse organoid source, just pass through
        source_grouping_concepts_dict = source_metadata

    return source_grouping_concepts_dict


def get_source_data_phrase(source_type: str, source_data: dict) -> str:
    """
    Given a age, race, and sex metadata for a Source which was returned from Neo4j, generate an appropriate and
    consistent string phrase.

    Parameters
    ----------
    source_type : str
        The type of source
    source_data : dict
        A Python dict containing metadata attrobutes for a Source
        Human: age, race, sex
        Mouse: strain

    Returns
    -------
    str: A consistent string phrase appropriate for the Source's metadata
    """
    source_types = Ontology.ops().source_types()

    if source_type in [source_types.HUMAN, source_types.HUMAN_ORGANOID]:
        # human and human organ
        age = source_data.get("age")
        age_units = source_data.get("age_units")
        race = source_data.get("race")
        sex = source_data.get("sex")

        if age is None and race is not None and sex is not None:
            return f"{race} {sex} of unknown age"
        elif race is None and age is not None and sex is not None:
            return f"{age} {age_units}-old {sex} of unknown race"
        elif sex is None and age is not None and race is not None:
            return f"{age} {age_units}-old {race} {source_type.lower()} of unknown sex"
        elif age is None and race is None and sex is not None:
            return f"{sex} {source_type.lower()} of unknown age and race"
        elif age is None and sex is None and race is not None:
            return f"{race} {source_type.lower()} of unknown age and sex"
        elif race is None and sex is None and age is not None:
            return f"{age} {age_units}-old {source_type.lower()} of unknown race and sex"
        elif age is None and race is None and sex is None:
            return f"{source_type.lower()} of unknown age, race and sex"
        else:
            return f"{age} {age_units}-old {race} {sex}"
    else:
        # mouse and mouse organoid
        parts = []
        if strain := source_data.get("strain"):
            parts.append(strain)

        if sex := source_data.get("sex"):
            parts.append(sex.lower())

        parts.append(source_type.lower())

        return " ".join(parts)


def generate_title(
    organ_abbrev_set: set,
    source_uuid_set: set,
    dataset_type: str,
    organs_description_phrase: str,
    sources_description_phrase: str,
    source_organ_association_phrase: str,
    max_entity_list_length: int = 5,
) -> str:
    if len(organ_abbrev_set) == 1 and len(source_uuid_set) == 1:
        # One source, one organ type
        return f"{dataset_type} data from the {organs_description_phrase} of a {sources_description_phrase}"
    elif len(organ_abbrev_set) > 1 and len(organ_abbrev_set) <= max_entity_list_length and len(source_uuid_set) == 1:
        # One source, and more than 1 and less than max_entity_list_length organ types
        return f"{dataset_type} data from {organs_description_phrase} of " f"a {sources_description_phrase}"
    elif len(organ_abbrev_set) > max_entity_list_length and len(source_uuid_set) == 1:
        # One source, more than max_entity_list_length organ types
        return f"{dataset_type} data from {len(organ_abbrev_set)} organs of " f"a {sources_description_phrase}"
    elif len(organ_abbrev_set) == 1 and len(source_uuid_set) > 1 and len(source_uuid_set) <= max_entity_list_length:
        # More than 1 and less than max_entity_list_length sources, and one organ type
        return (
            f"{dataset_type} data from the {organs_description_phrase} of "
            f"{len(source_uuid_set)} different sources: {sources_description_phrase}"
        )
    elif len(organ_abbrev_set) == 1 and len(source_uuid_set) > max_entity_list_length:
        # More than max_entity_list_length sources, one organ type
        return (
            f"{dataset_type} data from the {organs_description_phrase} " f"of {len(source_uuid_set)} different sources"
        )
    elif (
        len(organ_abbrev_set) > 1
        and len(organ_abbrev_set) <= max_entity_list_length
        and len(source_uuid_set) > 1
        and len(source_uuid_set) <= max_entity_list_length
    ):
        # More than 1 and less than max_entity_list_length sources, and
        # more than 1 and less than max_entity_list_length organ types
        return (
            f"{dataset_type} data from {len(organ_abbrev_set)} organs of "
            f"{len(source_uuid_set)} different sources: "
            f"{source_organ_association_phrase}"
        )
    elif (
        len(organ_abbrev_set) > max_entity_list_length
        and len(source_uuid_set) > 1
        and len(source_uuid_set) <= max_entity_list_length
    ):
        #  More than 1 and less than max_entity_list_length sources, and more than max_entity_list_length organ type
        return (
            f"{dataset_type} data from {len(organ_abbrev_set)} organs of "
            f"{len(source_uuid_set)} different sources: "
            f"{sources_description_phrase}"
        )
    elif (
        len(organ_abbrev_set) > 1
        and len(organ_abbrev_set) <= max_entity_list_length
        and len(source_uuid_set) > max_entity_list_length
    ):
        #  More than max_entity_list_length sources, and more than 1 and less than max_entity_list_length organ type
        return (
            f"{dataset_type} data from the {organs_description_phrase} " f"of {len(source_uuid_set)} different sources"
        )
    else:
        # Default, including more than max_entity_list_length sources, and more than max_entity_list_length organ types
        return (
            f"{dataset_type} data from {len(organ_abbrev_set)} organs of " f"{len(source_uuid_set)} different sources"
        )

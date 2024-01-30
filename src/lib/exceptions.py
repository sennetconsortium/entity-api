def create_trigger_error_msg(msg: str, existing_data_dict: dict, new_data_dict: dict) -> str:
    """Creates an error message for a schema trigger function by appending additional
    useful information to the message.

    Args:
        msg (str): The message to display before the additional information.
        existing_data_dict (dict): The existing data.
        new_data_dict (dict): The new data.

    Returns:
        str: The error message.
    """
    return (
        f"{msg} "
        f"UUID: {existing_data_dict.get('uuid') or new_data_dict.get('uuid')} "
        f"SenNet ID: {existing_data_dict.get('sennet_id') or new_data_dict.get('sennet_id')} "
        f"Entity type: {existing_data_dict.get('entity_type') or new_data_dict.get('entity_type')}"
    )

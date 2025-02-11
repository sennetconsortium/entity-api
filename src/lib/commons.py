import json
from hubmap_commons.string_helper import convert_str_literal

def get_as_dict(data_str):
    """
    A fallback method to temporarily replace hubmap_commons.convert_str_literal

    Parameters
    ----------
    data_str : str

    Returns
    -------
    A dict representation of the data_str
    """
    if isinstance(data_str, str):
        try:
            data = json.loads(data_str)

            if isinstance(data, (list, dict)):
                # The input string literal has been converted to {type(data)} successfully
                return data

        except (SyntaxError, ValueError, TypeError) as e:
            try:
                # Fallback with HM method
                data = convert_str_literal(data_str)
                if isinstance(data, (list, dict)):
                    # The input string literal has been converted to {type(data)} successfully
                    return data
            except (SyntaxError, ValueError, TypeError) as e:
                raise ValueError(f"Invalid expression (string value): {data_str} from ast.literal_eval(); "
                                 f"specific error: {str(e)}")
    # Skip any non-string data types, or a string literal that is not list or dict after evaluation
    return data_str
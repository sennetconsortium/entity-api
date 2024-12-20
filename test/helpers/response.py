from unittest.mock import MagicMock


def mock_response(status_code=200, json_data=None):
    res = MagicMock()
    res.status_code = status_code
    if json_data:
        res.json.return_value = json_data
    return res

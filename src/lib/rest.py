from enum import IntEnum
from flask import request, abort, make_response


class StatusCodes(IntEnum):
    OK = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    UNACCEPTABLE = 406
    SERVER_ERR = 500


def is_json_request():
    return request.content_type == 'application/json'


def server_error(e):
    return rest_response(StatusCodes.SERVER_ERR, 'Sever Error', f"{e}")


def rest_response(code: StatusCodes, name: str, desc):
    return {
        'code': code,
        'name': name,
        'description': desc
    }


def full_response(response: dict):
    return make_response(response, int(response.get('code')), get_json_header())


def get_json_header(headers: dict = None):
    if headers is None:
        headers = {}
    headers["Content-Type"] = "application/json"
    return headers


def bad_request_error(desc):
    abort(StatusCodes.BAD_REQUEST, description=desc)
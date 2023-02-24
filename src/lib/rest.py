from enum import IntEnum, Enum
from flask import request, abort, make_response


class StatusCodes(IntEnum):
    OK = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    UNACCEPTABLE = 406
    SERVER_ERR = 500


class StatusMsgs(str, Enum):
    OK = 'OK'
    BAD_REQUEST = 'Bad Request'
    NOT_FOUND = 'Not Found'
    UNACCEPTABLE = 'Unacceptable Request'
    SERVER_ERR = 'Internal Server Error'


def is_json_request():
    return request.content_type == 'application/json'


def rest_sever_err(e):
    return rest_response(StatusCodes.SERVER_ERR, StatusMsgs.SERVER_ERR, f"{e}")


def rest_ok(desc):
    return rest_response(StatusCodes.OK, StatusMsgs.OK, desc)


def rest_bad_req(desc):
    return rest_response(StatusCodes.BAD_REQUEST, StatusMsgs.BAD_REQUEST, desc)


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

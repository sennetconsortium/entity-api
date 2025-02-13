from test.helpers import GROUP, GROUP_ID, USER
from unittest.mock import MagicMock, patch

import pytest

AUTH_TOKEN = "test_token"


@pytest.fixture(scope="session")
def auth():
    globus_group_info = {"by_id": {GROUP_ID: GROUP}}

    auth_mock = MagicMock()
    auth_mock.get_globus_groups_info.return_value = globus_group_info
    auth_mock.getAuthorizationTokens = MagicMock(side_effect=get_authorization_tokens)
    auth_mock.getProcessSecret.return_value = "super_process_secret"
    auth_mock.groupNameToId.return_value = GROUP
    auth_mock.getUserTokenFromRequest = MagicMock(side_effect=get_user_token_from_request)
    auth_mock.getUserInfoUsingRequest = MagicMock(side_effect=get_user_info_using_request)
    auth_mock.getUserInfo = MagicMock(side_effect=get_user_info)

    with (
        patch("hubmap_commons.hm_auth.AuthHelper.configured_instance", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.create", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.instance", return_value=auth_mock),
    ):
        yield auth_mock


def get_authorization_tokens(headers):
    return headers.get("Authorization").split(" ")[1]


def get_user_token_from_request(req, getGroups=True):
    auth_header = req.headers.get("Authorization")
    if auth_header == f"Bearer {AUTH_TOKEN}":
        return AUTH_TOKEN
    else:
        return None


def get_user_info_using_request(req, getGroups=True):
    auth_header = req.headers.get("Authorization")
    if auth_header == f"Bearer {AUTH_TOKEN}":
        return USER
    else:
        return None


def get_user_info(token, getGroups=True):
    if token == AUTH_TOKEN:
        return USER
    else:
        return None

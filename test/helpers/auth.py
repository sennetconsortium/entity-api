from test.helpers import GROUP, GROUP_ID, USER
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session")
def auth():
    token = "test_token"
    user_info = USER
    globus_group_info = {"by_id": {GROUP_ID: GROUP}}

    auth_mock = MagicMock()
    auth_mock.get_globus_groups_info.return_value = globus_group_info
    auth_mock.getUserTokenFromRequest.return_value = token
    auth_mock.getUserInfoUsingRequest.return_value = user_info
    auth_mock.getUserInfo.return_value = user_info

    with (
        patch("hubmap_commons.hm_auth.AuthHelper.configured_instance", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.create", return_value=auth_mock),
        patch("hubmap_commons.hm_auth.AuthHelper.instance", return_value=auth_mock),
    ):
        yield auth_mock

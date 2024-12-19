import pytest


class RequestsMock:
    def __init__(self):
        self._responses = {}

    def add_response(self, url, method, response):
        self._responses[method.lower()][url.lower()] = response

    def get(self, url, *args, **kwargs):
        return self._responses["get"][url.lower()]

    def post(self, url, *args, **kwargs):
        return self._responses["post"][url.lower()]

    def put(self, url, *args, **kwargs):
        return self._responses["put"][url.lower()]

    def delete(self, url, *args, **kwargs):
        return self._responses["delete"][url.lower()]


@pytest.fixture(scope="session")
def requests(monkeypatch):
    mock = RequestsMock()

    monkeypatch.setattr(requests, "get", mock.get)
    monkeypatch.setattr(requests, "post", mock.post)
    monkeypatch.setattr(requests, "put", mock.put)
    monkeypatch.setattr(requests, "delete", mock.delete)

    return mock

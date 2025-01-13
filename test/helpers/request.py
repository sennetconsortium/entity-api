import pytest
import requests as requests_module


class RequestsMock:
    def __init__(self):
        self._responses = {
            "get": {},
            "post": {},
            "put": {},
            "delete": {},
        }
        self._call_index = {
            "get": {},
            "post": {},
            "put": {},
            "delete": {},
        }

    def add_response(self, url, method, response):
        normalized_url = self._normalize_url(url)
        if normalized_url in self._responses[method.lower()]:
            self._responses[method.lower()][normalized_url].append(response)
        else:
            self._responses[method.lower()][normalized_url] = [response]

    def get(self, url, *args, **kwargs):
        return self._get_response(url, "get")

    def post(self, url, *args, **kwargs):
        return self._get_response(url, "post")

    def put(self, url, *args, **kwargs):
        return self._get_response(url, "put")

    def delete(self, url, *args, **kwargs):
        return self._get_response(url, "delete")

    def _get_response(self, url, method):
        normalized_url = self._normalize_url(url)
        if normalized_url not in self._responses[method]:
            raise ValueError(f"No response for {method.upper()} {url}")

        idx = self._call_index[method].get(normalized_url, 0)
        if idx >= len(self._responses[method][normalized_url]):
            raise ValueError(f"No more responses for {method.upper()} {url}")
        value = self._responses[method][normalized_url][idx]
        self._call_index[method][normalized_url] = idx + 1
        return value

    def _normalize_url(self, url):
        return url.lower().strip("/")


@pytest.fixture()
def requests(monkeypatch):
    mock = RequestsMock()

    monkeypatch.setattr(requests_module, "get", mock.get)
    monkeypatch.setattr(requests_module, "post", mock.post)
    monkeypatch.setattr(requests_module, "put", mock.put)
    monkeypatch.setattr(requests_module, "delete", mock.delete)

    yield mock

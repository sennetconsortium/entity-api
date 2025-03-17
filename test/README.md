# Entity-API Tests

## Troubleshoot
- When adding tests, note that calls to various services must be mocked. For example, if a particular request make 2 calls to the UUID service in the actual api, this number of responses must be added/mocked.
Otherwise, the test will fail and raise a `ValueError`. Example:
``` 
ValueError: No more responses for GET http://uuid-api:8080/uuid/d2be3c6ff38f4ef7a2301f6d9f5c65ff. The URL was called 2 times but only 1 responses are in the RequestsMock. Please add an additional response in tests.
```
- To see the results of `print()` pass `-s` flag to `pytest -W ignore::DeprecationWarning`


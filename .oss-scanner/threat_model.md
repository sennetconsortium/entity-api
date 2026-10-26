# SenNet Entity API Threat Model

## Running the Tests

Neo4j 5.20 with APOC is installed in the image and must be started first. Run `start-neo4j`, then `python -m pytest -q` from `/src`. The tests use a local Neo4j and mocked UUID API and UBKG API responses, so they need no network access.

## Project Scope

The SenNet Entity API is a Flask service for creating, reading, and updating entity metadata. It uses Neo4j for graph storage and integrates with SenNet Consortium web services. This assessment should focus on code in this repository and security impact through the API.

## Trust Boundaries and Attacker Model

Treat unauthenticated remote callers and authenticated callers with limited permissions as potentially malicious. Treat request bodies, query parameters, headers, identifiers, and metadata values supplied by callers as untrusted. Assume an attacker may try to bypass authentication or authorization, access or alter another user's or group's metadata, or cause unsafe behavior in downstream integrations.

## In-Scope Areas

- HTTP request parsing, input validation, and error handling.
- Authentication, authorization, and entity-level access controls.
- Entity creation, retrieval, update, provenance, and metadata visibility logic.
- Neo4j query construction and handling of database results.
- Handling of configuration and credentials by code in this repository.
- Calls from this service to external consortium services, where an issue in this repository creates a security impact.

Prioritize demonstrated confidentiality, integrity, or availability impact. Reports should identify affected endpoints and preconditions, explain the impact, and include a minimal reproducible example. Do not use production services, real credentials, or real user data when validating a finding.

## Out of Scope

- Vulnerabilities confined to third-party dependencies or external services without a demonstrable security impact through this repository's code.
- Deployment, cloud, network, or host configuration issues outside this repository.
- Findings that require access to production secrets or data to demonstrate.
- Availability claims based only on theoretical resource consumption without a practical impact demonstration.
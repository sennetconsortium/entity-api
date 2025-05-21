# SenNet Entity API

A set of standard RESTful web service that provides CRUD operations into our entity metadata store. A description of the API calls is found here: [Entities API](https://smart-api.info/ui/7d838c9dee0caa2f8fe57173282c5812).

## Entities schema yaml

The yaml file `src/resources/hubmap-entities.yaml` contains all the attributes of each entity type and generated metadata information of attributes via trigger methods. This file is being used to validate the user input and also as a way of standarding all the details of entities.

## Docker build for local/DEV development

There are a few configurable environment variables to keep in mind:

- `COMMONS_BRANCH`: build argument only to be used during image creation when we need to use a branch of commons from github rather than the published PyPI package. Default to master branch if not set or null.
- `HOST_UID`: the user id on the host machine to be mapped to the container. Default to 1002 if not set or null.
- `HOST_GID`: the user's group id on the host machine to be mapped to the container. Default to 1002 if not set or null.

```
cd docker
./docker-development.sh [check|config|build|start|stop|down]
```

## Docker build for deployment on PROD

```
cd docker
./docker-deployment.sh [start|stop|down]
```

## Development process

### To release via TEST infrastructure
- Make new feature or bug fix branches from `main` branch (the default branch)
- Make PRs to `main`
- As a codeowner, Zhou (github username `yuanzhou`) is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to TEST infrastructure, and redeploy the TEST instance.
- Developer or someone on the team who is familiar with the change will test/qa the change
- When any current changes in the `main` have been approved after test/qa on TEST, Zhou will release to PROD using the same docker image that has been tested on TEST infrastructure.

### To work on features in the development environment before ready for testing and releasing
- Make new feature branches off the `main` branch
- Make PRs to `dev-integrate`
- As a codeowner, Zhou is automatically assigned as a reviewer to each PR. When all other reviewers have approved, he will approve as well, merge to devel, and redeploy the DEV instance.
- When a feature branch is ready for testing and release, make a PR to `main` for deployment and testing on the TEST infrastructure as above.

### Updating API Documentation

The documentation for the API calls is hosted on SmartAPI. Modifying the `entity-api-spec.yaml` file and committing the changes to github should update the API shown on SmartAPI. SmartAPI allows users to register API documents.  The documentation is associated with this github account: api-developers@sennetconsortium.org.

## Formatting

Python code in this repository uses [black](https://black.readthedocs.io/en/stable/) for formatting. This development dependency can be installed using `pip install -r src/requirements.dev.txt`. Black provides integration for various IDEs, such as [PyCharm](https://black.readthedocs.io/en/stable/integrations/editors.html#pycharm-intellij-idea) and [VSCode](https://black.readthedocs.io/en/stable/integrations/editors.html#visual-studio-code). Black can also be used in the terminal using the following commands.

```bash
# Reformat single file (src/app.py)
black src/app.py

# Reformat multiple files (all files in src/ directory)
black src/

# Reformat single file within specific line numbers 1 through 10 (src/app.py)
black --line-ranges=1-10 src/app.py

# Check without reformatting single file (src/app.py)
black --check src/app.py

# Check without reformatting multiple files (all files in src/ directory)
black --check src/
```

## Testing

Install the development dependencies using `pip install -r src/requirements.dev.txt`. Install Docker and ensure it is running. Run `./run_tests.sh` at the root of the project. This test script will create a temporary Neo4J database using Docker for integration tests.

### Run a neo4j test instance if needed for debugging
```angular2html
docker run \
	--env=NEO4J_AUTH=none \
    --publish=7474:7474 --publish=7687:7687 \
    --volume=$HOME/neo4j/data:/data \
    --name neo4j-apoc \
    -e NEO4J_apoc_export_file_enabled=true \
    -e NEO4J_apoc_import_file_enabled=true \
    -e NEO4J_apoc_import_file_use__neo4j__config=true \
    -e NEO4J_PLUGINS=\[\"apoc\"\] \
    neo4j
```

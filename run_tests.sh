# check if the neo4j-test docker container is running and stop it
if [ "$(docker ps -q -f name=neo4j-test)" ]; then
    echo "Stopping the existing neo4j-test container"
    docker stop neo4j-test > /dev/null
fi

# check if the neo4j-test docker container exists and remove it
if [ "$(docker ps -aq -f name=neo4j-test)" ]; then
    echo "Removing the existing neo4j-test container"
    docker rm neo4j-test > /dev/null
fi

# create a new neo4j-test docker container
echo "Creating a new neo4j-test container"
docker run -d \
    --name neo4j-test \
    -p 7474:7474 \
    -p 7687:7687 \
    -e NEO4J_AUTH=none \
    -e NEO4JLABS_PLUGINS=\[\"apoc\"\] \
    neo4j:5.20.0-ubi8

# Read values from config file and set them as environment variables
UBKG_SERVER=$(awk -F ' = ' '/UBKG_SERVER/ {print $2}' src/instance/app.cfg | tr -d '[:space:]' | sed "s/^'//;s/'$//")
UBKG_ENDPOINT_VALUESET=$(awk -F ' = ' '/UBKG_ENDPOINT_VALUESET/ {print $2}' src/instance/app.cfg | tr -d '[:space:]' | sed "s/^'//;s/'$//")
UBKG_CODES=$(awk -F ' = ' '/UBKG_CODES/ {print $2}' src/instance/app.cfg | tr -d '[:space:]' | sed "s/^'//;s/'$//")

# Set the test config file and backup the original config file
mv src/instance/app.cfg src/instance/app.cfg.bak
cp test/config/app.test.cfg src/instance/app.cfg

echo "Running tests"
UBKG_SERVER=$UBKG_SERVER \
UBKG_ENDPOINT_VALUESET=$UBKG_ENDPOINT_VALUESET \
UBKG_CODES=$UBKG_CODES \
pytest -W ignore::DeprecationWarning

# Restore the original config file
mv src/instance/app.cfg.bak src/instance/app.cfg

echo "Stopping and removing the neo4j-test container"
docker stop neo4j-test > /dev/null
docker rm neo4j-test > /dev/null

# Check if there's a entity-api-test container running, if found, tear it down and start fresh
if [ "$(docker ps -q -f name=entity-api-test)" ]; then
    echo "Tearing down the existing entity-api-test container"
    docker-compose -f docker/docker-compose.test.yml down
fi

# check if there's a src/instance/app.cfg file, if found, rename it to app.cfg.bak
if [ -f src/instance/app.cfg ]; then
    echo "Backing up src/instance/app.cfg to src/instance/app.cfg.bak"
    cp src/instance/app.cfg src/instance/app.cfg.bak

    echo "Setting environment variables from src/instance/app.cfg"
    UBKG_SERVER=$(grep UBKG_SERVER src/instance/app.cfg | cut -d"'" -f2)
    UBKG_ENDPOINT_VALUESET=$(grep UBKG_ENDPOINT_VALUESET src/instance/app.cfg | cut -d"'" -f2)
    UBKG_CODES=$(grep UBKG_CODES src/instance/app.cfg | cut -d"'" -f2)
else
    echo "No src/instance/app.cfg file found"
    echo "Setting default environment variables"
fi

# escape the & character
UBKG_ENDPOINT_VALUESET=$(echo "$UBKG_ENDPOINT_VALUESET" | sed 's/&/\\&/g')
UBKG_CODES=$(echo "$UBKG_CODES" | sed 's/&/\\&/g')

# copy the src/instance/app.cfg.example to src/instance/app.cfg
cp src/instance/app.cfg.example src/instance/app.cfg

# search src/instance/app.cfg for the lines that start with NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# replace with the test values
# sed options are different on mac and linux
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i "" "s|NEO4J_URI = 'bolt://hubmap-neo4j-localhost:7687'|NEO4J_URI = 'bolt://neo4j-test:7687'|g" src/instance/app.cfg
    sed -i "" "s|NEO4J_PASSWORD = '123'|NEO4J_PASSWORD = None|g" src/instance/app.cfg

    # search src/instance/app.cfg for the lines that start with MEMCACHED_MODE and replace with false
    sed -i "" "s|MEMCACHED_MODE = True|MEMCACHED_MODE = False|g" src/instance/app.cfg

    # search for UBKG values and replace with test values in environment variables
    sed -i "" "s|UBKG_SERVER =|UBKG_SERVER = '${UBKG_SERVER}'|g" src/instance/app.cfg
    sed -i "" "s|UBKG_ENDPOINT_VALUESET =|UBKG_ENDPOINT_VALUESET = '${UBKG_ENDPOINT_VALUESET}'|g" src/instance/app.cfg
    sed -i "" "s|UBKG_CODES =|UBKG_CODES = '${UBKG_CODES}'|g" src/instance/app.cfg
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sed -i "s|NEO4J_URI = 'bolt://hubmap-neo4j-localhost:7687'|NEO4J_URI = 'bolt://neo4j-test:7687'|g" src/instance/app.cfg
    sed -i "s|NEO4J_PASSWORD = '123'|NEO4J_PASSWORD = None|g" src/instance/app.cfg

    # search src/instance/app.cfg for the lines that start with MEMCACHED_MODE and replace with false
    sed -i "s|MEMCACHED_MODE = True|MEMCACHED_MODE = False|g" src/instance/app.cfg

    # search for UBKG values and replace with test values in environment variables
    sed -i "s|UBKG_SERVER =|UBKG_SERVER = '${UBKG_SERVER}'|g" src/instance/app.cfg
    sed -i "s|UBKG_ENDPOINT_VALUESET =|UBKG_ENDPOINT_VALUESET = '${UBKG_ENDPOINT_VALUESET}'|g" src/instance/app.cfg
    sed -i "s|UBKG_CODES =|UBKG_CODES = '${UBKG_CODES}'|g" src/instance/app.cfg
else
    echo "Unsupported OS"
    exit 1
fi

cp -r src/ docker/entity-api/src/

docker-compose -f docker/docker-compose.test.yml run --rm entity-api-test sh -c "pytest --disable-warnings"
# tear down the test containers
docker-compose -f docker/docker-compose.test.yml down

# if there's a src/instance/app.cfg.bak file, revert it to src/instance/app.cfg
if [ -f src/instance/app.cfg.bak ]; then
    echo "Restoring src/instance/app.cfg from src/instance/app.cfg.bak"
    mv src/instance/app.cfg.bak src/instance/app.cfg
fi

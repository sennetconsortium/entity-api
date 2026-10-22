# Same Neo4j version as run_tests.sh; only its distribution files are used
FROM neo4j:5.20.0-ubi8 AS neo4j

FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NEO4J_HOME=/opt/neo4j \
    PATH=/opt/neo4j/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless libnss-myhostname \
    && rm -rf /var/lib/apt/lists/*

COPY --from=neo4j /var/lib/neo4j /opt/neo4j

# Scanner expects the checkout directory at /src
COPY . /src
WORKDIR /src

RUN python -m pip install --no-cache-dir -r src/requirements.txt -r src/requirements.dev.txt \
    && mkdir -p src/instance \
    && cp test/config/app.test.cfg src/instance/app.cfg \
    && printf 'oss-scanner\n' > BUILD \
    && python -m compileall -q src test \
    && cp .oss-scanner/start-neo4j.sh /usr/local/bin/start-neo4j \
    && chmod 755 /usr/local/bin/start-neo4j \
    && cp /opt/neo4j/labs/apoc-*-core.jar /opt/neo4j/plugins/ \
    && printf '%s\n' \
        'dbms.security.auth_enabled=false' \
        'dbms.usage_report.enabled=false' \
        'dbms.security.procedures.unrestricted=apoc.*' \
        'server.bolt.listen_address=127.0.0.1:7687' \
        >> /opt/neo4j/conf/neo4j.conf \
    && rm -f /opt/neo4j/data /opt/neo4j/logs \
    && mkdir -p /opt/neo4j/data /opt/neo4j/logs

#!/bin/bash
# Starts the local Neo4j and waits until Bolt accepts connections.
set -e

neo4j start > /dev/null

for _ in $(seq 1 60); do
    if (exec 3<>/dev/tcp/127.0.0.1/7687) 2> /dev/null; then
        exit 0
    fi
    sleep 1
done

echo "Neo4j did not become ready; see /opt/neo4j/logs/neo4j.log" >&2
exit 1

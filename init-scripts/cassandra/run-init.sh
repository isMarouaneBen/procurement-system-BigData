#!/bin/bash
set -e

echo "Waiting for Cassandra to be ready..."

# Loop until Cassandra responds
for i in {1..60}; do
    if cqlsh cassandra1 -e "DESCRIBE CLUSTER" >/dev/null 2>&1; then
        echo "Cassandra is ready!"
        break
    fi
    echo "Cassandra not ready yet... waiting (attempt $i/60)"
    sleep 2
done

echo "Running init.cql..."
cqlsh cassandra1 -f /init-scripts/cassandra/init.cql
echo "Cassandra initialization done."
#!/bin/bash
# Start namenode in background
/run.sh &

# Wait for HDFS to be ready
sleep 30
while ! hdfs dfs -ls / > /dev/null 2>&1; do sleep 5; done

# Create directories if they don't exist
if ! hdfs dfs -test -d /procurement 2>/dev/null; then
    hdfs dfs -mkdir -p /procurement/raw/orders /procurement/raw/stock /procurement/raw/snapshots /procurement/processed/aggregated_orders /procurement/processed/net_demand /procurement/output/supplier_orders /procurement/logs/exceptions /procurement/logs/summaries
    hdfs dfs -chmod -R 777 /procurement
fi

wait
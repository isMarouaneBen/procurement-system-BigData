"""
Create Airflow connections for the procurement system
Run this after Airflow webserver/scheduler is running
"""

from airflow.models import Connection
from airflow.utils.db import merge_conn

# HDFS Connection
hdfs_conn = Connection(
    conn_id='hdfs_default',
    conn_type='hdfs',
    host='namenode',
    port=9000,
    extra='{"namenode_principal": "hdfs"}'
)
merge_conn(hdfs_conn)
print("✓ Created HDFS connection: hdfs_default")

# Cassandra Connection
cassandra_conn = Connection(
    conn_id='cassandra_default',
    conn_type='cassandra',
    host='cassandra1',
    port=9042,
    extra='{"cluster_name": "procurement_cassandra_cluster"}'
)
merge_conn(cassandra_conn)
print("✓ Created Cassandra connection: cassandra_default")

# Trino Connection
trino_conn = Connection(
    conn_id='trino_default',
    conn_type='trino',
    host='trino',
    port=8080,
    login='airflow',
    extra='{"catalog": "hive", "schema": "procurement"}'
)
merge_conn(trino_conn)
print("✓ Created Trino connection: trino_default")

# PostgreSQL Connection
postgres_conn = Connection(
    conn_id='postgres_default',
    conn_type='postgres',
    host='postgres',
    port=5432,
    login='admin',
    password='admin123',
    schema='postgres'
)
merge_conn(postgres_conn)
print("✓ Created PostgreSQL connection: postgres_default")

print("\n✓ All connections created successfully!")

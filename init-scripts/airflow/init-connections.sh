#!/bin/bash
# Initialize Airflow connections

echo "Waiting for Airflow webserver to be ready..."
sleep 10

echo "Creating Airflow connections..."
airflow python /init-scripts/airflow/create_connections.py

echo "✓ Airflow initialization complete"

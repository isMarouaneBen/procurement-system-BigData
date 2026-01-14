"""
Procurement System Daily ETL Pipeline DAG (Trino-based) - Sequential Execution with HDFS Storage

This DAG orchestrates the complete daily procurement data pipeline sequentially:
1. Load orders to HDFS
2. Load stock to HDFS
3. Store snapshots to Cassandra
4. Create Hive external tables for orders and stock
5. Aggregate orders using Trino (federated queries only) -> HDFS
6. Calculate net demand using Trino queries only -> HDFS
7. Generate supplier orders using Trino -> HDFS
8. Generate pipeline summary -> HDFS

Schedule: Daily at 23:00
Author: Procurement System Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.exceptions import AirflowException

import os
import json
import csv
import logging
from pathlib import Path
from typing import Dict
import traceback
import trino
from io import StringIO

# =============================================================================
# CONFIGURATION
# =============================================================================

DATE_FORMAT = "%d-%m-%Y"

# Base paths (local - for temporary processing)
BASE_DATA_PATH = "/opt/airflow/data"
RAW_PATH = f"{BASE_DATA_PATH}/raw"
TEMP_PATH = f"{BASE_DATA_PATH}/temp"

# HDFS paths
HDFS_BASE_PATH = "/procurement"
HDFS_RAW_PATH = f"{HDFS_BASE_PATH}/raw"
HDFS_PROCESSED_PATH = f"{HDFS_BASE_PATH}/processed"
HDFS_OUTPUT_PATH = f"{HDFS_BASE_PATH}/output"
HDFS_LOGS_PATH = f"{HDFS_BASE_PATH}/logs"

# Connection IDs
HDFS_CONN_ID = "hdfs_default"
CASSANDRA_CONN_ID = "cassandra_default"

# Trino connection settings
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "airflow"
TRINO_CATALOG_HIVE = "hive"
TRINO_CATALOG_POSTGRES = "postgresql"
TRINO_CATALOG_CASSANDRA = "cassandra"
TRINO_SCHEMA_HIVE = "procurement"
TRINO_SCHEMA_POSTGRES = "public"

# Cassandra settings
CASSANDRA_KEYSPACE = "procurement"
CASSANDRA_TABLE = "inventory_snapshots"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_trino_connection():
    """Create and return a Trino connection"""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG_HIVE,
        schema=TRINO_SCHEMA_HIVE
    )


def log_task_execution(task_name: str, execution_date: str, status: str, 
                       details: Dict = None, context: Dict = None) -> str:
    """Log task execution details to JSON file in HDFS"""
    try:
        # Create temp local file
        temp_dir = f"{TEMP_PATH}/logs/tasks/{execution_date}"
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_log_file = os.path.join(temp_dir, f"{task_name}_{timestamp}.json")
        
        log_data = {
            "task_name": task_name,
            "timestamp": timestamp,
            "execution_date": execution_date,
            "status": status,
            "details": details or {},
        }
        
        with open(temp_log_file, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_log_path = f"{HDFS_LOGS_PATH}/tasks/{execution_date}/{task_name}_{timestamp}.json"
        hdfs_hook.load_file(source=temp_log_file, destination=hdfs_log_path, overwrite=True)
        
        # Clean up temp file
        os.remove(temp_log_file)
        
        return hdfs_log_path
    except Exception as log_err:
        logging.warning(f"Failed to write task execution log: {log_err}")
        return ""


def log_exception(exception: Exception, task_name: str, execution_date: str, 
                  additional_info: Dict = None) -> str:
    """Log exception details to JSON file in HDFS and raise"""
    try:
        # Create temp local file
        temp_dir = f"{TEMP_PATH}/logs/exceptions/{execution_date}"
        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_exception_file = os.path.join(temp_dir, f"{task_name}_{timestamp}.json")
        
        exception_data = {
            "task_name": task_name,
            "timestamp": timestamp,
            "execution_date": execution_date,
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            "traceback": traceback.format_exc(),
            "additional_info": additional_info or {}
        }
        
        with open(temp_exception_file, 'w') as f:
            json.dump(exception_data, f, indent=2)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_exception_path = f"{HDFS_LOGS_PATH}/exceptions/{execution_date}/{task_name}_{timestamp}.json"
        hdfs_hook.load_file(source=temp_exception_file, destination=hdfs_exception_path, overwrite=True)
        
        # Clean up temp file
        os.remove(temp_exception_file)
        
        logging.error(f"Exception logged to HDFS: {hdfs_exception_path}")
    except Exception as log_err:
        logging.warning(f"Failed to write exception log: {log_err}")
    
    raise AirflowException(f"{task_name} failed: {str(exception)}")


# =============================================================================
# TASK 1: LOAD ORDERS TO HDFS
# =============================================================================

def load_orders_to_hdfs(**context):
    """Load orders from local directory to HDFS"""
    logging.info("Loading orders to HDFS...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    date_str_iso = execution_date.strftime("%Y-%m-%d")
    
    try:
        ti = context['ti']
        orders_file = f"{RAW_PATH}/orders/{date_str}/orders.csv"
        
        if not os.path.exists(orders_file):
            raise FileNotFoundError(f"Orders file not found at {orders_file}")
        
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_orders_path = f"{HDFS_RAW_PATH}/orders/{date_str}/orders.csv"
        
        hdfs_hook.load_file(source=orders_file, destination=hdfs_orders_path, overwrite=True)
        
        log_task_execution("load_orders_to_hdfs", date_str, "success", {
            "hdfs_path": hdfs_orders_path,
            "file_size": os.path.getsize(orders_file)
        }, context)
        
        ti.xcom_push(key='hdfs_orders_path', value=hdfs_orders_path)
        ti.xcom_push(key='date_str', value=date_str)
        ti.xcom_push(key='date_str_iso', value=date_str_iso)
        
        return {"status": "success", "hdfs_path": hdfs_orders_path}
        
    except Exception as e:
        log_exception(e, "load_orders_to_hdfs", date_str, {"stage": "hdfs_orders_upload"})


# =============================================================================
# TASK 2: LOAD STOCK TO HDFS
# =============================================================================

def load_stock_to_hdfs(**context):
    """Load stock from local directory to HDFS (convert JSON to CSV)"""
    logging.info("Loading stock to HDFS...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    
    try:
        ti = context['ti']
        stock_file = f"{RAW_PATH}/stock/{date_str}/stock.json"
        
        if not os.path.exists(stock_file):
            raise FileNotFoundError(f"Stock file not found at {stock_file}")
        
        # Convert JSON to CSV for Hive compatibility
        with open(stock_file, 'r') as f:
            stock_data = json.load(f)
        
        stock_csv_file = stock_file.replace('.json', '.csv')
        with open(stock_csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['warehouse_id', 'sku_id', 'current_stock'])
            writer.writeheader()
            writer.writerows(stock_data)
        
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_stock_path = f"{HDFS_RAW_PATH}/stock/{date_str}/stock.csv"
        
        hdfs_hook.load_file(source=stock_csv_file, destination=hdfs_stock_path, overwrite=True)
        
        log_task_execution("load_stock_to_hdfs", date_str, "success", {
            "hdfs_path": hdfs_stock_path,
            "record_count": len(stock_data)
        }, context)
        
        ti.xcom_push(key='hdfs_stock_path', value=hdfs_stock_path)
        return {"status": "success", "hdfs_path": hdfs_stock_path}
        
    except Exception as e:
        log_exception(e, "load_stock_to_hdfs", date_str, {"stage": "hdfs_stock_upload"})


# =============================================================================
# TASK 3: STORE SNAPSHOTS TO CASSANDRA
# =============================================================================

def store_snapshots_to_cassandra(**context):
    """Store inventory snapshots to Cassandra"""
    logging.info("Storing snapshots to Cassandra...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    
    try:
        ti = context['ti']
        snapshots_file = f"{RAW_PATH}/snapshots/{date_str}/snapshot.json"
        
        if not os.path.exists(snapshots_file):
            raise FileNotFoundError(f"Snapshots file not found: {snapshots_file}")
        
        with open(snapshots_file, 'r') as f:
            snapshots = json.load(f)
        
        cassandra_hook = CassandraHook(cassandra_conn_id=CASSANDRA_CONN_ID)
        session = cassandra_hook.get_conn()
        
        insert_query = f"""
            INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} 
            (sku_code, snapshot_date, warehouse_code, available_qty, reserved_qty)
            VALUES (?, ?, ?, ?, ?)
        """
        prepared_stmt = session.prepare(insert_query)
        
        for snapshot in snapshots:
            session.execute(prepared_stmt, [
                snapshot['sku_code'],
                snapshot['snapshot_date'],
                snapshot['warehouse_code'],
                snapshot['available_qty'],
                snapshot['reserved_qty']
            ])
        
        log_task_execution("store_snapshots_to_cassandra", date_str, "success", {
            "inserted_count": len(snapshots)
        }, context)
        
        ti.xcom_push(key='cassandra_inserted_count', value=len(snapshots))
        return {"status": "success", "inserted_count": len(snapshots)}
        
    except Exception as e:
        log_exception(e, "store_snapshots_to_cassandra", date_str, {"stage": "cassandra_insert"})


# =============================================================================
# TASK 4: CREATE HIVE TABLES
# =============================================================================

def create_hive_tables(**context):
    """Create Hive external tables for orders and stock data pointing to HDFS"""
    logging.info("Creating Hive tables...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    
    try:
        ti = context['ti']
        
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        # Create schema
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}")
        
        # Drop and recreate tables
        cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders")
        cursor.execute(f"DROP TABLE IF EXISTS {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.stock")
        
        # Create orders table
        orders_hdfs_dir = f"hdfs://namenode:9000{HDFS_RAW_PATH}/orders/{date_str}"
        cursor.execute(f"""
            CREATE TABLE {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders (
                order_id VARCHAR,
                supplier_id VARCHAR,
                sku_id VARCHAR,
                quantity VARCHAR,
                warehouse_id VARCHAR,
                order_date VARCHAR
            )
            WITH (
                format = 'CSV',
                external_location = '{orders_hdfs_dir}',
                skip_header_line_count = 1
            )
        """)
        
        # Create stock table
        stock_hdfs_dir = f"hdfs://namenode:9000{HDFS_RAW_PATH}/stock/{date_str}"
        cursor.execute(f"""
            CREATE TABLE {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.stock (
                warehouse_id VARCHAR,
                sku_id VARCHAR,
                current_stock VARCHAR
            )
            WITH (
                format = 'CSV',
                external_location = '{stock_hdfs_dir}',
                skip_header_line_count = 1
            )
        """)
        
        # Verify row counts
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders")
        orders_count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.stock")
        stock_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        log_task_execution("create_hive_tables", date_str, "success", {
            "orders_count": orders_count,
            "stock_count": stock_count
        }, context)
        
        ti.xcom_push(key='orders_count', value=orders_count)
        ti.xcom_push(key='stock_count', value=stock_count)
        ti.xcom_push(key='date_str', value=date_str)
        ti.xcom_push(key='date_str_iso', value=execution_date.strftime("%Y-%m-%d"))
        
        return {"status": "success", "orders_count": orders_count, "stock_count": stock_count}
        
    except Exception as e:
        log_exception(e, "create_hive_tables", date_str, {"stage": "hive_table_creation"})


# =============================================================================
# TASK 5: AGGREGATE ORDERS USING TRINO -> HDFS
# =============================================================================

def aggregate_orders_with_trino(**context):
    """Aggregate orders using Trino federated queries and store to HDFS"""
    logging.info("Aggregating orders using Trino...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    
    try:
        ti = context['ti']
        
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        aggregate_sql = f"""
            SELECT 
                CAST(o.sku_id AS BIGINT) as sku_id,
                p.sku_code,
                p.name AS product_name,
                p.category,
                CAST(o.warehouse_id AS BIGINT) as warehouse_id,
                w.warehouse_code,
                w.name AS warehouse_name,
                w.city,
                SUM(CAST(o.quantity AS BIGINT)) AS total_quantity,
                COUNT(*) AS order_count,
                MAX(o.order_date) AS order_date
            FROM {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders o
            JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.products p ON CAST(o.sku_id AS BIGINT) = p.sku_id
            JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.warehouses w ON CAST(o.warehouse_id AS BIGINT) = w.warehouse_id
            GROUP BY CAST(o.sku_id AS BIGINT), p.sku_code, p.name, p.category, CAST(o.warehouse_id AS BIGINT), w.warehouse_code, w.name, w.city
            ORDER BY total_quantity DESC
        """
        
        cursor.execute(aggregate_sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        aggregated_orders = [dict(zip(columns, row)) for row in rows]
        
        cursor.close()
        conn.close()
        
        # Save to temporary local files
        temp_dir = Path(f"{TEMP_PATH}/aggregated_orders/{date_str}")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        temp_json = temp_dir / "aggregated_orders.json"
        with open(temp_json, 'w') as f:
            json.dump(aggregated_orders, f, indent=2, default=str)
        
        temp_csv = temp_dir / "aggregated_orders.csv"
        if aggregated_orders:
            with open(temp_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=aggregated_orders[0].keys())
                writer.writeheader()
                writer.writerows(aggregated_orders)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_json_path = f"{HDFS_PROCESSED_PATH}/aggregated_orders/{date_str}/aggregated_orders.json"
        hdfs_csv_path = f"{HDFS_PROCESSED_PATH}/aggregated_orders/{date_str}/aggregated_orders.csv"
        
        hdfs_hook.load_file(source=str(temp_json), destination=hdfs_json_path, overwrite=True)
        hdfs_hook.load_file(source=str(temp_csv), destination=hdfs_csv_path, overwrite=True)
        
        log_task_execution("aggregate_orders_with_trino", date_str, "success", {
            "aggregated_count": len(aggregated_orders),
            "hdfs_json_path": hdfs_json_path,
            "hdfs_csv_path": hdfs_csv_path
        }, context)
        
        ti.xcom_push(key='aggregated_orders_file', value=hdfs_json_path)
        ti.xcom_push(key='aggregated_orders_count', value=len(aggregated_orders))
        
        return {"status": "success", "aggregated_count": len(aggregated_orders)}
        
    except Exception as e:
        log_exception(e, "aggregate_orders_with_trino", date_str, {"stage": "aggregation"})


# =============================================================================
# TASK 6: CALCULATE NET DEMAND USING TRINO -> HDFS
# =============================================================================

def calculate_net_demand_with_trino(**context):
    """Calculate net demand using Trino federated queries and store to HDFS"""
    logging.info("Calculating net demand using Trino...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    date_str_iso = execution_date.strftime("%Y-%m-%d")
    
    try:
        ti = context['ti']
        
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        net_demand_sql = f"""
            WITH aggregated_orders AS (
                SELECT 
                    CAST(o.sku_id AS BIGINT) as sku_id, p.sku_code, p.name AS product_name, p.category,
                    CAST(o.warehouse_id AS BIGINT) as warehouse_id, w.warehouse_code, w.name AS warehouse_name, w.city,
                    SUM(CAST(o.quantity AS BIGINT)) AS total_quantity
                FROM {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders o
                JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.products p ON CAST(o.sku_id AS BIGINT) = p.sku_id
                JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.warehouses w ON CAST(o.warehouse_id AS BIGINT) = w.warehouse_id
                GROUP BY CAST(o.sku_id AS BIGINT), p.sku_code, p.name, p.category, CAST(o.warehouse_id AS BIGINT), w.warehouse_code, w.name, w.city
            ),
            safety_stock_combined AS (
                SELECT 
                    COALESCE(ssw.sku_id, ss.sku_id) AS sku_id,
                    COALESCE(ssw.warehouse_id, w.warehouse_id) AS warehouse_id,
                    COALESCE(ssw.safety_stock_qty, ss.safety_stock_qty, 0) AS safety_stock_qty
                FROM {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.safety_stock ss
                CROSS JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.warehouses w
                LEFT JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.safety_stock_by_warehouse ssw
                    ON ss.sku_id = ssw.sku_id AND w.warehouse_id = ssw.warehouse_id
            ),
            inventory_data AS (
                SELECT sku_code, warehouse_code, available_qty, reserved_qty
                FROM {TRINO_CATALOG_CASSANDRA}.{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
                WHERE snapshot_date = DATE '{date_str_iso}'
            )
            SELECT 
                ao.sku_id, ao.sku_code, ao.product_name, ao.category,
                ao.warehouse_id, ao.warehouse_code, ao.warehouse_name, ao.city,
                ao.total_quantity AS aggregated_orders,
                COALESCE(ss.safety_stock_qty, 0) AS safety_stock,
                COALESCE(inv.available_qty, 0) AS available_stock,
                COALESCE(inv.reserved_qty, 0) AS reserved_stock,
                COALESCE(inv.available_qty, 0) - COALESCE(inv.reserved_qty, 0) AS effective_stock,
                GREATEST(0, 
                    ao.total_quantity + COALESCE(ss.safety_stock_qty, 0) 
                    - (COALESCE(inv.available_qty, 0) - COALESCE(inv.reserved_qty, 0))
                ) AS net_demand
            FROM aggregated_orders ao
            LEFT JOIN safety_stock_combined ss ON ao.sku_id = ss.sku_id AND ao.warehouse_id = ss.warehouse_id
            LEFT JOIN inventory_data inv ON ao.sku_code = inv.sku_code AND ao.warehouse_code = inv.warehouse_code
            ORDER BY net_demand DESC
        """
        
        cursor.execute(net_demand_sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        net_demand_results = [dict(zip(columns, row)) for row in rows]
        
        for result in net_demand_results:
            result['calculation_date'] = date_str
        
        cursor.close()
        conn.close()
        
        # Save to temporary local files
        temp_dir = Path(f"{TEMP_PATH}/net_demand/{date_str}")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        temp_json = temp_dir / "net_demand.json"
        with open(temp_json, 'w') as f:
            json.dump(net_demand_results, f, indent=2, default=str)
        
        temp_csv = temp_dir / "net_demand.csv"
        if net_demand_results:
            with open(temp_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=net_demand_results[0].keys())
                writer.writeheader()
                writer.writerows(net_demand_results)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_json_path = f"{HDFS_PROCESSED_PATH}/net_demand/{date_str}/net_demand.json"
        hdfs_csv_path = f"{HDFS_PROCESSED_PATH}/net_demand/{date_str}/net_demand.csv"
        
        hdfs_hook.load_file(source=str(temp_json), destination=hdfs_json_path, overwrite=True)
        hdfs_hook.load_file(source=str(temp_csv), destination=hdfs_csv_path, overwrite=True)
        
        total_net_demand = sum(r['net_demand'] for r in net_demand_results)
        items_with_demand = len([r for r in net_demand_results if r['net_demand'] > 0])
        
        log_task_execution("calculate_net_demand_with_trino", date_str, "success", {
            "total_combinations": len(net_demand_results),
            "items_with_demand": items_with_demand,
            "total_net_demand": total_net_demand,
            "hdfs_json_path": hdfs_json_path,
            "hdfs_csv_path": hdfs_csv_path
        }, context)
        
        ti.xcom_push(key='net_demand_file', value=hdfs_json_path)
        ti.xcom_push(key='net_demand_count', value=len(net_demand_results))
        ti.xcom_push(key='total_net_demand', value=total_net_demand)
        ti.xcom_push(key='items_with_demand', value=items_with_demand)
        
        return {"status": "success", "net_demand_items": len(net_demand_results)}
        
    except Exception as e:
        log_exception(e, "calculate_net_demand_with_trino", date_str, {"stage": "net_demand_calculation"})


# =============================================================================
# TASK 7: GENERATE SUPPLIER ORDERS USING TRINO -> HDFS
# =============================================================================

def generate_supplier_orders_with_trino(**context):
    """Generate supplier orders using Trino federated queries and store to HDFS"""
    logging.info("Generating supplier orders using Trino...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    date_str_iso = execution_date.strftime("%Y-%m-%d")
    
    try:
        ti = context['ti']
        
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        supplier_orders_sql = f"""
            WITH aggregated_orders AS (
                SELECT 
                    CAST(o.sku_id AS BIGINT) as sku_id, p.sku_code, p.name AS product_name, p.category,
                    CAST(o.warehouse_id AS BIGINT) as warehouse_id, w.warehouse_code, w.name AS warehouse_name, w.city,
                    SUM(CAST(o.quantity AS BIGINT)) AS total_quantity
                FROM {TRINO_CATALOG_HIVE}.{TRINO_SCHEMA_HIVE}.orders o
                JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.products p ON CAST(o.sku_id AS BIGINT) = p.sku_id
                JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.warehouses w ON CAST(o.warehouse_id AS BIGINT) = w.warehouse_id
                GROUP BY CAST(o.sku_id AS BIGINT), p.sku_code, p.name, p.category, CAST(o.warehouse_id AS BIGINT), w.warehouse_code, w.name, w.city
            ),
            safety_stock_combined AS (
                SELECT 
                    COALESCE(ssw.sku_id, ss.sku_id) AS sku_id,
                    COALESCE(ssw.warehouse_id, w.warehouse_id) AS warehouse_id,
                    COALESCE(ssw.safety_stock_qty, ss.safety_stock_qty, 0) AS safety_stock_qty
                FROM {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.safety_stock ss
                CROSS JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.warehouses w
                LEFT JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.safety_stock_by_warehouse ssw
                    ON ss.sku_id = ssw.sku_id AND w.warehouse_id = ssw.warehouse_id
            ),
            inventory_data AS (
                SELECT sku_code, warehouse_code, available_qty, reserved_qty
                FROM {TRINO_CATALOG_CASSANDRA}.{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
                WHERE snapshot_date = DATE '{date_str_iso}'
            ),
            net_demand_calc AS (
                SELECT 
                    ao.sku_id, ao.sku_code, ao.product_name, ao.category,
                    ao.warehouse_id, ao.warehouse_code, ao.warehouse_name, ao.city,
                    GREATEST(0, 
                        ao.total_quantity + COALESCE(ss.safety_stock_qty, 0) 
                        - (COALESCE(inv.available_qty, 0) - COALESCE(inv.reserved_qty, 0))
                    ) AS net_demand
                FROM aggregated_orders ao
                LEFT JOIN safety_stock_combined ss ON ao.sku_id = ss.sku_id AND ao.warehouse_id = ss.warehouse_id
                LEFT JOIN inventory_data inv ON ao.sku_code = inv.sku_code AND ao.warehouse_code = inv.warehouse_code
            ),
            ranked_suppliers AS (
                SELECT 
                    sp.supplier_id, s.supplier_code, s.name AS supplier_name, sp.sku_id,
                    sp.pack_size, sp.min_order_qty, sp.lead_time_days, sp.unit_price, sp.currency,
                    ROW_NUMBER() OVER (PARTITION BY sp.sku_id ORDER BY sp.unit_price ASC) AS price_rank
                FROM {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.supplier_products sp
                JOIN {TRINO_CATALOG_POSTGRES}.{TRINO_SCHEMA_POSTGRES}.suppliers s ON sp.supplier_id = s.supplier_id
                WHERE sp.is_active = TRUE AND s.is_active = TRUE
            )
            SELECT 
                nd.sku_id, nd.sku_code, nd.product_name, nd.category,
                nd.warehouse_id, nd.warehouse_code, nd.warehouse_name, nd.city,
                rs.supplier_id, rs.supplier_code, rs.supplier_name,
                nd.net_demand, rs.pack_size, rs.min_order_qty, rs.unit_price, rs.currency, rs.lead_time_days,
                GREATEST(rs.min_order_qty, CEILING(CAST(nd.net_demand AS DOUBLE) / rs.pack_size) * rs.pack_size) AS order_quantity,
                GREATEST(rs.min_order_qty, CEILING(CAST(nd.net_demand AS DOUBLE) / rs.pack_size) * rs.pack_size) * rs.unit_price AS total_cost,
                DATE_ADD('day', rs.lead_time_days, DATE '{date_str_iso}') AS expected_delivery_date
            FROM net_demand_calc nd
            JOIN ranked_suppliers rs ON nd.sku_id = rs.sku_id AND rs.price_rank = 1
            WHERE nd.net_demand > 0
            ORDER BY total_cost DESC
        """
        
        cursor.execute(supplier_orders_sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        supplier_orders = []
        for i, row in enumerate(rows, 1):
            order_dict = dict(zip(columns, row))
            order_dict['order_id'] = f"PO-{date_str_iso.replace('-', '')}-{i:05d}"
            order_dict['order_date'] = date_str_iso
            order_dict['status'] = 'PENDING'
            supplier_orders.append(order_dict)
        
        cursor.close()
        conn.close()
        
        # Save to temporary local files
        temp_dir = Path(f"{TEMP_PATH}/supplier_orders/{date_str}")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        temp_json = temp_dir / "supplier_orders.json"
        with open(temp_json, 'w') as f:
            json.dump(supplier_orders, f, indent=2, default=str)
        
        temp_csv = temp_dir / "supplier_orders.csv"
        if supplier_orders:
            with open(temp_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=supplier_orders[0].keys())
                writer.writeheader()
                writer.writerows(supplier_orders)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_json_path = f"{HDFS_OUTPUT_PATH}/supplier_orders/{date_str}/supplier_orders.json"
        hdfs_csv_path = f"{HDFS_OUTPUT_PATH}/supplier_orders/{date_str}/supplier_orders.csv"
        
        hdfs_hook.load_file(source=str(temp_json), destination=hdfs_json_path, overwrite=True)
        hdfs_hook.load_file(source=str(temp_csv), destination=hdfs_csv_path, overwrite=True)
        
        total_cost = sum(float(o['total_cost']) for o in supplier_orders)
        
        log_task_execution("generate_supplier_orders_with_trino", date_str, "success", {
            "orders_generated": len(supplier_orders),
            "total_cost": total_cost,
            "hdfs_json_path": hdfs_json_path,
            "hdfs_csv_path": hdfs_csv_path
        }, context)
        
        ti.xcom_push(key='supplier_orders_file', value=hdfs_json_path)
        ti.xcom_push(key='supplier_orders_count', value=len(supplier_orders))
        ti.xcom_push(key='total_procurement_cost', value=total_cost)
        
        return {"status": "success", "orders_generated": len(supplier_orders), "total_cost": total_cost}
        
    except Exception as e:
        log_exception(e, "generate_supplier_orders_with_trino", date_str, {"stage": "supplier_order_generation"})


# =============================================================================
# TASK 8: PIPELINE SUMMARY -> HDFS
# =============================================================================

def generate_pipeline_summary(**context):
    """Generate comprehensive pipeline execution summary and store to HDFS"""
    logging.info("Generating pipeline summary...")
    
    execution_date = context.get('execution_date') or context.get('logical_date') or datetime.now()
    if hasattr(execution_date, 'to_pydatetime'):
        execution_date = execution_date.to_pydatetime()
    
    date_str = execution_date.strftime(DATE_FORMAT)
    
    try:
        ti = context['ti']
        
        summary = {
            "execution_date": date_str,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "completed",
            "hive_tables": {
                "orders_count": ti.xcom_pull(task_ids='create_hive_tables', key='orders_count') or 0,
                "stock_count": ti.xcom_pull(task_ids='create_hive_tables', key='stock_count') or 0
            },
            "cassandra": {
                "snapshots_inserted": ti.xcom_pull(task_ids='store_snapshots', key='cassandra_inserted_count') or 0
            },
            "aggregation": {
                "combinations": ti.xcom_pull(task_ids='aggregate_orders', key='aggregated_orders_count') or 0
            },
            "net_demand": {
                "combinations": ti.xcom_pull(task_ids='calculate_net_demand', key='net_demand_count') or 0,
                "items_with_demand": ti.xcom_pull(task_ids='calculate_net_demand', key='items_with_demand') or 0,
                "total_quantity": ti.xcom_pull(task_ids='calculate_net_demand', key='total_net_demand') or 0
            },
            "supplier_orders": {
                "orders_generated": ti.xcom_pull(task_ids='generate_supplier_orders', key='supplier_orders_count') or 0,
                "total_cost": ti.xcom_pull(task_ids='generate_supplier_orders', key='total_procurement_cost') or 0
            }
        }
        
        # Save to temporary local file
        temp_dir = Path(f"{TEMP_PATH}/pipeline_summary")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        temp_summary = temp_dir / f"summary_{date_str}.json"
        with open(temp_summary, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Upload to HDFS
        hdfs_hook = WebHDFSHook(webhdfs_conn_id=HDFS_CONN_ID)
        hdfs_summary_path = f"{HDFS_LOGS_PATH}/summaries/summary_{date_str}.json"
        hdfs_hook.load_file(source=str(temp_summary), destination=hdfs_summary_path, overwrite=True)
        
        logging.info("=" * 70)
        logging.info("PIPELINE SUMMARY")
        logging.info("=" * 70)
        logging.info(f"Orders in Hive: {summary['hive_tables']['orders_count']}")
        logging.info(f"Supplier Orders: {summary['supplier_orders']['orders_generated']}")
        logging.info(f"Total Cost: {summary['supplier_orders']['total_cost']:,.2f} MAD")
        logging.info(f"Summary stored at: {hdfs_summary_path}")
        logging.info("=" * 70)
        
        return {"status": "success", "summary_file": hdfs_summary_path}
        
    except Exception as e:
        log_exception(e, "generate_pipeline_summary", date_str, {"stage": "summary_generation"})


# =============================================================================
# DAG DEFINITION - SEQUENTIAL EXECUTION
# =============================================================================

default_args = {
    'owner': 'procurement_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='procurement_daily_pipeline_sequential',
    default_args=default_args,
    description='Daily procurement ETL pipeline (Sequential execution): Load → HDFS/Cassandra → Hive → Trino → Supplier orders (all to HDFS)',
    schedule_interval='0 23 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['procurement', 'etl', 'trino', 'hive', 'sequential', 'hdfs'],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    
    # Task 1: Load orders to HDFS
    load_orders_task = PythonOperator(
        task_id='load_orders',
        python_callable=load_orders_to_hdfs,
        provide_context=True
    )
    
    # Task 2: Load stock to HDFS (runs after load_orders)
    load_stock_task = PythonOperator(
        task_id='load_stock',
        python_callable=load_stock_to_hdfs,
        provide_context=True
    )
    
    # Task 3: Store snapshots to Cassandra (runs after load_stock)
    store_snapshots_task = PythonOperator(
        task_id='store_snapshots',
        python_callable=store_snapshots_to_cassandra,
        provide_context=True
    )
    
    # Task 4: Create Hive tables (runs after store_snapshots)
    create_hive_tables_task = PythonOperator(
        task_id='create_hive_tables',
        python_callable=create_hive_tables,
        provide_context=True
    )
    
    # Task 5: Aggregate orders (runs after create_hive_tables)
    aggregate_orders_task = PythonOperator(
        task_id='aggregate_orders',
        python_callable=aggregate_orders_with_trino,
        provide_context=True
    )
    
    # Task 6: Calculate net demand (runs after aggregate_orders)
    calculate_net_demand_task = PythonOperator(
        task_id='calculate_net_demand',
        python_callable=calculate_net_demand_with_trino,
        provide_context=True
    )
    
    # Task 7: Generate supplier orders (runs after calculate_net_demand)
    generate_supplier_orders_task = PythonOperator(
        task_id='generate_supplier_orders',
        python_callable=generate_supplier_orders_with_trino,
        provide_context=True
    )
    
    # Task 8: Generate summary (runs after generate_supplier_orders)
    generate_summary_task = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_pipeline_summary,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    # Define sequential execution: task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8
    load_orders_task >> load_stock_task >> store_snapshots_task >> create_hive_tables_task
    create_hive_tables_task >> aggregate_orders_task >> calculate_net_demand_task
    calculate_net_demand_task >> generate_supplier_orders_task >> generate_summary_task
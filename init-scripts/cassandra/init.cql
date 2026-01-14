CREATE KEYSPACE IF NOT EXISTS procurement
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

CREATE TABLE IF NOT EXISTS procurement.inventory_snapshots (
  sku_code        text,        -- SKU identifier
  snapshot_date   date,        -- daily snapshot date
  warehouse_code  text,        -- warehouse ID

  available_qty   int,         -- available stock
  reserved_qty    int,         -- reserved stock

  PRIMARY KEY ((sku_code), snapshot_date, warehouse_code)
)
WITH CLUSTERING ORDER BY (snapshot_date DESC, warehouse_code ASC);
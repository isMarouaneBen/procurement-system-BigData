import csv
import json
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
NUM_ORDERS = 1000
TODAY = datetime.now()
BASE_PATH = Path(os.environ.get("DATA_PATH", "/opt/airflow/data"))
OUTPUT_DIR = BASE_PATH / "raw/orders" / TODAY.strftime("%d-%m-%Y")
OUTPUT_FILE = OUTPUT_DIR / "orders.csv"

# Master data based on schema analysis
SUPPLIERS = list(range(1, 36))  # 35 suppliers (supplier_id 1-35)
WAREHOUSES = list(range(1, 11))  # 10 warehouses (warehouse_id 1-10)

# Supplier-Product relationships with procurement rules
# Format: (supplier_id, sku_id, pack_size, min_order_qty, lead_time_days, unit_price)
SUPPLIER_PRODUCTS = [
    (1, 1, 10, 5, 2, 45.00), (1, 2, 20, 10, 1, 8.50), (1, 10, 15, 5, 3, 12.00),
    (2, 3, 1, 1, 2, 89.99), (2, 4, 5, 2, 3, 15.50), (2, 5, 1, 1, 4, 45.00),
    (3, 6, 1, 1, 5, 899.00), (3, 7, 1, 1, 4, 120.00), (3, 8, 1, 1, 7, 650.00),
    (4, 11, 24, 6, 2, 5.50), (4, 12, 50, 10, 2, 3.20),
    (5, 13, 1, 1, 10, 5500.00), (5, 14, 50, 20, 3, 22.50),
    (6, 15, 100, 50, 2, 8.75), (6, 16, 1, 1, 2, 189.99),
    (7, 17, 10, 5, 3, 18.00), (7, 18, 1, 1, 4, 78.50),
    (8, 19, 1, 1, 7, 1250.00), (8, 20, 1, 1, 10, 3200.00),
    (9, 21, 1, 1, 8, 1850.00), (9, 22, 5, 2, 1, 42.00),
    (10, 23, 1, 1, 3, 89.50), (10, 24, 1, 1, 5, 2150.00),
    (11, 25, 40, 10, 1, 3.75), (11, 26, 1, 1, 6, 450.00),
    (12, 27, 1, 1, 8, 890.00), (12, 28, 1, 1, 3, 125.00),
    (13, 29, 10, 5, 2, 52.00), (13, 30, 8, 4, 2, 38.50),
    (14, 31, 20, 10, 4, 125.00), (14, 32, 20, 10, 4, 98.50),
    (15, 33, 25, 10, 5, 15.75), (15, 34, 1, 1, 6, 85.00),
    (16, 35, 1, 1, 14, 4500.00), (16, 36, 1, 1, 12, 3200.00),
    (17, 39, 1, 1, 7, 1200.00), (17, 6, 1, 1, 5, 899.00),
    (18, 2, 20, 10, 1, 8.50), (18, 1, 10, 5, 2, 45.00),
    (19, 37, 1, 1, 10, 750.00), (19, 38, 1, 1, 8, 285.00),
    (20, 4, 5, 2, 3, 15.50), (20, 5, 1, 1, 4, 45.00),
    (21, 3, 1, 1, 2, 89.99), (21, 39, 1, 1, 7, 1200.00),
    (22, 40, 10, 5, 3, 22.50), (22, 11, 24, 6, 2, 5.50),
    (23, 19, 1, 1, 7, 1250.00), (23, 20, 1, 1, 10, 3200.00),
    (24, 14, 50, 20, 3, 22.50), (24, 15, 100, 50, 2, 8.75),
    (25, 7, 1, 1, 4, 120.00), (25, 8, 1, 1, 7, 650.00),
    (26, 9, 1, 1, 8, 580.00), (26, 12, 50, 10, 2, 3.20),
    (27, 35, 1, 1, 14, 4500.00), (27, 36, 1, 1, 12, 3200.00),
    (28, 34, 1, 1, 6, 85.00), (28, 33, 25, 10, 5, 15.75),
    (29, 31, 20, 10, 4, 125.00), (29, 32, 20, 10, 4, 98.50),
    (30, 1, 10, 5, 2, 45.00), (30, 2, 20, 10, 1, 8.50),
    (31, 16, 1, 1, 2, 189.99), (31, 28, 1, 1, 3, 125.00),
    (32, 18, 1, 1, 4, 78.50), (32, 17, 10, 5, 3, 18.00),
    (33, 6, 1, 1, 5, 899.00), (33, 39, 1, 1, 7, 1200.00),
    (34, 9, 1, 1, 8, 580.00), (34, 7, 1, 1, 4, 120.00),
    (35, 40, 10, 5, 3, 22.50), (35, 10, 15, 5, 3, 12.00),
]

# Product categories for realistic ordering patterns
HIGH_VOLUME_SKUS = [1, 2, 10, 11, 14, 15, 25]  # Office supplies, packaging
MEDIUM_VOLUME_SKUS = [3, 4, 5, 17, 22, 29, 30, 40]  # IT, cleaning, electrical
LOW_VOLUME_SKUS = [6, 8, 13, 19, 20, 35, 36]  # Expensive equipment

def generate_order_id(index):
    """Generate unique order ID"""
    return f"ORD-{TODAY.strftime('%Y%m%d')}-{index:05d}"

def calculate_quantity(sku_id, pack_size, min_order_qty):
    """Calculate realistic order quantity based on SKU type and constraints"""
    if sku_id in HIGH_VOLUME_SKUS:
        multiplier = random.randint(2, 10)
    elif sku_id in MEDIUM_VOLUME_SKUS:
        multiplier = random.randint(1, 5)
    else:
        multiplier = random.choice([1, 1, 1, 2, 2, 3])
    
    base_qty = max(min_order_qty, multiplier * min_order_qty)
    
    if random.random() < 0.8:
        packs = max(1, base_qty // pack_size)
        return packs * pack_size
    
    return base_qty

def generate_orders():
    """Generate realistic orders based on schema"""
    orders = []
    
    for i in range(1, NUM_ORDERS + 1):
        supplier_id, sku_id, pack_size, min_order_qty, lead_time_days, unit_price = random.choice(SUPPLIER_PRODUCTS)
        
        order_id = generate_order_id(i)
        warehouse_id = random.choice(WAREHOUSES)
        quantity = calculate_quantity(sku_id, pack_size, min_order_qty)
        
        order = {
            'order_id': order_id,
            'supplier_id': supplier_id,
            'sku_id': sku_id,
            'quantity': quantity,
            'warehouse_id': warehouse_id,
            'order_date': TODAY.strftime('%Y-%m-%d')
        }
        
        orders.append(order)
    
    return orders

def save_orders_to_csv(orders):
    """Save orders to CSV file"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    fieldnames = ['order_id', 'supplier_id', 'sku_id', 'quantity', 'warehouse_id', 'order_date']
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✓ Generated {len(orders)} orders")
    print(f"✓ Saved to: {OUTPUT_FILE}")


# ========== STOCK GENERATION ==========

STOCK_OUTPUT_DIR = BASE_PATH / "raw/stock" / TODAY.strftime("%d-%m-%Y")
STOCK_OUTPUT_FILE = STOCK_OUTPUT_DIR / "stock.json"

STOCK_WAREHOUSES = list(range(1, 11))
STOCK_SKUS = list(range(1, 41))

WAREHOUSE_SAFETY_STOCK = {
    1: {1: 100, 2: 150, 3: 50, 5: 30, 8: 5, 10: 200, 14: 150, 17: 100, 19: 5, 25: 300},
    2: {1: 75, 4: 60, 6: 8, 11: 300, 15: 250, 18: 25, 22: 80, 29: 70, 30: 40},
    3: {2: 120, 7: 15, 9: 3, 12: 200, 16: 15, 20: 2, 23: 8, 28: 15},
    4: {3: 35, 5: 25, 13: 2, 24: 3, 27: 3, 31: 150, 32: 120},
    5: {1: 80, 4: 55, 10: 180, 14: 120, 26: 5, 33: 300, 34: 12},
    6: {3: 60, 6: 10, 11: 250, 35: 2, 36: 2, 37: 75, 38: 8},
    7: {2: 100, 7: 12, 17: 80, 21: 2, 25: 250},
    8: {4: 50, 8: 4, 39: 4, 40: 120},
    9: {9: 4, 12: 180, 19: 3, 20: 1},
    10: {1: 60, 23: 6, 28: 12, 29: 60, 30: 35}
}

GLOBAL_SAFETY_STOCK = {
    1: 50, 2: 100, 3: 25, 4: 50, 5: 20, 6: 5, 7: 10, 8: 3, 9: 2, 10: 100,
    11: 200, 12: 150, 13: 1, 14: 100, 15: 200, 16: 10, 17: 50, 18: 15, 19: 2, 20: 1,
    21: 1, 22: 50, 23: 5, 24: 2, 25: 200, 26: 3, 27: 2, 28: 10, 29: 50, 30: 30,
    31: 100, 32: 100, 33: 250, 34: 10, 35: 1, 36: 1, 37: 50, 38: 5, 39: 3, 40: 100
}

def generate_current_stock(warehouse_id, sku_id):
    """Generate realistic current stock level"""
    safety_stock = WAREHOUSE_SAFETY_STOCK.get(warehouse_id, {}).get(sku_id, 
                   GLOBAL_SAFETY_STOCK.get(sku_id, 10))
    
    multiplier = random.choice([
        0, 0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0
    ])
    
    current = int(safety_stock * multiplier)
    return max(0, current)

def generate_stock():
    """Generate stock data for all warehouse-SKU combinations"""
    stock_data = []
    
    for warehouse_id in STOCK_WAREHOUSES:
        for sku_id in STOCK_SKUS:
            if sku_id in WAREHOUSE_SAFETY_STOCK.get(warehouse_id, {}) or random.random() < 0.3:
                current_stock = generate_current_stock(warehouse_id, sku_id)
                
                stock_entry = {
                    'warehouse_id': warehouse_id,
                    'sku_id': sku_id,
                    'current_stock': current_stock
                }
                
                stock_data.append(stock_entry)
    
    return stock_data

def save_stock_to_json(stock_data):
    """Save stock data to JSON file"""
    STOCK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(STOCK_OUTPUT_FILE, 'w', encoding='utf-8') as jsonfile:
        json.dump(stock_data, jsonfile, indent=2)
    
    print(f"✓ Generated {len(stock_data)} stock records")
    print(f"✓ Saved to: {STOCK_OUTPUT_FILE}")


# ========== SNAPSHOT GENERATION ==========

SNAPSHOT_OUTPUT_DIR = BASE_PATH / "raw/snapshots" / TODAY.strftime("%d-%m-%Y")
SNAPSHOT_OUTPUT_FILE = SNAPSHOT_OUTPUT_DIR / "snapshot.json"

SKU_CODES = [
    'PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005', 'PROD006', 'PROD007', 'PROD008',
    'PROD009', 'PROD010', 'PROD011', 'PROD012', 'PROD013', 'PROD014', 'PROD015', 'PROD016',
    'PROD017', 'PROD018', 'PROD019', 'PROD020', 'PROD021', 'PROD022', 'PROD023', 'PROD024',
    'PROD025', 'PROD026', 'PROD027', 'PROD028', 'PROD029', 'PROD030', 'PROD031', 'PROD032',
    'PROD033', 'PROD034', 'PROD035', 'PROD036', 'PROD037', 'PROD038', 'PROD039', 'PROD040'
]

WAREHOUSE_CODES = [
    'WH001', 'WH002', 'WH003', 'WH004', 'WH005',
    'WH006', 'WH007', 'WH008', 'WH009', 'WH010'
]

SNAPSHOT_SAFETY_STOCK = {
    'PROD001': 50, 'PROD002': 100, 'PROD003': 25, 'PROD004': 50, 'PROD005': 20,
    'PROD006': 5, 'PROD007': 10, 'PROD008': 3, 'PROD009': 2, 'PROD010': 100,
    'PROD011': 200, 'PROD012': 150, 'PROD013': 1, 'PROD014': 100, 'PROD015': 200,
    'PROD016': 10, 'PROD017': 50, 'PROD018': 15, 'PROD019': 2, 'PROD020': 1,
    'PROD021': 1, 'PROD022': 50, 'PROD023': 5, 'PROD024': 2, 'PROD025': 200,
    'PROD026': 3, 'PROD027': 2, 'PROD028': 10, 'PROD029': 50, 'PROD030': 30,
    'PROD031': 100, 'PROD032': 100, 'PROD033': 250, 'PROD034': 10, 'PROD035': 1,
    'PROD036': 1, 'PROD037': 50, 'PROD038': 5, 'PROD039': 3, 'PROD040': 100
}

def generate_available_qty(sku_code):
    """Generate realistic available quantity"""
    safety_stock = SNAPSHOT_SAFETY_STOCK.get(sku_code, 10)
    
    multiplier = random.choice([
        0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 4.0
    ])
    
    return max(0, int(safety_stock * multiplier))

def generate_reserved_qty(available_qty):
    """Generate realistic reserved quantity"""
    if available_qty == 0:
        return 0
    
    reserved_pct = random.choice([
        0, 0, 0, 0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30
    ])
    
    return int(available_qty * reserved_pct)

def generate_snapshots():
    """Generate inventory snapshots for all SKU-warehouse combinations"""
    snapshots = []
    snapshot_date = TODAY.strftime('%Y-%m-%d')
    
    for sku_code in SKU_CODES:
        for warehouse_code in WAREHOUSE_CODES:
            if random.random() < 0.7:
                available_qty = generate_available_qty(sku_code)
                reserved_qty = generate_reserved_qty(available_qty)
                
                snapshot = {
                    'sku_code': sku_code,
                    'snapshot_date': snapshot_date,
                    'warehouse_code': warehouse_code,
                    'available_qty': available_qty,
                    'reserved_qty': reserved_qty
                }
                
                snapshots.append(snapshot)
    
    return snapshots

def save_snapshots_to_json(snapshots):
    """Save snapshots to JSON file"""
    SNAPSHOT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(SNAPSHOT_OUTPUT_FILE, 'w', encoding='utf-8') as jsonfile:
        json.dump(snapshots, jsonfile, indent=2)
    
    print(f"✓ Generated {len(snapshots)} inventory snapshots")
    print(f"✓ Saved to: {SNAPSHOT_OUTPUT_FILE}")


def main():
    """Main execution"""
    print(f"Generating procurement data for {TODAY.strftime('%d-%m-%Y')}...\n")
    
    print(f"[1/3] Generating {NUM_ORDERS} orders...")
    orders = generate_orders()
    save_orders_to_csv(orders)
    
    print(f"\n[2/3] Generating stock data...")
    stock_data = generate_stock()
    save_stock_to_json(stock_data)
    
    print(f"\n[3/3] Generating inventory snapshots...")
    snapshots = generate_snapshots()
    save_snapshots_to_json(snapshots)
    
    print(f"\n✓ Data generation completed successfully!")

if __name__ == "__main__":
    main()
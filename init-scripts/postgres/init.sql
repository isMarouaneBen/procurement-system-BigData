CREATE DATABASE airflow;
CREATE DATABASE hive_metastore;
CREATE DATABASE procurement_system;
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO admin;
GRANT ALL PRIVILEGES ON DATABASE procurement_system TO admin;
\c procurement_system;
-- 1) Suppliers
CREATE TABLE IF NOT EXISTS suppliers (
  supplier_id      BIGSERIAL PRIMARY KEY,
  supplier_code    TEXT UNIQUE NOT NULL,
  name             TEXT NOT NULL,
  email            TEXT,
  phone            TEXT,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2) Products (SKUs)
CREATE TABLE IF NOT EXISTS products (
  sku_id           BIGSERIAL PRIMARY KEY,
  sku_code         TEXT UNIQUE NOT NULL,
  name             TEXT NOT NULL,
  category         TEXT,
  uom              TEXT NOT NULL DEFAULT 'unit',
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) Warehouses
CREATE TABLE IF NOT EXISTS warehouses (
  warehouse_id     BIGSERIAL PRIMARY KEY,
  warehouse_code   TEXT UNIQUE NOT NULL,
  name             TEXT NOT NULL,
  city             TEXT,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE
);

-- 4) Supplier-Product mapping + procurement rules
CREATE TABLE IF NOT EXISTS supplier_products (
  supplier_id      BIGINT NOT NULL REFERENCES suppliers(supplier_id),
  sku_id           BIGINT NOT NULL REFERENCES products(sku_id),

  pack_size        INT NOT NULL CHECK (pack_size > 0),
  min_order_qty    INT NOT NULL CHECK (min_order_qty >= 0),
  lead_time_days   INT NOT NULL CHECK (lead_time_days >= 0),

  unit_price       NUMERIC(12,4) CHECK (unit_price >= 0),
  currency         TEXT NOT NULL DEFAULT 'MAD',

  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (supplier_id, sku_id)
);

-- 5) Safety stock rules (global per SKU)
CREATE TABLE IF NOT EXISTS safety_stock (
  sku_id           BIGINT PRIMARY KEY REFERENCES products(sku_id),
  safety_stock_qty INT NOT NULL CHECK (safety_stock_qty >= 0),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6) Safety stock per warehouse
CREATE TABLE IF NOT EXISTS safety_stock_by_warehouse (
  warehouse_id     BIGINT NOT NULL REFERENCES warehouses(warehouse_id),
  sku_id           BIGINT NOT NULL REFERENCES products(sku_id),
  safety_stock_qty INT NOT NULL CHECK (safety_stock_qty >= 0),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (warehouse_id, sku_id)
);

-- ============================================
-- SAMPLE DATA FOR MOROCCAN PROCUREMENT SYSTEM
-- ============================================

-- INSERT INTO suppliers (around 35 suppliers)
INSERT INTO suppliers (supplier_code, name, email, phone, is_active) VALUES
('SUP001', 'Maroc Logistique SA', 'contact@maroclog.ma', '+212 5 22 123 456', TRUE),
('SUP002', 'Atlas Trade Distribution', 'info@atlastrade.ma', '+212 5 37 654 321', TRUE),
('SUP003', 'Casablanca Supply Co', 'supply@casa-supply.ma', '+212 5 22 456 789', TRUE),
('SUP004', 'Fès Import Export', 'trade@fes-import.ma', '+212 5 35 789 123', TRUE),
('SUP005', 'Tangier Port Logistics', 'logistics@tanger-port.ma', '+212 5 39 234 567', TRUE),
('SUP006', 'Marrakech Wholesale Ltd', 'wholesale@marrakech-wh.ma', '+212 5 24 567 890', TRUE),
('SUP007', 'Agadir Commerce Group', 'commerce@agadir-group.ma', '+212 5 28 123 456', TRUE),
('SUP008', 'Rabat Distribution Center', 'dist@rabat-center.ma', '+212 5 37 456 789', TRUE),
('SUP009', 'Meknes Supply Partners', 'partners@meknes-supply.ma', '+212 5 35 123 456', TRUE),
('SUP010', 'Oujda Import Trading', 'trading@oujda-import.ma', '+212 5 36 789 012', TRUE),
('SUP011', 'Hassan II Port Suppliers', 'supply@hassan2-port.ma', '+212 5 22 789 456', TRUE),
('SUP012', 'Casablanca Industrial Goods', 'industrial@casa-ind.ma', '+212 5 22 234 567', TRUE),
('SUP013', 'Tangier Free Zone Traders', 'trade@tangier-fz.ma', '+212 5 39 567 890', TRUE),
('SUP014', 'Fès Commercial Hub', 'hub@fes-commercial.ma', '+212 5 35 456 123', TRUE),
('SUP015', 'Marrakech Manufacturing Co', 'mfg@marrakech-mfg.ma', '+212 5 24 234 567', TRUE),
('SUP016', 'Agadir Maritime Suppliers', 'maritime@agadir-sea.ma', '+212 5 28 456 789', TRUE),
('SUP017', 'Rabat Tech Procurement', 'tech@rabat-tech.ma', '+212 5 37 234 567', TRUE),
('SUP018', 'Meknes Agricultural Supply', 'agri@meknes-agri.ma', '+212 5 35 234 567', TRUE),
('SUP019', 'Oujda Industrial Park Co', 'industrial@oujda-park.ma', '+212 5 36 234 567', TRUE),
('SUP020', 'Casablanca Distribution Hub', 'hub@casa-dist.ma', '+212 5 22 567 890', TRUE),
('SUP021', 'Tangier Electronics Suppliers', 'electronics@tangier-elec.ma', '+212 5 39 123 456', TRUE),
('SUP022', 'Fès Textile Traders', 'textiles@fes-textile.ma', '+212 5 35 345 678', TRUE),
('SUP023', 'Marrakech Food Distributors', 'food@marrakech-food.ma', '+212 5 24 345 678', TRUE),
('SUP024', 'Agadir Fishing Supplies Co', 'fishing@agadir-fish.ma', '+212 5 28 234 567', TRUE),
('SUP025', 'Rabat Packaging Solutions', 'packaging@rabat-pack.ma', '+212 5 37 567 890', TRUE),
('SUP026', 'Meknes Construction Materials', 'materials@meknes-const.ma', '+212 5 35 567 890', TRUE),
('SUP027', 'Oujda Machinery Imports', 'machinery@oujda-mach.ma', '+212 5 36 456 789', TRUE),
('SUP028', 'Casablanca Spare Parts Inc', 'parts@casa-parts.ma', '+212 5 22 345 678', TRUE),
('SUP029', 'Tangier Chemicals Trading', 'chemicals@tangier-chem.ma', '+212 5 39 789 012', TRUE),
('SUP030', 'Fès Office Supplies Ltd', 'office@fes-office.ma', '+212 5 35 678 901', TRUE),
('SUP031', 'Marrakech Medical Supplies', 'medical@marrakech-med.ma', '+212 5 24 123 456', TRUE),
('SUP032', 'Agadir Energy Solutions', 'energy@agadir-energy.ma', '+212 5 28 789 012', TRUE),
('SUP033', 'Rabat IT Equipment Co', 'it@rabat-it.ma', '+212 5 37 345 678', TRUE),
('SUP034', 'Meknes Furniture Trading', 'furniture@meknes-furn.ma', '+212 5 35 890 123', TRUE),
('SUP035', 'Oujda Print & Label Services', 'print@oujda-print.ma', '+212 5 36 567 890', TRUE);

-- INSERT INTO products (around 40 products - realistic procurement items)
INSERT INTO products (sku_code, name, category, uom, is_active) VALUES
('PROD001', 'Office Paper A4 (500 sheets)', 'Office Supplies', 'ream', TRUE),
('PROD002', 'Ballpoint Pens (box of 50)', 'Office Supplies', 'box', TRUE),
('PROD003', 'Printer Ink Cartridge (Black)', 'IT Equipment', 'unit', TRUE),
('PROD004', 'USB Cable 2m', 'IT Equipment', 'unit', TRUE),
('PROD005', 'Wireless Mouse', 'IT Equipment', 'unit', TRUE),
('PROD006', 'LCD Monitor 24 inch', 'IT Equipment', 'unit', TRUE),
('PROD007', 'Laptop Stand Aluminum', 'Office Furniture', 'unit', TRUE),
('PROD008', 'Office Chair Ergonomic', 'Office Furniture', 'unit', TRUE),
('PROD009', 'Metal Shelving Unit 4-tier', 'Warehouse Equipment', 'unit', TRUE),
('PROD010', 'Cardboard Boxes (Medium)', 'Packaging', 'case', TRUE),
('PROD011', 'Packing Tape (50mm)', 'Packaging', 'roll', TRUE),
('PROD012', 'Bubble Wrap 500mm', 'Packaging', 'meter', TRUE),
('PROD013', 'Forklift Pallet Jack', 'Warehouse Equipment', 'unit', TRUE),
('PROD014', 'Safety Helmet Yellow', 'Safety Equipment', 'unit', TRUE),
('PROD015', 'Safety Gloves (Nitrile)', 'Safety Equipment', 'pair', TRUE),
('PROD016', 'First Aid Kit Standard', 'Safety Equipment', 'unit', TRUE),
('PROD017', 'Fluorescent Light Bulbs', 'Lighting', 'unit', TRUE),
('PROD018', 'LED Desk Lamp', 'Lighting', 'unit', TRUE),
('PROD019', 'Coffee Machine 10-cup', 'Kitchen Equipment', 'unit', TRUE),
('PROD020', 'Refrigerator 500L', 'Kitchen Equipment', 'unit', TRUE),
('PROD021', 'Stainless Steel Sink', 'Kitchen Equipment', 'unit', TRUE),
('PROD022', 'Hand Sanitizer 5L', 'Cleaning Supplies', 'liter', TRUE),
('PROD023', 'Floor Cleaning Mop', 'Cleaning Supplies', 'unit', TRUE),
('PROD024', 'Industrial Vacuum Cleaner', 'Cleaning Supplies', 'unit', TRUE),
('PROD025', 'Toilet Paper (bulk roll)', 'Cleaning Supplies', 'case', TRUE),
('PROD026', 'Door Lock Digital', 'Security', 'unit', TRUE),
('PROD027', 'CCTV Camera 1080p', 'Security', 'unit', TRUE),
('PROD028', 'Fire Extinguisher 2kg', 'Safety Equipment', 'unit', TRUE),
('PROD029', 'Extension Power Strip 6-outlet', 'Electrical', 'unit', TRUE),
('PROD030', 'Surge Protector Power Bar', 'Electrical', 'unit', TRUE),
('PROD031', 'Industrial Hydraulic Fluid', 'Maintenance', 'liter', TRUE),
('PROD032', 'Machine Oil ISO 46', 'Maintenance', 'liter', TRUE),
('PROD033', 'Welding Electrode Mild Steel', 'Maintenance', 'kg', TRUE),
('PROD034', 'Bearing Roller Stainless Steel', 'Maintenance', 'unit', TRUE),
('PROD035', 'Pump Centrifugal 1HP', 'Machinery', 'unit', TRUE),
('PROD036', 'Electric Motor 3HP', 'Machinery', 'unit', TRUE),
('PROD037', 'Industrial Belt Conveyor', 'Machinery', 'meter', TRUE),
('PROD038', 'Pneumatic Cylinder Actuator', 'Machinery', 'unit', TRUE),
('PROD039', 'PLC Control Module', 'IT Equipment', 'unit', TRUE),
('PROD040', 'Thermal Transfer Label', 'Packaging', 'case', TRUE);

-- INSERT INTO warehouses (Moroccan cities)
INSERT INTO warehouses (warehouse_code, name, city, is_active) VALUES
('WH001', 'Casablanca Main Hub', 'Casablanca', TRUE),
('WH002', 'Rabat Distribution Center', 'Rabat', TRUE),
('WH003', 'Fès Logistics Hub', 'Fès', TRUE),
('WH004', 'Marrakech Distribution', 'Marrakech', TRUE),
('WH005', 'Agadir Port Terminal', 'Agadir', TRUE),
('WH006', 'Tangier Free Zone', 'Tangier', TRUE),
('WH007', 'Meknes Distribution Point', 'Meknes', TRUE),
('WH008', 'Oujda Border Warehouse', 'Oujda', TRUE),
('WH009', 'Safi Industrial Park', 'Safi', TRUE),
('WH010', 'El Jadida Storage Facility', 'El Jadida', TRUE);

-- INSERT INTO supplier_products (Supplier-Product relationships with pricing)
INSERT INTO supplier_products (supplier_id, sku_id, pack_size, min_order_qty, lead_time_days, unit_price, currency, is_active) VALUES
-- SUP001 products
(1, 1, 10, 5, 2, 45.00, 'MAD', TRUE),
(1, 2, 20, 10, 1, 8.50, 'MAD', TRUE),
(1, 10, 15, 5, 3, 12.00, 'MAD', TRUE),
-- SUP002 products
(2, 3, 1, 1, 2, 89.99, 'MAD', TRUE),
(2, 4, 5, 2, 3, 15.50, 'MAD', TRUE),
(2, 5, 1, 1, 4, 45.00, 'MAD', TRUE),
-- SUP003 products
(3, 6, 1, 1, 5, 899.00, 'MAD', TRUE),
(3, 7, 1, 1, 4, 120.00, 'MAD', TRUE),
(3, 8, 1, 1, 7, 650.00, 'MAD', TRUE),
-- SUP004 products
(4, 11, 24, 6, 2, 5.50, 'MAD', TRUE),
(4, 12, 50, 10, 2, 3.20, 'MAD', TRUE),
-- SUP005 products
(5, 13, 1, 1, 10, 5500.00, 'MAD', TRUE),
(5, 14, 50, 20, 3, 22.50, 'MAD', TRUE),
-- SUP006 products
(6, 15, 100, 50, 2, 8.75, 'MAD', TRUE),
(6, 16, 1, 1, 2, 189.99, 'MAD', TRUE),
-- SUP007 products
(7, 17, 10, 5, 3, 18.00, 'MAD', TRUE),
(7, 18, 1, 1, 4, 78.50, 'MAD', TRUE),
-- SUP008 products
(8, 19, 1, 1, 7, 1250.00, 'MAD', TRUE),
(8, 20, 1, 1, 10, 3200.00, 'MAD', TRUE),
-- SUP009 products
(9, 21, 1, 1, 8, 1850.00, 'MAD', TRUE),
(9, 22, 5, 2, 1, 42.00, 'MAD', TRUE),
-- SUP010 products
(10, 23, 1, 1, 3, 89.50, 'MAD', TRUE),
(10, 24, 1, 1, 5, 2150.00, 'MAD', TRUE),
-- SUP011 products
(11, 25, 40, 10, 1, 3.75, 'MAD', TRUE),
(11, 26, 1, 1, 6, 450.00, 'MAD', TRUE),
-- SUP012 products
(12, 27, 1, 1, 8, 890.00, 'MAD', TRUE),
(12, 28, 1, 1, 3, 125.00, 'MAD', TRUE),
-- SUP013 products
(13, 29, 10, 5, 2, 52.00, 'MAD', TRUE),
(13, 30, 8, 4, 2, 38.50, 'MAD', TRUE),
-- SUP014 products
(14, 31, 20, 10, 4, 125.00, 'MAD', TRUE),
(14, 32, 20, 10, 4, 98.50, 'MAD', TRUE),
-- SUP015 products
(15, 33, 25, 10, 5, 15.75, 'MAD', TRUE),
(15, 34, 1, 1, 6, 85.00, 'MAD', TRUE),
-- SUP016 products
(16, 35, 1, 1, 14, 4500.00, 'MAD', TRUE),
(16, 36, 1, 1, 12, 3200.00, 'MAD', TRUE),
-- SUP017 products
(17, 39, 1, 1, 7, 1200.00, 'MAD', TRUE),
(17, 6, 1, 1, 5, 899.00, 'MAD', TRUE),
-- SUP018 products
(18, 2, 20, 10, 1, 8.50, 'MAD', TRUE),
(18, 1, 10, 5, 2, 45.00, 'MAD', TRUE),
-- SUP019 products
(19, 37, 1, 1, 10, 750.00, 'MAD', TRUE),
(19, 38, 1, 1, 8, 285.00, 'MAD', TRUE),
-- SUP020 products
(20, 4, 5, 2, 3, 15.50, 'MAD', TRUE),
(20, 5, 1, 1, 4, 45.00, 'MAD', TRUE),
-- SUP021 products
(21, 3, 1, 1, 2, 89.99, 'MAD', TRUE),
(21, 39, 1, 1, 7, 1200.00, 'MAD', TRUE),
-- SUP022 products
(22, 40, 10, 5, 3, 22.50, 'MAD', TRUE),
(22, 11, 24, 6, 2, 5.50, 'MAD', TRUE),
-- SUP023 products
(23, 19, 1, 1, 7, 1250.00, 'MAD', TRUE),
(23, 20, 1, 1, 10, 3200.00, 'MAD', TRUE),
-- SUP024 products
(24, 14, 50, 20, 3, 22.50, 'MAD', TRUE),
(24, 15, 100, 50, 2, 8.75, 'MAD', TRUE),
-- SUP025 products
(25, 7, 1, 1, 4, 120.00, 'MAD', TRUE),
(25, 8, 1, 1, 7, 650.00, 'MAD', TRUE),
-- SUP026 products
(26, 9, 1, 1, 8, 580.00, 'MAD', TRUE),
(26, 12, 50, 10, 2, 3.20, 'MAD', TRUE),
-- SUP027 products
(27, 35, 1, 1, 14, 4500.00, 'MAD', TRUE),
(27, 36, 1, 1, 12, 3200.00, 'MAD', TRUE),
-- SUP028 products
(28, 34, 1, 1, 6, 85.00, 'MAD', TRUE),
(28, 33, 25, 10, 5, 15.75, 'MAD', TRUE),
-- SUP029 products
(29, 31, 20, 10, 4, 125.00, 'MAD', TRUE),
(29, 32, 20, 10, 4, 98.50, 'MAD', TRUE),
-- SUP030 products
(30, 1, 10, 5, 2, 45.00, 'MAD', TRUE),
(30, 2, 20, 10, 1, 8.50, 'MAD', TRUE),
-- SUP031 products
(31, 16, 1, 1, 2, 189.99, 'MAD', TRUE),
(31, 28, 1, 1, 3, 125.00, 'MAD', TRUE),
-- SUP032 products
(32, 18, 1, 1, 4, 78.50, 'MAD', TRUE),
(32, 17, 10, 5, 3, 18.00, 'MAD', TRUE),
-- SUP033 products
(33, 6, 1, 1, 5, 899.00, 'MAD', TRUE),
(33, 39, 1, 1, 7, 1200.00, 'MAD', TRUE),
-- SUP034 products
(34, 9, 1, 1, 8, 580.00, 'MAD', TRUE),
(34, 7, 1, 1, 4, 120.00, 'MAD', TRUE),
-- SUP035 products
(35, 40, 10, 5, 3, 22.50, 'MAD', TRUE),
(35, 10, 15, 5, 3, 12.00, 'MAD', TRUE);

-- INSERT INTO safety_stock (global safety stock by SKU)
INSERT INTO safety_stock (sku_id, safety_stock_qty, updated_at) VALUES
(1, 50, NOW()),
(2, 100, NOW()),
(3, 25, NOW()),
(4, 50, NOW()),
(5, 20, NOW()),
(6, 5, NOW()),
(7, 10, NOW()),
(8, 3, NOW()),
(9, 2, NOW()),
(10, 100, NOW()),
(11, 200, NOW()),
(12, 150, NOW()),
(13, 1, NOW()),
(14, 100, NOW()),
(15, 200, NOW()),
(16, 10, NOW()),
(17, 50, NOW()),
(18, 15, NOW()),
(19, 2, NOW()),
(20, 1, NOW()),
(21, 1, NOW()),
(22, 50, NOW()),
(23, 5, NOW()),
(24, 2, NOW()),
(25, 200, NOW()),
(26, 3, NOW()),
(27, 2, NOW()),
(28, 10, NOW()),
(29, 50, NOW()),
(30, 30, NOW()),
(31, 100, NOW()),
(32, 100, NOW()),
(33, 250, NOW()),
(34, 10, NOW()),
(35, 1, NOW()),
(36, 1, NOW()),
(37, 50, NOW()),
(38, 5, NOW()),
(39, 3, NOW()),
(40, 100, NOW());

-- INSERT INTO safety_stock_by_warehouse (warehouse-specific safety stock)
INSERT INTO safety_stock_by_warehouse (warehouse_id, sku_id, safety_stock_qty, updated_at) VALUES
-- Casablanca Main Hub (WH001) - Main distribution center
(1, 1, 100, NOW()), (1, 2, 150, NOW()), (1, 3, 50, NOW()), (1, 5, 30, NOW()), (1, 8, 5, NOW()),
(1, 10, 200, NOW()), (1, 14, 150, NOW()), (1, 17, 100, NOW()), (1, 19, 5, NOW()), (1, 25, 300, NOW()),
-- Rabat Distribution Center (WH002)
(2, 1, 75, NOW()), (2, 4, 60, NOW()), (2, 6, 8, NOW()), (2, 11, 300, NOW()), (2, 15, 250, NOW()),
(2, 18, 25, NOW()), (2, 22, 80, NOW()), (2, 29, 70, NOW()), (2, 30, 40, NOW()),
-- Fès Logistics Hub (WH003)
(3, 2, 120, NOW()), (3, 7, 15, NOW()), (3, 9, 3, NOW()), (3, 12, 200, NOW()), (3, 16, 15, NOW()),
(3, 20, 2, NOW()), (3, 23, 8, NOW()), (3, 28, 15, NOW()),
-- Marrakech Distribution (WH004)
(4, 3, 35, NOW()), (4, 5, 25, NOW()), (4, 13, 2, NOW()), (4, 24, 3, NOW()), (4, 27, 3, NOW()),
(4, 31, 150, NOW()), (4, 32, 120, NOW()),
-- Agadir Port Terminal (WH005)
(5, 1, 80, NOW()), (5, 4, 55, NOW()), (5, 10, 180, NOW()), (5, 14, 120, NOW()), (5, 26, 5, NOW()),
(5, 33, 300, NOW()), (5, 34, 12, NOW()),
-- Tangier Free Zone (WH006)
(6, 3, 60, NOW()), (6, 6, 10, NOW()), (6, 11, 250, NOW()), (6, 35, 2, NOW()), (6, 36, 2, NOW()),
(6, 37, 75, NOW()), (6, 38, 8, NOW()),
-- Meknes Distribution Point (WH007)
(7, 2, 100, NOW()), (7, 7, 12, NOW()), (7, 17, 80, NOW()), (7, 21, 2, NOW()), (7, 25, 250, NOW()),
-- Oujda Border Warehouse (WH008)
(8, 4, 50, NOW()), (8, 8, 4, NOW()), (8, 39, 4, NOW()), (8, 40, 120, NOW()),
-- Safi Industrial Park (WH009)
(9, 9, 4, NOW()), (9, 12, 180, NOW()), (9, 19, 3, NOW()), (9, 20, 1, NOW()),
-- El Jadida Storage Facility (WH010)
(10, 1, 60, NOW()), (10, 23, 6, NOW()), (10, 28, 12, NOW()), (10, 29, 60, NOW()), (10, 30, 35, NOW());
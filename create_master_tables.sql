-- Create and use the database
CREATE DATABASE IF NOT EXISTS metro_dw;
USE metro_dw;

-- Drop tables if they exist
DROP TABLE IF EXISTS CUSTOMERS, PRODUCTS;

-- Create CUSTOMERS table
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(100),
    GENDER VARCHAR(10)
);

-- Create PRODUCTS table
CREATE TABLE PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100),
    PRODUCT_PRICE DECIMAL(10,2),
    SUPPLIER_ID INT,
    SUPPLIER_NAME VARCHAR(100),
    STORE_ID INT,
    STORE_NAME VARCHAR(100)
);

-- Enable local infile loading
SET GLOBAL local_infile=1;

-- Load CUSTOMERS data
LOAD DATA LOCAL INFILE '/home/talha/UNI_DATA/Data_Warehouse/project/customers_data.csv'
INTO TABLE CUSTOMERS
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load PRODUCTS data (with price formatting)
LOAD DATA LOCAL INFILE '/home/talha/UNI_DATA/Data_Warehouse/project/products_data.csv'
INTO TABLE PRODUCTS
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(PRODUCT_ID, PRODUCT_NAME, @product_price, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME)
SET PRODUCT_PRICE = REPLACE(REPLACE(@product_price, '$', ''), ' ', '');

-- Verify the data
SELECT 'Customer Count:' as '', COUNT(*) FROM CUSTOMERS;
SELECT 'Product Count:' as '', COUNT(*) FROM PRODUCTS;

-- Show sample data
SELECT 'Sample Customers:' as '';
SELECT * FROM CUSTOMERS LIMIT 5;

SELECT 'Sample Products:' as '';
SELECT * FROM PRODUCTS LIMIT 5;

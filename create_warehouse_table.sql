USE metro_dw;

-- Drop the warehouse table if it exists
DROP TABLE IF EXISTS WAREHOUSE_SALES;

-- Create the warehouse table that will store transformed data
CREATE TABLE WAREHOUSE_SALES (
    ORDER_ID INT PRIMARY KEY,
    ORDER_DATE DATE,
    PRODUCT_ID INT,
    PRODUCT_NAME VARCHAR(100),
    UNIT_PRICE DECIMAL(10,2),
    QUANTITY INT,
    CUSTOMER_ID INT,
    CUSTOMER_FIRST_NAME VARCHAR(50),
    CUSTOMER_LAST_NAME VARCHAR(50),
    CUSTOMER_EMAIL VARCHAR(100),
    SUPPLIER_ID INT,
    SUPPLIER_NAME VARCHAR(100),
    STORE_NAME VARCHAR(100),
    SALE DECIMAL(10,2),
    TRANSACTION_ID VARCHAR(20),
    FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);
USE metro_dw;

DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    order_id INT PRIMARY KEY,
    order_date DATETIME,
    product_id INT,
    quantity INT,
    customer_id INT,
    time_id INT
);

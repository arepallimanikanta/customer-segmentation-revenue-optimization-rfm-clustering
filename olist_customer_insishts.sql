select count(*) from customers;
select count(*) from orders_dataset;
select count(*) from orders_payment;
drop table customers;
drop table orders_payment;
show tables from olist_customer_insishts_db;
-- customer table 
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;




SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';
CREATE TABLE orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status,
 @order_purchase_timestamp,
 @order_approved_at,
 @order_delivered_carrier_date,
 @order_delivered_customer_date,
 @order_estimated_delivery_date)
SET
order_purchase_timestamp = NULLIF(@order_purchase_timestamp, ''),
order_approved_at = NULLIF(@order_approved_at, ''),
order_delivered_carrier_date = NULLIF(@order_delivered_carrier_date, ''),
order_delivered_customer_date = NULLIF(@order_delivered_customer_date, ''),
order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date, '');
select count(*) from orders;

-- payment table 
CREATE TABLE payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value FLOAT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- order_items
CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price FLOAT,
    freight_value FLOAT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, order_item_id, product_id, seller_id,
 @shipping_limit_date, price, freight_value)
SET
shipping_limit_date = NULLIF(@shipping_limit_date, '');
-- count of all table rows

SELECT table_name, table_rows 
FROM information_schema.tables 
WHERE table_schema = DATABASE();
select * from order_items;
-- joining two tables 
select 
c.customer_id,o.order_id,oi.price,p.payment_value from customers c
join orders o on c. customer_id=o.customer_id
join order_items oi on o.order_id=oi.order_id 
join payments p on o.order_id=p.order_id
limit 10;
-- revenue per customer 
select 
o.customer_id,sum(p.payment_value) as total_spent
from orders o
join payments p on o.order_id =p.order_id
group by o.customer_id
order by total_spent desc
limit 10;
select * from orders;
-- create rfm base tabel 
select o.customer_id,max(o.order_purchase_timestamp) as last_purchase_date,
count(distinct o.order_id) as frequency ,
sum(p.payment_value)as monetary from orders o
join payments p on o.order_id=p.order_id 
group by o.customer_id;
-- calculate recenecy
SELECT 
    customer_id,
    DATEDIFF(
        (SELECT MAX(order_purchase_timestamp) FROM orders), 
        MAX(order_purchase_timestamp)
    ) AS recency,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(payment_value) AS monetary
FROM orders o
JOIN payments p 
    ON o.order_id = p.order_id 
GROUP BY customer_id;
-- segment customers
select *, case
when monetary > 1000 and frequenecy > 5 then 'VIP'
when monetary > 500 then 'high value'
when frequenecy > 3 then 'frequent'
else 'low value'
end as customer_segment 
from (select customer_id,datediff((select max(order_purchase_timestamp) from orders),
max(order_purchase_timestamp)) as recency,
count(distinct o.order_id) as frequenecy,
sum(payment_value) as monetary
from orders o 
join payments p on o.order_id=p.order_id
group by customer_id 

) t ;
-- business insights query
SELECT 
    customer_segment,
    COUNT(*) AS num_customers,
    SUM(monetary) AS total_revenue
FROM (
    SELECT *,
    CASE
        WHEN monetary > 1000 AND frequency > 5 THEN 'VIP'
        WHEN monetary > 500 THEN 'High Value'
        WHEN frequency > 3 THEN 'Frequent'
        ELSE 'Low Value'
    END AS customer_segment
    FROM (
        SELECT 
            customer_id,
            COUNT(DISTINCT o.order_id) AS frequency,
            SUM(payment_value) AS monetary
        FROM orders o
        JOIN payments p ON o.order_id = p.order_id
        GROUP BY customer_id
    ) t
) final
GROUP BY customer_segment;


SELECT 
    customer_id,
    DATEDIFF(
        (SELECT MAX(order_purchase_timestamp) FROM orders),
        MAX(order_purchase_timestamp)
    ) AS recency,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(payment_value) AS monetary
FROM orders o
JOIN payments p ON o.order_id = p.order_id
GROUP BY customer_id;
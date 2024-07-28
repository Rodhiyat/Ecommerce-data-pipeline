
-- Create schema
CREATE SCHEMA IF NOT EXISTS e_commerce;


-- create and populate tables
-- create customers table
create table if not exists e_commerce.customers
(
    customer_id uuid primary key,
    customer_unique_id uuid,
    customer_zip_code_prefix int
    customer_city varchar,
    customer_state varchar(2)    
);
--provide the command to copy the customers data in the /data folder into e_commerce.customers
COPY e_commerce.customers (customer_id, customer_unique_id,	customer_zip_code_prefix,	customer_city,	customer_state
)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup geolocation table 
create table if not exists e_commerce.geolocation_data
(
    geolocation_zip_code_prefix int,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city varchar,
    geolocation_state varchar(2)
);
--provide the command to copy the geolocation data in the /data folder into e_commerce.geolocation_data
COPY e_commerce.geolocation_data(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,	geolocation_city, geolocation_state
)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the order_items table 
create table if not exists e_commerce.order_items
(
    order_id UUID NOT NULL,
    order_item_id INT NOT NULL,
    product_id UUID NOT NULL,
    seller_id uuid NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC NOT NULL,
    freight_value NUMERIC NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);
-- provide the command to copy orders data into POSTGRES
COPY e_commerce.order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
)
FROM '/data/olist_orders_items_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the order_payments table 
create table if not exists e_commerce.order_payments
(
    order_id UUID NOT NULL,
    payment_sequential INT,
    payment_type VARCHAR(32),
    payment_installments INT,
    payment_value NUMERIC,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)    
);
-- provide the command to copy order payments data into POSTGRES
COPY e_commerce.order_payments (order_id, payment_sequential, payment_type, payment_installments,payment_value
)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the order reviews 
create table if not exists e_commerce.order_reviews
(
    review_id VARCHAR(32),
    order_id VARCHAR(32),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);
-- provide the command to copy order review into POSTGRES
COPY e_commerce.order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the orders table 
create table if not exists e_commerce.orders
(
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
-- provide the command to copy orders data into POSTGRES
COPY e_commerce.orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the products table 
create table if not exists e_commerce.products
(
    product_id STRING UUID PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT    
);
-- provide the command to copy products data into POSTGRES
COPY e_commerce.products (product_id, product_category_name, product_name_length, product_description_length,product_photos_qty,product_weight_g, product_length_cm, product_height_cm, product_width_cm
)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;


-- setup the sellers table 
create table if not exists e_commerce.sellers
(
    seller_id UUID PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state VARCHAR(2)
);
-- provide the command to copy order sellers data into POSTGRES
COPY e_commerce.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state
)
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

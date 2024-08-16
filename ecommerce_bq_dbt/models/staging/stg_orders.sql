with orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_delivered_customer_date
    from {{ source('ecommerce_airflow_etl', 'orders') }}
),

customers as (
    select
        customer_id,
        customer_city,
        customer_state
    from {{ source('ecommerce_airflow_etl', 'customers') }}
),

order_items as (
    select
        order_id,
        order_item_id,
        product_id,
        price,
        freight_value
    from {{ source('ecommerce_airflow_etl', 'order_items') }}
)

select
    orders.order_id,
    orders.customer_id,
    customers.customer_state,
    orders.order_status,
    orders.order_purchase_timestamp,
    orders.order_delivered_customer_date,
    order_items.product_id,
    round(sum(order_items.price),2) as price,
    round(sum(order_items.freight_value),2) as freight,
    sum(order_items.price) + sum(order_items.freight_value) as total_order_value
from orders 
left join customers 
    on orders.customer_id = customers.customer_id
left join order_items
    on orders.order_id = order_items.order_id
group by
    orders.order_id,
    orders.customer_id,
    customers.customer_state,
    orders.order_status,
    orders.order_purchase_timestamp,
    orders.order_delivered_customer_date,
    order_items.product_id

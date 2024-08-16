-- Intermediate model to calculate delivery time for each order 
select
    order_id,
    round(date_diff(order_delivered_customer_date, order_purchase_timestamp, day)) as delivery_time_days
from {{ ref('stg_orders') }}
where order_status = 'delivered'

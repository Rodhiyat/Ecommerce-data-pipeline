{{ config(materialized='table') }}

select
    customer_state,
    total_orders
from {{ ref('int_orders_by_state') }}

{{ config(materialized='table') }}

select
    order_id,
    delivery_time_days
from {{ ref('int_avg_delivery_time') }}

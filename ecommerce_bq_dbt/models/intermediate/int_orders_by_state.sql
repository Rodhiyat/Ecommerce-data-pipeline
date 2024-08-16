select
    customer_state,
    count(distinct order_id) as total_orders
from {{ ref('stg_orders') }}
group by customer_state
order by total_orders desc

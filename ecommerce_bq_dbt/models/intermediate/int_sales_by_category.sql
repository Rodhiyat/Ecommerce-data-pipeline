select
    stg_products.product_category_name,
    round(sum(stg_orders.price),2) as total_sales
from {{ ref('stg_orders') }} stg_orders
join {{ ref('stg_products') }} stg_products
on stg_orders.product_id = stg_products.product_id
group by
    stg_products.product_category_name

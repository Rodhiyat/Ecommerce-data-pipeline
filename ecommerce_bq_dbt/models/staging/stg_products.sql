select
    product_id,
    trim(lower(coalesce(product_category_name, 'not available'))) as product_category_name
from {{ source('ecommerce_airflow_etl', 'products') }}

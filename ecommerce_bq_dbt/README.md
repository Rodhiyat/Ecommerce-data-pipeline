## Project Overview
This DBT project is designed to perform ETL and analytics on a Brazilian E-Commerce dataset. The primary objective is to transform raw data into structured models, allowing for insightful analysis on key business metrics such as sales by product category, average delivery time, and order distribution by state.

## Dataset
The dataset originates from a BigQuery dataset named ecommerce_airflow_etl, which resides in the project altschoolproject-425708. It contains several tables that hold information on orders, products, customers, order items, and more.

## Project Structure
This DBT project is organized into three primary levels of models:

- Staging Models: These models clean and standardize the raw data.

    - stg_orders.sql: Aggregates and cleans data related to orders.
    - stg_products.sql: Cleans and standardizes product data, including handling missing product categories.
    - Intermediate Models: These models apply business logic to the cleaned data.
- Intermediate Models: These models apply business logic to the cleaned data.
    - int_sales_by_category.sql: Aggregates sales data by product category.
    - int_avg_delivery_time.sql: Calculates the average delivery time for each order.
    - int_orders_by_state.sql: Counts the number of orders per state.
- Final Models: These are the end results, ready for analysis or export.

    - fct_sales_by_category.sql: Final model for analyzing sales by category.
    - fct_avg_delivery_time.sql: Final model for analyzing average delivery time.
    - fct_orders_by_state.sql: Final model for analyzing the number of orders by state.
### Schema Configuration
Each model has its corresponding schema.yml file that defines the structure of the data and sets up testing for data quality.
## How to Run the Project
- Clone the Repository: Start by cloning the repository to your local environment.
- Set Up the Environment: Ensure that your environment is correctly configured with access to your BigQuery project. You'll need a profiles.yml file with your BigQuery connection details.
- Install DBT: create a virtual environment and install dbt

    ```pip install dbt-bigquery```
- Run the model

    ```dbt run```


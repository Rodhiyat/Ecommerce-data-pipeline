# End-to-End ETL Process with PostgreSQL, Docker, Airflow, dbt, and BigQuery
## Project Overview
This project involves developing an end-to-end ETL process using the *[Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)* from Kaggle to help end users answer analytical questions. The project demonstrates the use of tools like PostgreSQL, Docker, Docker Compose, Apache Airflow, dbt, and Google BigQuery.
## Step 1: Data Ingestion into PostgreSQL
- Download the Dataset: *[Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)*
- Setup PostgreSQL Database using Docker and Docker Compose in the `postgresql-docker-init folder`

The folder contains the following;
 + data: this folder contains the CSV file to be loaded into the ecommerce database.
 + infra-scripts: this folder contains the SQL script to initialize the database schema, create tables, and load the data.
 + docker-compose.yml: the docker Compose configuration file to set up the PostgreSQL container. It defines the PostgreSQL service, setting up the environment variables, ports, and volumes.

## Step 2: Setting up Apache Airflow

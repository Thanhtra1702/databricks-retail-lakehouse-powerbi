-- =========================================================================
-- SCRIPT: 01_create_gold_tables.sql
-- DESCRIPTION: DDL definitions for Kimball Star Schema in Gold Layer
-- ENGINE: Databricks Unity Catalog / Delta Lake
-- =========================================================================

USE CATALOG omni_retail_catalog;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- 1. Dimension: dim_product
CREATE OR REPLACE TABLE dim_product (
    product_id STRING NOT NULL,
    product_name STRING,
    unit_price DECIMAL(18, 2),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_id)
)
USING DELTA
COMMENT 'Product dimension table containing normalized product catalog';

-- 2. Dimension: dim_customer
CREATE OR REPLACE TABLE dim_customer (
    customer_id STRING NOT NULL,
    customer_name STRING,
    email STRING,
    city STRING,
    state STRING,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id)
)
USING DELTA
COMMENT 'Customer dimension table with current active profiles (SCD Type 2 snapshot)';

-- 3. Fact Table: fact_sales_daily (Transaction Grain)
CREATE OR REPLACE TABLE fact_sales_daily (
    order_id STRING NOT NULL,
    order_date DATE NOT NULL,
    customer_id STRING NOT NULL,
    product_id STRING NOT NULL,
    quantity INT,
    unit_price DECIMAL(18, 2),
    total_sales_amount DECIMAL(18, 2),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
)
USING DELTA
CLUSTER BY (order_date, product_id)
COMMENT 'Core sales transaction fact table optimized with Liquid Clustering';
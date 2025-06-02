

IF OBJECT_ID('DWH.GOLD.fact_sales', 'U') IS NOT NULL
    DROP TABLE DWH.GOLD.fact_sales;
CREATE TABLE DWH.GOLD.fact_sales (
    sales_order_number VARCHAR(50),
    sales_product_key INT,
    sales_customer_key INT,
    sales_order_date DATE,
    sales_ship_date DATE,
    sales_due_date DATE,
    sales_sold_price DECIMAL(10,2),
    sales_quantity INT,
    sales_original_price DECIMAL(10,2),
    );


DROP VIEW GOLD.dim_customers;

IF OBJECT_ID('DWH.GOLD.dim_customers', 'U') IS NOT NULL
    DROP TABLE DWH.GOLD.dim_customers;
CREATE TABLE DWH.GOLD.dim_customers (
	customer_key INT PRIMARY KEY,
	customer_id INT,
	customer_number VARCHAR(50),
	customer_first_name VARCHAR(50),
	customer_last_name VARCHAR(50),
	customer_country VARCHAR(50),
	customer_marital_status VARCHAR(50),
	customer_gender VARCHAR(50),
	customer_birthdate DATE,
	customer_create_date DATE,
);



IF OBJECT_ID('DWH.GOLD.dim_products', 'U') IS NOT NULL
    DROP TABLE DWH.GOLD.dim_products;
CREATE TABLE DWH.GOLD.dim_products (
	product_key INT PRIMARY KEY,
	product_id INT,
	product_number VARCHAR(50),
	product_name VARCHAR(50),
	category_id VARCHAR(50),
	category_name VARCHAR(50),
	subcategory_name VARCHAR(50),
	product_maintenance VARCHAR(50),
	product_cost INT,
	product_line VARCHAR(50),
	product_start_date DATE,
);



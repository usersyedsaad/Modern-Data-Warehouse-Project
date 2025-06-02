
/* What is data modelling

The process of taking this data than organization it into more reasonable form
Conceptual data model
Logical data model With cardinality
Physical data model
The new model would be suiltable for analytics

Star schema

fact surrounded by dimentions

*/

--Making dim_customers


USE DWH;

-- WE'LL CREATE VIEWS
-- VIEWS REFERENCE THE DATA IN THE TABLE AND DO NOT CREATE NEW TABLES IN MEMORY
TRUNCATE TABLE DWH.GOLD.dim_customers;
INSERT INTO DWH.GOLD.dim_customers(
	customer_key,
	customer_id,
	customer_number,
	customer_first_name,
	customer_last_name,
	customer_country,
	customer_marital_status,
	customer_gender,
	customer_birthdate,
	customer_create_date
)
SELECT
ROW_NUMBER() OVER(ORDER BY cci.cst_create_date) customer_key,
cci.cst_id customer_id,
cci.cst_key customer_number,
cci.cst_firstname customer_first_name,
cci.cst_lastname customer_last_name,
ela.cntry customer_country,
cci.cst_marital_status customer_marital_status,
CASE
	WHEN cci.cst_gndr != 'N/A' THEN cci.cst_gndr
	ELSE COALESCE(eca.gen,'N/A')
END AS customer_gender,
eca.bdate customer_birthdate,
cci.cst_create_date customer_create_date
FROM SILVER.crm_cust_info cci
LEFT JOIN SILVER.erp_cust_az12 eca 
ON cci.cst_key = eca.cid
LEFT JOIN SILVER.erp_loc_a101 ela
ON cci.cst_key = ela.cid



TRUNCATE TABLE DWH.GOLD.dim_products
INSERT INTO DWH.GOLD.dim_products(
	product_key,
	product_id,
	product_number,
	product_name,
	category_id,
	category_name,
	subcategory_name,
	product_maintenance,
	product_cost,
	product_line,
	product_start_date
)
SELECT 
ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt, cpi.prd_key) as product_key,
cpi.prd_id product_id,
cpi.prd_key product_number,
cpi.prd_nm product_name,
cpi.cat_id category_id,
epcg.cat category_name,
epcg.subcat subcategory_name,
epcg.maintenance maintenance,
cpi.prd_cost product_cost,
cpi.prd_line product_line,
cpi.prd_start_dt product_start_date
FROM SILVER.crm_prd_info cpi
LEFT JOIN SILVER.erp_px_cat_g1v2 epcg
ON cpi.cat_id = epcg.id
WHERE cpi.prd_end_dt IS NULL --latest products
;

TRUNCATE TABLE DWH.GOLD.fact_sales;
INSERT INTO DWH.GOLD.fact_sales (
	sales_order_number,
	sales_product_key,
	sales_customer_key,
	sales_order_date,
	sales_ship_date,
	sales_due_date,
	sales_sold_price,
	sales_quantity,
	sales_original_price
)
SELECT
	sls_ord_num,
	dp.product_key,
	dc.customer_key,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM SILVER.crm_sales_details csd
LEFT JOIN DWH.GOLD.dim_products dp
	ON csd.sls_prd_key = dp.product_number
LEFT JOIN DWH.GOLD.dim_customers dc
	ON csd.sls_cust_id = dc.customer_id;


SELECT * FROM DWH.GOLD.fact_sales fs
JOIN DWH.GOLD.dim_customers dc
ON fs.sales_customer_key = dc.customer_key
JOIN DWH.GOLD.dim_products dp
ON fs.sales_product_key = dp.product_key



SELECT * FROM DWH.GOLD.fact_sales;
SELECT * FROM DWH.GOLD.dim_customers;
SELECT * FROM DWH.GOLD.dim_products;

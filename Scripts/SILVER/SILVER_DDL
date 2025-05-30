use master;
use DWH;


/*
 * Data Warehouse Project - SILVER Layer Setup
 *
 * Purpose:
 * This script sets up the SILVER layer of the Data Warehouse.
 * It handles raw data ingestion from source systems without transformations or modeling.
 * Medallion architecture is followed with SILVER as the staging layer.
 *
 * Key Activities:
 * - Understand business context and data requirements
 * - Identify source system details and integration methods
 * - Design schema and create tables in the SILVER layer
 * - Drop existing tables if they already exist (for clean reloads)
 *
 * Data Ingestion Strategy:
 * - Batch processing
 * - Full load
 * - No transformations or modeling at this stage
 */

-- Interview source system experts to gather:
-- - Business context the data supports
-- - Documentation availability
-- - Data models and catalogues
-- - Storage method and integration capabilities
-- - Load strategies (incremental vs full)
-- - Expected data size and volume constraints
-- - Source system performance considerations
-- - Authentication requirements

-- Create SILVER Layer Tables (Drop if already exists)

IF OBJECT_ID('DWH.SILVER.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.crm_cust_info;

CREATE TABLE DWH.SILVER.crm_cust_info (
    cst_id VARCHAR(50),
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);

IF OBJECT_ID('DWH.SILVER.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.crm_prd_info;

CREATE TABLE DWH.SILVER.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost DECIMAL(10,2) NOT NULL,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID('DWH.SILVER.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.crm_sales_details;

CREATE TABLE DWH.SILVER.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(50),
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT NOT NULL,
    sls_quantity INT NOT NULL,
    sls_price INT NOT NULL
);

IF OBJECT_ID('DWH.SILVER.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.erp_cust_az12;

CREATE TABLE DWH.SILVER.erp_cust_az12 (
    cid NVARCHAR(50) NOT NULL,
    bdate DATE,
    gen NVARCHAR(50)
);

IF OBJECT_ID('DWH.SILVER.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.erp_loc_a101;

CREATE TABLE DWH.SILVER.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

IF OBJECT_ID('DWH.SILVER.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE DWH.SILVER.erp_px_cat_g1v2;

CREATE TABLE DWH.SILVER.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);



CREATE TABLE DWH.SILVER.silver_load_log(
	id INT IDENTITY(1,1),
	job_name NVARCHAR(50),
	step_name NVARCHAR(50),
	total_duration FLOAT,
	message VARCHAR(50)
);
-- Data Validation:
-- - Ensure completeness of records
-- - Run basic data checks (e.g., nulls, types)

-- Documentation:
-- - Comment and document each step clearly
-- - Maintain version control through commits

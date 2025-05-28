/*
 * Data Warehouse Project - Bronze Layer Setup
 *
 * Purpose:
 * This script sets up the BRONZE layer of the Data Warehouse.
 * It handles raw data ingestion from source systems without transformations or modeling.
 * Medallion architecture is followed with BRONZE as the staging layer.
 *
 * Key Activities:
 * - Understand business context and data requirements
 * - Identify source system details and integration methods
 * - Design schema and create tables in the BRONZE layer
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

-- Create BRONZE Layer Tables (Drop if already exists)

IF OBJECT_ID('DWH.BRONZE.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.crm_cust_info;

CREATE TABLE DWH.BRONZE.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date NVARCHAR(50)
);

IF OBJECT_ID('DWH.BRONZE.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.crm_prd_info;

CREATE TABLE DWH.BRONZE.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost NVARCHAR(50),
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

IF OBJECT_ID('DWH.BRONZE.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.crm_sales_details;

CREATE TABLE DWH.BRONZE.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(50),
    sls_order_dt NVARCHAR(50),
    sls_ship_dt NVARCHAR(50),
    sls_due_dt NVARCHAR(50),
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

IF OBJECT_ID('DWH.BRONZE.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.erp_cust_az12;

CREATE TABLE DWH.BRONZE.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

IF OBJECT_ID('DWH.BRONZE.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.erp_loc_a101;

CREATE TABLE DWH.BRONZE.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

IF OBJECT_ID('DWH.BRONZE.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE DWH.BRONZE.erp_px_cat_g1v2;

CREATE TABLE DWH.BRONZE.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);



CREATE TABLE DWH.BRONZE.bronze_load_log(
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

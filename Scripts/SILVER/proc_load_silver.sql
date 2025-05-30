/*
===============================================================================
Procedure Name : load_silver
Schema         : DWH.SILVER
Author         : Syed Saad Sohail
Created On     : 2025-05-29
Last Updated   : 2025-05-29
===============================================================================
Purpose:
---------
This stored procedure performs an ETL operation that loads and transforms data 
from the BRONZE layer into the SILVER layer of the Data Warehouse (DWH).
It ensures data quality by applying transformations, cleansing logic, and 
consistency checks before inserting into target tables. The procedure logs 
each step's duration and status for audit and debugging purposes.

Description:
-------------
The procedure performs the following tasks:
1. Begins a transaction to ensure data consistency and atomicity.
2. Clears staging tables in the SILVER schema using `TRUNCATE`.
3. Applies transformation logic to cleanse and enrich data.
4. Inserts cleaned data from BRONZE to SILVER schema tables.
5. Logs each ETL step (TRUNCATE/INSERT) into `silver_load_log`.
6. In case of errors, rolls back the transaction and logs the error in 
   `bronze_load_log`.

Transformations:
------------------
- Trim and standardize values (e.g., names, gender, marital status).
- Convert codes to meaningful labels (e.g., 'M' → 'Male').
- Filter out null or invalid values (e.g., dates, sales).
- Normalize and split product keys into ID and category.
- Deduplicate customer records based on most recent create date.
- Validate calculated fields (e.g., recompute sales if missing).
- Replace malformed or inconsistent values with defaults (e.g., 'N/A').

Tables Affected:
-----------------
- DWH.SILVER.crm_cust_info
- DWH.SILVER.crm_prd_info
- DWH.SILVER.crm_sales_details
- DWH.SILVER.erp_cust_az12
- DWH.SILVER.erp_loc_a101
- DWH.SILVER.erp_px_cat_g1v2
- DWH.SILVER.silver_load_log
- DWH.BRONZE.bronze_load_log (used for error logging)

Log Table:
-----------
- silver_load_log: Logs each step with job_name, step_name, duration, and 
  status ('SUCCESS' or error).
- bronze_load_log: Captures error messages when the procedure fails.

File Sources:
--------------
- BRONZE.crm_cust_info
- BRONZE.crm_prd_info
- BRONZE.crm_sales_details
- BRONZE.erp_cust_az12
- BRONZE.erp_loc_a101
- BRONZE.erp_px_cat_g1v2

Important Notes:
-----------------
- Entire procedure is wrapped in a TRY-CATCH block for error handling.
- Only the most recent, non-null, and clean records are inserted into SILVER.
- This procedure assumes consistent schema and availability of BRONZE tables.

Execution:
-----------
To run the procedure:
    EXEC DWH.SILVER.load_silver;

To view logs:
    SELECT * FROM DWH.SILVER.silver_load_log;
    SELECT * FROM DWH.BRONZE.bronze_load_log;

===============================================================================
*/







CREATE OR ALTER PROCEDURE SILVER.load_silver
AS
BEGIN
	BEGIN TRY
		BEGIN TRANSACTION
			SET NOCOUNT ON;
			
			
			
			DECLARE
				@start_time DATETIME2(7),
				@end_time DATETIME2(7),
				@duration_ms FLOAT,
				@batch_start_time DATETIME2(7),
				@batch_end_time DATETIME2(7),
				@batch_duration_ms FLOAT;
			
			
			TRUNCATE TABLE DWH.SILVER.silver_load_log;
	    	SET    @batch_start_time = SYSDATETIME();
			PRINT '>> LOADING CRM'
	--------------------------------------------------------------
		--TABLE 01 # Truncate and Insert SILVER.crm_cust_info
	--------------------------------------------------------------
			
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.crm_cust_info at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE SILVER.crm_cust_info
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.crm_cust_info at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_cust_info', 'TRUNCATE', @duration_ms, 'SUCCESS');
			
			
			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.BRONZE.crm_cust_info at ' + CONVERT(VARCHAR, @start_time, 121);
			
			INSERT INTO SILVER.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT 
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			FROM (
			SELECT
			CAST(cst_id as VARCHAR(50)) cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE 
				WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
				WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
				ELSE 'N/A'
			END as cst_marital_status
			,
			CASE 
				WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
				WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
				ELSE 'N/A'
			END as cst_gndr
			,
			CAST(cst_create_date as DATE) AS cst_create_date,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
			FROM BRONZE.crm_cust_info
			WHERE cst_id IS NOT NULL) as sub
			WHERE row_num = 1 
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.crm_cust_info at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_cust_info', 'INSERT', @duration_ms, 'SUCCESS');
			
			--------------------------------------------------------------
				--TABLE 02 # Truncate and Insert SILVER.crm_prd_info
			--------------------------------------------------------------
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.crm_prd_info at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE DWH.SILVER.crm_prd_info
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.crm_prd_info at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_prd_info', 'TRUNCATE', @duration_ms, 'SUCCESS');
			
			
			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.SILVER.crm_prd_info at ' + CONVERT(VARCHAR, @start_time, 121);
			
			INSERT INTO DWH.SILVER.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT * FROM (
			SELECT 
				CAST(prd_id as VARCHAR(50)) as prd_id,
				REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') as cat_id,
				SUBSTRING(TRIM(prd_key), 7,LEN(prd_key)) as prd_key,
				prd_nm,
				ISNULL(CAST(prd_cost AS INT), 0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'N/A'
				END AS prd_line,
				prd_start_dt,
				DATEADD(DAY,-1,LEAD(prd_start_dt, 1, NULL) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
			FROM BRONZE.crm_prd_info) sub
			;
			
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.crm_prd_info at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_prd_info', 'INSERT', @duration_ms, 'SUCCESS');
			
			--------------------------------------------------------------
				--TABLE 03 # Truncate and Insert SILVER.crm_sales_details
			--------------------------------------------------------------
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.crm_sales_details at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE DWH.SILVER.crm_sales_details;
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.crm_sales_details at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_sales_details', 'TRUNCATE', @duration_ms, 'SUCCESS');
			
			
			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.SILVER.crm_sales_details at ' + CONVERT(VARCHAR, @start_time, 121);
			
			INSERT INTO DWH.SILVER.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
			sls_ord_num, 
			sls_prd_key, 
			sls_cust_id, 
			CASE 
				WHEN LEN(sls_order_dt) != 8 THEN NULL 
				ELSE CAST(sls_order_dt as DATE)
			END as sls_order_dt,
			CASE 
				WHEN LEN(sls_ship_dt) != 8 THEN NULL 
				ELSE CAST(sls_ship_dt as DATE)
			END as sls_ship_dt, 
			CASE 
				WHEN LEN(sls_due_dt) != 8 THEN NULL 
				ELSE CAST(sls_due_dt as DATE)
			END as sls_due_dt ,
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price)
					THEN ABS(sls_price)*sls_quantity 
				ELSE sls_sales
			END as sls_sales,
			sls_quantity , 
			CASE
				WHEN sls_price <= 0 OR sls_price IS NULL
					THEN sls_sales/sls_quantity 
				ELSE ABS(sls_price)
			END AS sls_price
			FROM BRONZE.crm_sales_details csd;
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.crm_sales_details at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.crm_sales_details', 'INSERT', @duration_ms, 'SUCCESS');
			--------------------------------------------------------------
				--TABLE 04 # Truncate and Insert SILVER.erp_cust_az12
			--------------------------------------------------------------
			
			PRINT '>> LOADING ERP';
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.erp_cust_az12 at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE DWH.SILVER.erp_cust_az12;
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.erp_cust_az12 at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_cust_az12', 'TRUNCATE', @duration_ms, 'SUCCESS');

			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.SILVER.erp_cust_az12 at ' + CONVERT(VARCHAR, @start_time, 121);
			
			INSERT INTO DWH.SILVER.erp_cust_az12 (
				cid,
				bdate,
				gen
			)
			SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END as cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate
			,
			CASE
				WHEN TRIM(gen) IS NULL OR TRIM(gen) = '' THEN 'N/A'
				WHEN UPPER(gen) = 'F' THEN 'Female'
				WHEN UPPER(gen) = 'M' THEN 'Male'
				ELSE TRIM(gen)
			END AS gen
			FROM BRONZE.erp_cust_az12 eca
			
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.erp_cust_az12 at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_cust_az12', 'INSERT', @duration_ms, 'SUCCESS');
			
			
			--------------------------------------------------------------
				--TABLE 05 # Truncate and Insert SILVER.erp_loc_a101
			--------------------------------------------------------------
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.erp_loc_a101 at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE DWH.SILVER.erp_loc_a101;
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.erp_loc_a101 at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_loc_a101', 'TRUNCATE', @duration_ms, 'SUCCESS');

			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.SILVER.erp_loc_a101 at ' + CONVERT(VARCHAR, @start_time, 121);			
			
			INSERT INTO DWH.SILVER.erp_loc_a101 (
				cid, cntry
			)
			SELECT 
			REPLACE(cid, '-', '_') AS cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END as cntry
			FROM BRONZE.erp_loc_a101
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.erp_loc_a101 at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_loc_a101', 'INSERT', @duration_ms, 'SUCCESS');
			
			--------------------------------------------------------------
				--TABLE 06 # Truncate and Insert SILVER.erp_px_cat_g1v2
			--------------------------------------------------------------
			
			SET @start_time = SYSDATETIME()
			PRINT 'Start Truncating: DWH.SILVER.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @start_time, 121);
			
			TRUNCATE TABLE DWH.SILVER.erp_px_cat_g1v2;
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Truncating: DWH.SILVER.erp_loc_a101 at ' + CONVERT(VARCHAR, @end_time, 121);
			SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_px_cat_g1v2', 'TRUNCATE', @duration_ms, 'SUCCESS');
			
			SET @start_time = SYSDATETIME();
        	PRINT 'Start Inserting: DWH.SILVER.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @start_time, 121);			
			
			
			INSERT INTO DWH.SILVER.erp_px_cat_g1v2 (
			id, cat, subcat, maintenance
			)
			SELECT * FROM BRONZE.erp_px_cat_g1v2;
			
			
			SET @end_time = SYSDATETIME();
			PRINT 'End Inserting: DWH.SILVER.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @end_time, 121); 
        	SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        	INSERT INTO DWH.SILVER.silver_load_log (job_name, step_name, total_duration, message)
        	VALUES ('DWH.SILVER.erp_px_cat_g1v2', 'INSERT', @duration_ms, 'SUCCESS');
			
		COMMIT;
			
	END TRY
	BEGIN CATCH
        -- Rollback everything if any step fails
        ROLLBACK;

        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        PRINT 'ERROR: ' + @error_message;

        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('LOAD BRONZE BATCH', 'FAILED', 0, @error_message);
    END CATCH
    SET		@batch_end_time = SYSDATETIME()
    SET @batch_duration_ms = DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) / 1000.0;
    PRINT 'The whole process took '+ CAST(@batch_duration_ms AS VARCHAR)+' seconds'

END;



EXEC SILVER.load_silver;

SElEcT job_name, COUNT(*) total_processes ,SUM(total_duration) FROM silver.silver_load_log
GROUP BY job_name



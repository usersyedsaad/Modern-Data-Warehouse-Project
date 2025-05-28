/*
===============================================================================
Procedure Name : load_bronze
Schema         : DWH.BRONZE
Author         : Syed Saad Sohail
Created On     : 5/08/2025
Last Updated   : 5/8/25
===============================================================================
Purpose:
---------
This stored procedure performs a batch ETL operation to load data into the 
bronze layer of a Data Warehouse (DWH). It processes CSV files from CRM and ERP 
sources and inserts the data into corresponding bronze staging tables.

Description:
-------------
The procedure performs the following tasks sequentially:
1. Starts a transaction to ensure atomicity.
2. Truncates existing data in the bronze staging tables to avoid duplication.
3. Loads new data from specified CSV files using BULK INSERT.
4. Logs the duration and status (SUCCESS/FAILED) of each operation in the
   `bronze_load_log` table.
5. Uses TRY-CATCH to handle any exceptions. If any step fails, the transaction 
   is rolled back and the error is logged.

Tables Affected:
-----------------
- DWH.BRONZE.crm_cust_info
- DWH.BRONZE.crm_prd_info
- DWH.BRONZE.crm_sales_details
- DWH.BRONZE.erp_cust_az12
- DWH.BRONZE.erp_loc_a101
- DWH.BRONZE.erp_px_cat_g1v2
- DWH.BRONZE.bronze_load_log

Log Table:
-----------
- bronze_load_log: Stores job_name, step_name, duration, and message 
  (SUCCESS/FAILED or error message).

File Sources:
--------------
- cust_info.csv
- prd_info.csv
- sales_details.csv
- cust_az12.csv
- loc_a101.csv
- px_cat_g1v2.csv

Important Notes:
-----------------
- Ensure that the file paths are valid and accessible from the SQL Server instance.
- The procedure must be run with appropriate privileges to truncate and insert 
  into the tables, and to access the file system for BULK INSERT.

Execution:
-----------
To run the procedure:
    EXEC load_bronze;

To view logs:
    SELECT * FROM DWH.BRONZE.bronze_load_log;

===============================================================================
*/






CREATE PROCEDURE load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2(7), 
            @end_time DATETIME2(7), 
            @duration_ms FLOAT,
    		@batch_start_time DATETIME2(7),
    		@batch_end_time DATETIME2(7),
    		@batch_duration_ms FLOAT;
    BEGIN TRY
        -- Start Transaction
        BEGIN TRANSACTION;
		
        -- Clear previous logs
        TRUNCATE TABLE DWH.BRONZE.bronze_load_log;
	    SET    @batch_start_time = SYSDATETIME();
		PRINT '>> LOADING CRM'
        -----------------------------
        -- Step 1: crm_cust_info
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.crm_cust_info at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.crm_cust_info;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_cust_info', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
        PRINT 'Start Inserting: DWH.BRONZE.crm_cust_info at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.crm_cust_info
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_cust_info', 'INSERT', @duration_ms, 'SUCCESS');


        -----------------------------
        -- Step 2: crm_prd_info
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.crm_prd_info at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.crm_prd_info;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_prd_info', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
        PRINT 'Start Inserting: DWH.BRONZE.crm_prd_info at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.crm_prd_info
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_prd_info', 'INSERT', @duration_ms, 'SUCCESS');
		
        
         -----------------------------
        -- Step 3: crm_sales_details
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.crm_sales_details at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.crm_sales_details;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_sales_details', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
        PRINT 'Start Inserting: DWH.BRONZE.crm_sales_details at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.crm_sales_details
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.crm_sales_details', 'INSERT', @duration_ms, 'SUCCESS');


        PRINT '>>LOADING ERP'
         -----------------------------
        -- Step 4: erp_cust_az12
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.erp_cust_az12 at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.erp_cust_az12;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_cust_az12', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
        PRINT 'Start Inserting: DWH.BRONZE.erp_cust_az12 at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.erp_cust_az12
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_erp\cust_az12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_cust_az12', 'INSERT', @duration_ms, 'SUCCESS');

        
        -----------------------------
        -- Step 5: erp_loc_a101
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.erp_loc_a101 at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.erp_loc_a101;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_loc_a101', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
        PRINT 'Start Inserting: DWH.BRONZE.erp_loc_a101 at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.erp_loc_a101
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_erp\loc_a101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_loc_a101', 'INSERT', @duration_ms, 'SUCCESS');
        
        
        
        
        
        -----------------------------
        -- Step 6: erp_px_cat_g1v2
        -----------------------------
        SET @start_time = SYSDATETIME();
        PRINT 'Start Truncating: DWH.BRONZE.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @start_time, 121);

        TRUNCATE TABLE DWH.BRONZE.erp_px_cat_g1v2;

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_px_cat_g1v2', 'TRUNCATE', @duration_ms, 'SUCCESS');

        SET @start_time = SYSDATETIME();
       	PRINT 'Start Inserting: DWH.BRONZE.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @start_time, 121);
        BULK INSERT DWH.BRONZE.erp_px_cat_g1v2
        FROM 'C:\Users\HP\Desktop\SQL Project\My Project\Datasets\source_erp\px_cat_g1v2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0;
        INSERT INTO DWH.BRONZE.bronze_load_log (job_name, step_name, total_duration, message)
        VALUES ('DWH.BRONZE.erp_px_cat_g1v2', 'INSERT', @duration_ms, 'SUCCESS');
        
        -- Commit if everything succeeds
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


DROP PROCEDURE load_bronze;
EXEC load_bronze;
SELECT * FROM bronze.bronze_load_log;


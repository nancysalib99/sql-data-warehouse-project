/*
================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
================================================================================
*/
CREATE OR ALTER PROCEDURE Sliver.Load_Sliver AS
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	    SET @batch_start_time = GETDATE()
		PRINT '============================'
		PRINT 'Loading Silver Layer'
		PRINT '============================'

		PRINT '============================'
		PRINT 'Loading CRM Tables'
		PRINT '============================'

		SET @start_time = GETDATE()
			PRINT '>> Truncating Table: sliver.crm_cust_info'; 
			TRUNCATE TABLE sliver.crm_cust_info;
			PRINT '>> Inserting Data Into: sliver.crm_cust_info';
			INSERT INTO sliver.crm_cust_info(
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status, --Normalize material status values to readable format
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'male'
				 ELSE 'n/a'
			END cst_gndr, -- Normalize gender values to readable format
			cst_create_date
			FROM (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
			FROM bronze.crm_cust_info) t 
			WHERE flag_last = 1 AND cst_id IS NOT NULL --Select the most recent record per customer 
		SET @end_time = GETDATE()
		PRINT 'loading Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR(50)) + 'Seconds'
		PRINT '----------------------------------------------------------'

		SET @start_time = GETDATE()
			PRINT 'Truncating Table: sliver.crm_prd_info'
			TRUNCATE TABLE sliver.crm_prd_info
			PRINT 'Inserting Data Into: sliver.crm_prd_info'
			INSERT INTO sliver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

			SELECT
			prd_id,
			REPLACE((SUBSTRING (prd_key, 1, 5)), '-', '_')  AS cat_key, --Extract catagory key
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost, -- Handling null values by replacing into zero values
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
			END prd_line, -- Maping product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt, -- remving time part
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  -- Calculate end_date as one day before the next start_date
			FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR(50)) + 'Seconds'
		PRINT '----------------------------------------------------'

		SET @start_time = GETDATE()
			PRINT 'Truncating Table: sliver.crm_sales_details'
			TRUNCATE TABLE sliver.crm_sales_details
			PRINT 'Inserting Data Into: sliver.crm_sales_details'
			INSERT INTO sliver.crm_sales_details (
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
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS nvarchar) AS DATE)
			END sls_order_dt, 
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS nvarchar) AS date)
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				  ELSE CAST(CAST(sls_due_dt AS NVARCHAR)AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL 
					OR sls_sales <= 0 
					OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN  sls_sales / NULLIF(sls_quantity,0) 
			ELSE sls_price 
			END sls_price
			FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR(50))
		PRINT '-------------------------------------------------------------'

		PRINT '======================'
		PRINT 'loadng ERP Tables'
		PRINT '======================'

		SET @start_time = GETDATE()
			PRINT 'Truncating Table: sliver.erp_cust_az12'
			TRUNCATE TABLE sliver.erp_cust_az12
			PRINT 'Inserting Data Into: sliver.erp_cust_az12'
			INSERT INTO sliver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
			SELECT
			CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				 ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END bdate, -- Set future birthdates to Null
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
			ELSE 'n\a'
			END gen -- Normalize gender vales and handle unkonwn cases
			FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR(50))
		PRINT '-------------------------------------------------------'

		SET @start_time = GETDATE()
			PRINT 'Truncating Table: sliver.erp_loc_a101'
			TRUNCATE TABLE sliver.erp_loc_a101
			PRINT 'Inserting Data Into Table: sliver.erp_loc_a101'
			INSERT INTO sliver.erp_loc_a101 (cid,cntry)

			SELECT 
			REPLACE(cid, '-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
			FROM bronze.erp_loc_a101 
		SET @end_time = GETDATE()
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR) + 'Seconds'
		PRINT '----------------------------------------------------------'

		SET @start_time = GETDATE()
			PRINT 'Truncating Table: sliver.erp_px_cat_g1v2'
			TRUNCATE TABLE sliver.erp_px_cat_g1v2
			PRINT 'Inserting Data Into: sliver.erp_px_cat_g1v2'
			INSERT INTO sliver.erp_px_cat_g1v2
			 (id, cat, subcat, maintenance)
 
			SELECT
			id,
			cat,
			subcat,
			maintenance
			FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(seconds, @start_time, @end_time)AS NVARCHAR(50)) + 'Seconds'
		PRINT '-----------------------------------------------------------'

		SET @batch_end_time = GETDATE()
		PRINT '============================================'
		PRINT 'Loading Silver Layer Is Completed'
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(seconds, @batch_start_time, @batch_end_time)AS NVARCHAR(50)) + 'Seconds'
		PRINT '============================================='
	END TRY
BEGIN CATCH
PRINT '========================='
PRINT 'ERROR OCCURRED DURING LOADING THE SLIVER LAYER'
PRINT 'Error Message' + ERROR_MESSAGE()
PRINT 'Error Number' + CAST(ERROR_NUMBER()AS NVARCHAR(50))
PRINT 'Error State' + CAST(ERROR_STATE()AS NVARCHAR(50))
PRINT '========================='
END CATCH
END
EXEC Sliver.Load_Sliver

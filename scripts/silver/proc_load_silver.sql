/*
===============================================================================
		Stored Procedure : Load Silver Layer (Bronze --> Silver)
===============================================================================
Script Purpose:
	This stored procedure perform the ETL(Extract, Transform and Load) 
	process to populate the 'silver' schema tables from 'bronze' schema.
Actions Performed:
	-- Truncates the Silver Tables.
	-- Inserts the transformed and cleansed datafrom Bronze into Silver tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC silver.load_silver;
================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';
		-- Loading silver.crm_cust_info Table
		PRINT 'Truncating Table:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting Data into Table:silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END	 cst_marital_status,  -- Normalize Maritial Status values to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'n/a'
		END	 cst_gndr, -- Normalize Gender values to readable format
		cst_create_date
		FROM(
			SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_latest
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_latest = 1 -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		PRINT '>>------------------------------------';

		-- Loading silver.crm_prd_info Table
		SET @start_time = GETDATE();
		PRINT 'Truncating Table:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting Data into Table:silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(
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
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extracted Category ID
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,	   -- Extracted Product_key
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost, -- Replacing NULL values to 0.
			CASE UPPER(TRIM(prd_line))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line, -- Map Product line codes to Descriptive Values
			prd_start_dt,
			DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) 
			AS prd_end_dt -- Calculated End date as One day before the next Start Date.
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		PRINT '>>------------------------------------';
	
		-- Loading silver.crm_sales_details Table
		SET @start_time = GETDATE();
		PRINT 'Truncating Table:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting Data into Table:silver.crm_sales_details';
		WITH data_clean AS
		(
		 -- STEP 1: Normalize numeric fields
		 -- - Convert negative values to positive using ABS()
		 -- - Convert 0 to NULL using NULLIF()
		 SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			NULLIF(ABS(sls_price),0) AS P,		-- Clean price
			NULLIF(ABS(sls_quantity),0) AS q,	-- Clean price
			NULLIF(ABS(sls_sales),0) AS s		-- Clean sales
		 FROM bronze.crm_sales_details
		),

		Fix_price AS
		(
		-- STEP 2: Derive price if missing or invalid
		-- Logic:
		-- - If price exists → keep it
		-- - Else if sales & quantity exist → price = sales / quantity
		SELECT 
			*,
			CASE WHEN P IS NOT NULL THEN P
				 WHEN q IS NOT NULL AND s IS NOT NULL 
					THEN s/NULLIF(q,0)  -- Safe division
			END	 final_p
		FROM data_clean
		),
		Fix_quantity AS
		(
		-- STEP 3: Derive quantity using corrected price
		-- IMPORTANT:
		-- Use final_p (not raw p) to avoid dependency issues
		SELECT 
			*,
			CASE WHEN q IS NOT NULL THEN q
				 WHEN final_p IS NOT NULL AND s IS NOT NULL THEN s/NULLIF(final_p,0)
			END	 final_q
		FROM Fix_price
		)
		-- STEP 4: Insert cleaned and transformed data into SILVER layer

		INSERT INTO silver.crm_sales_details( 
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
			-- STEP 5: Convert integer to dates (YYYYMMDD → DATE)
			-- - Handle NULL, 0, and invalid length
			-- - Use TRY_CAST to avoid runtime errors

			CASE WHEN sls_order_dt <=0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,

			CASE WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE WHEN sls_due_dt <=0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			-- STEP 6: Enforce business rule
			-- sales = quantity * price (always recomputed)
			CASE WHEN final_q IS NOT NULL AND final_p IS NOT NULL 
					THEN final_q * final_p
				 ELSE NULL
			END	sls_sales,
			final_q AS sls_quantity,
			final_p AS sls_price
		FROM Fix_quantity
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		PRINT '>>------------------------------------';

		------------------------------------------------------------------------------------------------------------
		PRINT '-----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------';
		-- loading silver.erp_cust_az12 Table
		SET @start_time = GETDATE();
		PRINT 'Truncating Table:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting Data into Table:silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Removed 'NAS' if present in Data.
				 ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL  -- Set Future bday's to NULL
				 ELSE bdate
			END  bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'	
	 			  WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'	
				  ELSE 'n/a'
			 END  gen -- Normalize gender values and handle the unknown cases.
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		PRINT '>>------------------------------------';
	
		-- loading silver.erp_loc_a101 Table
		SET @start_time = GETDATE();
		PRINT 'Truncating Table:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting Data into Table:silver.erp_loc_a101'

		 INSERT INTO silver.erp_loc_a101(cid,cntry)
		 SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
				 WHEN TRIM(UPPER(cntry)) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL   THEN 'n/a'
				 ELSE TRIM(cntry)
			END cntry  -- Normalize and handle missing or blank country codes.
		 FROM bronze.erp_loc_a101
		 SET @end_time = GETDATE();
		 PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		 PRINT '>>------------------------------------';
	 
		-- loading silver.erp_px_cat_g1v2 Table
		SET @start_time = GETDATE();
		PRINT 'Truncating Table:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting Data into Table:silver.erp_px_cat_g1v2'
 
		 INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		 )
		 SELECT 
			id,
			cat,
			subcat,
			maintenance
		 FROM bronze.erp_px_cat_g1v2
		 SET @end_time = GETDATE();
		 PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'Seconds';
		 PRINT '>>------------------------------------';
		 SET @batch_end_time = GETDATE();
		 PRINT '===============================================';
		 PRINT 'Loading Silver Layer is completed';
		 PRINT '- Total Load Duration: '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+'Seconds';
		 PRINT '===============================================';
	END TRY
	BEGIN CATCH
		 PRINT '===============================================';
		 PRINT 'Error Occured During Silver Layer Loading';
		 PRINT 'Error Message:'+ ERROR_MESSAGE();
		 PRINT 'Error Number:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		 PRINT 'ERROR_STATE:'+ CAST(ERROR_STATE() AS NVARCHAR);
		 PRINT '===============================================';

	END CATCH
END

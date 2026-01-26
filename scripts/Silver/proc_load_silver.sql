
/*
========================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract,Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
	- Truncates Silver tables.
	- Inserts transformed and Cleansed data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example :

	Exec Silver.load.silver;
==================================================================================
*/


CREATE OR ALTER PROCEDURE silver.Load_silver
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '========================================================';
        PRINT 'Loading Silver Layer';
        PRINT '========================================================';

        PRINT '---------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        /* =====================================================
           silver.crm_cust_info
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data into silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           silver.crm_prd_info
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data into silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                DATEADD(
                    DAY, -1,
                    LEAD(prd_start_dt)
                    OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
                ) AS DATE
            )
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           silver.crm_sales_details
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data into silver.crm_sales_details';

        ;WITH cleaned_sales AS (
            SELECT
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_quantity,
                CASE 
                    WHEN sls_sales IS NULL
                      OR sls_sales <= 0
                      OR sls_sales <> sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END AS sls_sales,
                CASE 
                    WHEN sls_price IS NULL OR sls_price <= 0
                    THEN 
                        (CASE 
                            WHEN sls_sales IS NULL OR sls_sales <= 0
                            THEN sls_quantity * ABS(sls_price)
                            ELSE sls_sales
                         END) / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END AS sls_price
            FROM bronze.crm_sales_details
        )
        INSERT INTO silver.crm_sales_details (
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
        SELECT * FROM cleaned_sales;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           silver.erp_cust_AZ12
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.erp_cust_AZ12';
        TRUNCATE TABLE silver.erp_cust_AZ12;

        PRINT '>> Inserting Data into silver.erp_cust_AZ12';

        INSERT INTO silver.erp_cust_AZ12 (cid, bdate, gen)
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,
            CASE 
                WHEN TRY_CONVERT(DATE, bdate) > GETDATE() THEN NULL
                ELSE TRY_CONVERT(DATE, bdate)
            END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM bronze.erp_CUST_AZ12;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           silver.erp_LOC_A101
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.erp_LOC_A101';
        TRUNCATE TABLE silver.erp_LOC_A101;

        PRINT '>> Inserting Data into silver.erp_LOC_A101';

        INSERT INTO silver.erp_LOC_A101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_LOC_A101;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           silver.erp_PX_CAT_G1V2
        ===================================================== */
        SET @start_time = GETDATE();

        PRINT '>> TRUNCATE TABLE silver.erp_PX_CAT_G1V2';
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

        PRINT '>> Inserting Data into silver.erp_PX_CAT_G1V2';

        INSERT INTO silver.erp_PX_CAT_G1V2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_PX_CAT_G1V2;

        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';

        /* =====================================================
           Batch Completion
        ===================================================== */
        SET @batch_end_time = GETDATE();

        PRINT '========================================================';
        PRINT 'Loading Silver Completed';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '========================================================';

    END TRY
    BEGIN CATCH
        PRINT '=======================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR(20));
        PRINT '=======================================================';
    END CATCH
END;

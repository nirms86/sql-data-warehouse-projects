*/
  ================================================================================
  Stored Procedure: Load Bronze Layer (Source -> Bronze)
  ================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'BULK INSERT command to load data from csv Files to bronze tables.
  
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
  
Usage Example:
EXEC bronze.load_bronze;
  
===================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

	SET @batch_start_time = GETDATE();
	PRINT '=====================================================================';
	PRINT 'Loading bronze Layer';
	PRINT '=====================================================================';

	PRINT '=====================================================================';
	PRINT 'Loading CRM Layer';
	PRINT '=====================================================================';

	SET @start_time = GETDATE();
    PRINT ' >> Truncating Table : bronze.crm_cust_info'  
    TRUNCATE TABLE bronze.crm_cust_info;

	PRINT ' >> Inserting data into : bronze.crm_cust_info'
    BULK INSERT bronze.crm_cust_info
    FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
   SET @end_time = GETDATE();
   PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
   PRINT '>>-----------------------------------';



    SET @start_time = GETDATE();
    PRINT ' >> Truncating Table : bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info;

	PRINT ' >> Inserting data into : bronze.crm_prd_info'
    BULK INSERT bronze.crm_prd_info
    FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>>-----------------------------------';



	 SET @start_time = GETDATE();	
     PRINT ' >> Truncating Table : bronze.crm_sales_details'
     TRUNCATE TABLE bronze.crm_sales_details;

     PRINT 'Inserting data into: bronze.crm_sales_details'
     BULK INSERT bronze.crm_sales_details
     FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
     WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
   PRINT '>>-----------------------------------';

	PRINT '=====================================================================';
	PRINT 'Loading ERP Layer';
	PRINT '=====================================================================';

    SET @start_time = GETDATE();
	PRINT ' >> Truncating Table : bronze.erp_CUST_AZ12'
    TRUNCATE TABLE bronze.erp_CUST_AZ12;

    PRINT 'Inserting data into : bronze.erp_CUST_AZ12'
    BULK INSERT bronze.erp_CUST_AZ12
    FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>>-----------------------------------';

   SET @start_time = GETDATE();
   PRINT '>> Truncating Table : bronze.erp_LOC_A101'
   TRUNCATE TABLE bronze.erp_LOC_A101;

   PRINT 'Inserting data into : bronze.erp_LOC_A101'
   BULK INSERT bronze.erp_LOC_A101
   FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
   WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>>-----------------------------------';



  SET @start_time = GETDATE();
  PRINT '>> Truncating Table : bronze.erp_PX_CAT_G1V2'
  TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

  PRINT 'Inserting data into : bronze.erp_PX_CAT_G1V2'
  BULK INSERT bronze.erp_PX_CAT_G1V2
  FROM 'D:\Green Technologies\Data with Bara\SQL Data Warehouse project files\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
  WITH (
	  FIRSTROW =2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
);
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time , @end_time) AS NVARCHAR) + 'seconds';
    PRINT '>>---------';

	SET @batch_end_time = GETDATE ();
	PRINT '============================================'
	PRINT 'Loading Bronze Layer is completed';
	PRINT '>> Total Load Duration: ' + CAST( DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '============================================'
END TRY
BEGIN CATCH	
     PRINT '=============================================================='
	 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	 PRINT 'Error Message' +  ERROR_MESSAGE    ()             ;
	 PRINT 'Error Message' +  CAST(ERROR_NUMBER() AS NVARCHAR);
	 PRINT 'Error Message' +  CAST(ERROR_STATE () AS NVARCHAR);
	 PRINT '=============================================================='
END CATCH
END
GO

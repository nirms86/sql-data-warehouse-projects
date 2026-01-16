/*
===================================================================================
DDL Script: Create Bronze Tables
===================================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables
if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
===================================================================================
*/ I

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info 
(
cst_id              INT,
cst_key             NVARCHAR(50),
cst_firstname       NVARCHAR(50),
cst_lastname        NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr            NVARCHAR(50),
cst_create_date     DATE
);


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info 
(
prd_id       INT,
prd_key      NVARCHAR (50),
prd_nm       NVARCHAR (50),
prd_cost     INT,
prd_line     NVARCHAR (50),
prd_start_dt DATE,
prd_end_dt   DATE
);


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details 
(
sls_ord_num   NVARCHAR(50),
sls_prd_key   NVARCHAR(50),
sls_cust_id   INT,
sls_order_dt  DATE,
sls_ship_dt   DATE,
sls_due_dt    DATE,
sls_sales     INT,
sls_quantity  INT,
sls_price     INT
);



IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12 
(
CID   NVARCHAR (50),
BDATE NVARCHAR (50),
GEN   NVARCHAR(50)
);



IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101
(
CID   NVARCHAR (50),
CNTRY NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2
(
ID          NVARCHAR (50),
CAT         NVARCHAR (50),
SUBCAT      NVARCHAR (50),
MAINTENANCE NVARCHAR (50)
);

-- Build Bronze Layer
-- Develop SQL Load Scripts



EXEC bronze.load_bronze

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

/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
     This script creates a new database named 'DataWarehouse' after checking if it already exists.
     If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
   Running this script will drop the entire 'DataWarehouse' database if it exists.
   All data in the database will be permanently deleted. Proceed with caution
   and ensure you have proper backups before running this script.
*/

-- Create Database 'Data Warehouse'

USE master;
GO

-- Drop and recreate the 'Datawarehuse' database.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE Datawarehouse;

END
GO;
-- CREATE the 'Datawarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE Datawarehouse;
GO
-- use "GO" to seperate batches when working with multiple SQL statements. 

  -- CREATE Schemas
CREATE SCHEMA bronze; 
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

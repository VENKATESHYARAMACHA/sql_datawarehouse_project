/*
----------------------------------------------------------------------------------------------
                                Create Database and Schemas
----------------------------------------------------------------------------------------------
Script Purpose:
	This script creates a new Database named 'DataWareHouse' after checking if it already exists.
	If the database exists, it is dropped and reacreated. Additionally, the script sets up three 
	schemas with in database: 'bronze','silver' and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWareHouse' database if it exists.
	All the data in the Database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop and recreate the  'DataWareHouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

--Create the 'DataWareHouse' Database:
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

--Create schemas:
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
GO

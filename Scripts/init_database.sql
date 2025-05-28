/*
 * This script is used to create the data base for the datawarehouse project
 * Medallion Architecture is used as the database architecture here
 * We have created three schemas
 * 
 * first schema, BRONZE is the staging layer
 * where we will pull the data from the sources by using batch processing method, no modelling or transformations will be done here and we wil use full load
 * 
 * second schema is the silver layer here we have used batch processing as the extraction method and full load method, all the transformation will be done here.
 * 
 * Third is the gold schema
 */




-- CHECKING WHETHER THE DATABASE EXISTS, IF IT DOES THAN DROP IT BECAUSE WE ARE CREATING A NEW ONE AFTER IT

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DWH')
BEGIN
    DROP DATABASE DWH;
END;

--CREATING DATABASE

CREATE DATABASE DWH;

-- USING DWH

USE DWH;


--CREATE SCHEMAS

CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;

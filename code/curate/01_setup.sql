USE ROLE SYSADMIN;

--Processed layer Schema creation

CREATE SCHEMA IF NOT EXISTS SUMMIT_23_PROCESSED_DB.CUSTOMER;
CREATE SCHEMA IF NOT EXISTS SUMMIT_23_PROCESSED_DB.INVOICE;

--Curated layer Schema creation


CREATE SCHEMA IF NOT EXISTS SUMMIT_23_CURATED_DB.SALES_PREDICTION;

-- COMMON_DB Setup

CREATE DATABASE IF NOT EXISTS SUMMIT_23_COMMON_DB;
CREATE SCHEMA IF NOT EXISTS SUMMIT_23_COMMON_DB.DL_CORE;

--create STAGE for python libs
CREATE STAGE IF NOT EXISTS SUMMIT_23_COMMON_DB.DL_CORE.common_libs_stage;

--EVENT TABLE CREATION

CREATE EVENT TABLE SUMMIT_23_COMMON_DB.DL_CORE.LOG_EVENTS;

-- Enable Event table

USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT SET EVENT_TABLE = SUMMIT_23_COMMON_DB.DL_CORE.LOG_EVENTS;
ALTER ACCOUNT SET LOG_LEVEL = INFO;
SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;

--Create Email integration 

CREATE NOTIFICATION INTEGRATION notify_email
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('naga.mahadevan@snowflake.com','paul.needleman@snowflake.com'); -- replace the email with your email details.

GRANT USAGE ON INTEGRATION notify_email TO ROLE SYSADMIN;    

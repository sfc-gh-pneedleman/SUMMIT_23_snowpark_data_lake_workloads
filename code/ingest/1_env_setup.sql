---------------------------------------------------
-- ENVIRONMENT SETUP
--create the three databases,  necessary schemas and WH
---------------------------------------------------
BEGIN
--raw
  CREATE DATABASE IF NOT EXISTS SUMMIT_23_RAW_DB ;
  CREATE SCHEMA IF NOT EXISTS SUMMIT_23_RAW_DB.BILLING;
  CREATE SCHEMA IF NOT EXISTS SUMMIT_23_RAW_DB.CUSTOMER_360;
  CREATE SCHEMA IF NOT EXISTS SUMMIT_23_RAW_DB.SALES;
  --curated
  CREATE DATABASE IF NOT EXISTS SUMMIT_23_CURATED_DB;
  --processed 
  CREATE DATABASE IF NOT EXISTS SUMMIT_23_PROCESSED_DB;

  --create XS, 2XL warehouse and L-Snowpark optimized warehouse
  CREATE WAREHOUSE IF NOT EXISTS COMPUTE_XS_WH WAREHOUSE_SIZE = 'X-Small';
  CREATE WAREHOUSE IF NOT EXISTS COMPUTE_2XL_WH WAREHOUSE_SIZE = '2X-Large';
  CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH WAREHOUSE_SIZE = 'Large'   WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED';

  --Create a customer stage to land the files
   CREATE STAGE IF NOT EXISTS CUSTOMER_360.CUSTOMER_DATA_STAGE; 

  --create a billing invoice stage to land PDFs 
   CREATE STAGE IF NOT EXISTS BILLING.PDF_FILE_STAGE
    directory = (enable = true )
    ENCRYPTION = (TYPE =  'SNOWFLAKE_SSE');

  --create a transaction history stage 
  CREATE STAGE IF NOT EXISTS SALES.TRANSACTION_DATA_STAGE;

  --create file format for parquet
  CREATE FILE FORMAT IF NOT EXISTS CUSTOMER_360.PARQUET_FILE_FORMAT
	TYPE = parquet;

  --creata file format for json
  CREATE FILE FORMAT IF NOT EXISTS SALES.json_file_format 
  TYPE = json;

END;


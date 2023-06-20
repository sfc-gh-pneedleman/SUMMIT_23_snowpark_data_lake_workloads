---------------------------------------------------
-- ENVIRONMENT SETUP
--create the three databases,  necessary schemas and WH
---------------------------------------------------
USE ROLE SYSADMIN;
--raw
CREATE DATABASE SUMMIT_23_RAW_DB;
CREATE SCHEMA BILLING;
CREATE SCHEMA CUSTOMER_360;
CREATE SCHEMA SALES;
--curated
CREATE DATABASE SUMMIT_23_CURATED_DB;
--processed 
CREATE DATABASE SUMMIT_23_PROCESSED_DB;

--create XS and XL warehouse
CREATE WAREHOUSE COMPUTE_XS_WH WAREHOUSE_SIZE = 'X-Small';
CREATE WAREHOUSE COMPUTE_2XL_WH WAREHOUSE_SIZE = '2X-Large';

------------------------------------------
-- CUSTOMER
-- Start with loading the Customer Data
------------------------------------------
--there are various ways to put files onto the stage. I recomomnd using SnowSQL. Here is a sample command:
--  put file://customer_data/* @CUSTOMER_360.CUSTOMER_DATA_STAGE/customer_data/'

USE WAREHOUSE COMPUTE_XS_WH;
--once the data is loaded we can inter the schmea to create the table definition 
CREATE TABLE CUSTOMER_360.CUSTOMER USING TEMPLATE
(select array_agg(object_construct(*))
  from table(
    infer_schema(
      location=>'@CUSTOMER_360.CUSTOMER_DATA_STAGE/customer_data/ '
      , file_format=>'parquet_file_format'
      )
    ));


--we can now load the data based on the matched column names  
COPY INTO CUSTOMER_360.CUSTOMER
 from @CUSTOMER_DATA_STAGE/customer_data
 file_format = 'parquet_file_format'
 match_by_column_name = CASE_INSENSITIVE;


-------------------------------------------------------------------
-- INVOICE 
-- Load and Process PDF invoices 
------------------------------------------------------------------

--create stage for PDF files using directory param and encryption 
CREATE STAGE BILLING.PDF_FILE_STAGE
directory = (enable = true )
ENCRYPTION = (TYPE =  'SNOWFLAKE_SSE');

--note this command to upload PDFs will take about 10min for me given that 1GB of data going through my upload speed 
-- ##### RUN the below command via SnowSQL to uplaod data to your Stage #####
--put file://pdfs/* @billing.pdf_file_stage AUTO_COMPRESS=false;
--#######


--since this is a directory stage used to support unstructured data, we must refresh the directory structure 
ALTER STAGE BILLING.PDF_FILE_STAGE REFRESH;
/* lets review our objects and take a quick look at the stages */ 
LIST @CUSTOMER_360.CUSTOMER_DATA_STAGE;
LIST @BILLING.PDF_FILE_STAGE;

--create a function to parse the PDF into text
CREATE OR REPLACE function BILLING.pdf_to_text_udf(file_path string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pypdf2')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from PyPDF2 import PdfFileReader
from io import BytesIO
 
def main(file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        f = BytesIO(f.readall())
        reader = PdfFileReader(f)
        pageObj = reader.pages[0]
        text = pageObj.extractText()
        return text
$$;

USE WAREHOUSE COMPUTE_2XL_WH;

--use a 2XL warehouse to convert the 25,000 PDF text files into text (1.3GB)
--save all the PDF data along with the file name into a new table in its text format 
CREATE OR REPLACE TRANSIENT TABLE BILLING.PDF_RAW_TEXT AS (
select RELATIVE_PATH, pdf_to_text_udf(build_scoped_file_url(@BILLING.PDF_FILE_STAGE,RELATIVE_PATH)) as PDF_TEXT
from (
    select file_url,RELATIVE_PATH
    from directory(@BILLING.PDF_FILE_STAGE)
    group by file_url, RELATIVE_PATH
));

--suspend warehouse and switch back to XS
ALTER WAREHOUSE COMPUTE_2XL_WH SUSPEND;
USE WAREHOUSE COMPUTE_XS_WH;

--create a view to parse the text from the raw pdf text
 CREATE OR REPLACE VIEW BILLING.VW_PDF_RAW_TEXT AS 
(SELECT RELATIVE_PATH, PDF_TEXT, 
LPAD(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Customer:', 2), ' ', 2), 10, 0) as CUSTOMER_ID,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Invoice #:', 2), ' ', 2) as INVOICE_NUM,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Generated On:', 2), ' ', 2) as INV_GEN_DT,
TRIM(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Status:', 2), 'Payment', 1)) as INV_STATUS,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Payment Date:', 2), ' ', 2) as PAYMENT_DT,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 1', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_1,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 2', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_2,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 3', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_3,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Total', 2), ' ', 2)::number(38,2) as TOTAL
FROM PDF_RAW_TEXT);

--view the newly created view with the invoice data
SELECT * FROM BILLING.VW_PDF_RAW_TEXT LIMIT 10;

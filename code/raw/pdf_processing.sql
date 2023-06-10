--create a function to parse the PDF into text
CREATE OR REPLACE function pdf_to_text_udf(file_path string)
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

--use a 2XL warehouse to convert the 10,000 PDF text files into text
--save all the PDF data along with the file name into a new table in its text format 
CREATE OR REPLACE TRANSIENT TABLE PDF_RAW_TEXT AS (
select RELATIVE_PATH, pdf_to_text_udf(build_scoped_file_url(@PDF_FILE_STAGE,RELATIVE_PATH)) as PDF_TEXT
from (
    select file_url,RELATIVE_PATH
    from directory(@PDF_FILE_STAGE)
    group by file_url, RELATIVE_PATH
));


--create a view to parse the text from the raw pdf text
 CREATE OR REPLACE VIEW VW_PDF_RAW_TEXT AS 
(SELECT RELATIVE_PATH, PDF_TEXT, 
LPAD(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Customer:', 2), ' ', 2), 10, 0) as CUSTOMER_ID,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Invoice:', 2), ' ', 2) as INVOICE_NUM,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Generated On:', 2), ' ', 2) as INV_GEN_DT,
TRIM(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Status:', 2), 'Payment', 1)) as INV_STATUS,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Payment Date:', 2), ' ', 2) as PAYMENT_DT,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 1', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_1,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 2', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_2,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 3', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_3,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Total', 2), ' ', 2)::number(38,2) as TOTAL
FROM PDF_RAW_TEXT);

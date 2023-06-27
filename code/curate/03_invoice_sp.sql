CREATE OR REPLACE PROCEDURE SUMMIT_23_COMMON_DB.DL_CORE.INVOICE_PROCESS_SP()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lpad,split,call_function,lit
import logging

logger = logging.getLogger("processed_logger")

def main(session: snowpark.Session): 

    
    inv_list_df = session.sql ("select file_url,RELATIVE_PATH from directory(@SUMMIT_23_RAW_DB.BILLING.PDF_FILE_STAGE) group by file_url, RELATIVE_PATH")
    
    logger.info(f'Invoke UDF to parse PDF into RAW TEXT for INVOICES')
    
    raw_inv_df = inv_list_df.select(col("RELATIVE_PATH"),call_udf("SUMMIT_23_RAW_DB.BILLING.pdf_to_text_udf",call_function("build_scoped_file_url","@SUMMIT_23_RAW_DB.BILLING.PDF_FILE_STAGE",col("RELATIVE_PATH"))).alias("PDF_TEXT"))

    logger.info(f'Started standardization process for INVOICE')
    raw_inv_df.create_or_replace_temp_view("SUMMIT_23_RAW_DB.BILLING.PDF_RAW_TEXT")
    bill_df = session.sql ("SELECT RELATIVE_PATH, LPAD(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Customer:', 2), ' ', 2), 10, 0) as CUSTOMER_ID,SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Invoice #:', 2), ' ', 2) as INVOICE_NUM,SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Generated On:', 2), ' ', 2) as INV_GEN_DT,TRIM(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Status:', 2), 'Payment', 1)) as INV_STATUS,SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Payment Date:', 2), ' ', 2) as PAYMENT_DT,TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 1', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_1,TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 2', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_2,TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 3', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_3,SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Total', 2), ' ', 2)::number(38,2) as TOTAL FROM SUMMIT_23_RAW_DB.BILLING.PDF_RAW_TEXT")
    
    #bill_df = bill_df.with_column("CUSTOMER_ID",call_function("split_part",call_function("split_part",col("PDF_TEXT"),lit("Customer"),lit("2")),lit(" "),lit("2")))
    
    # Write parsed Invoice data to table in processed layer
    logger.info(f'INVOICE Raw text parseed into separate colums')
    targetTable = 'SUMMIT_23_PROCESSED_DB.INVOICE.INVOICE_DETAILS'
    logger.info(f'Started writing data to {targetTable} table')
    bill_df.write.mode("overwrite").save_as_table(targetTable)
    logger.info(f'Finshed writing data to {targetTable} table')
    
    return f'{targetTable} CREATED'
$$;

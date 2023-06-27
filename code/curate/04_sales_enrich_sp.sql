CREATE OR REPLACE PROCEDURE SUMMIT_23_COMMON_DB.DL_CORE.SALES_ENRICH_SP()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
imports =('@SUMMIT_23_COMMON_DB.DL_CORE.COMMON_LIBS_STAGE/common_libs/common_utils.py')
HANDLER = 'sales_enriched'
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,row_number
from snowflake.snowpark.window import Window 
from common_utils import send_email
import logging
from datetime import datetime

logger = logging.getLogger("curated_sales_logger")

def sales_enriched(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    sales_table = 'SUMMIT_23_PROCESSED_DB.SALES.TRANSACTION'
    customer_table = 'SUMMIT_23_PROCESSED_DB.CUSTOMER.CUSTOMER'
    invoice_table = 'SUMMIT_23_PROCESSED_DB.INVOICE.INVOICE_DETAILS'
    product_table = 'PRODUCT_VIEWS_AND_PURCHASES_ON_RETAIL_SITES_AND_MARKETPLACES.DATAFEEDS.PRODUCT_VIEWS_AND_PURCHASES'
    logger.info(f'Started reading tables : {sales_table}, {customer_table}, {product_table},{invoice_table} ')
    # Read Sales,Customer, Invoice and Product tables to dataframes
    sales_df = session.table(sales_table)
    cust_df = session.table(customer_table)
    inv_df = session.table(invoice_table)   
    prod_df = session.table(product_table)
    # create Sales_amt field using sales transaction data
    sales_df = sales_df.with_column("SALES_AMT",col("TXN_QUANTITY")*col("PRODUCT_UNIT_PRICE"))
    logger.info("created derived column Sales_amt")
    # Join Sales and customer dataframes to add customer demographic info
    sales_cust_df = sales_df.join(cust_df,"CUSTOMER_ID").select(sales_df.TXN_ID, sales_df.TXN_DATE, sales_df.TXN_QUANTITY, sales_df.PRODUCT_ID,
	                                                            sales_df.PRODUCT_DESC, sales_df.PRODUCT_UNIT_PRICE, sales_df.SALES_AMT,
                                                                sales_df.PAYMENT_METHOD, sales_df.CUSTOMER_ID, cust_df.ZIP, cust_df.FIRST_NAME,
                                                                cust_df.LAST_NAME, cust_df.CITY, cust_df.STATE, cust_df.COUNTRY)
    
    # select product metadata from marketplace dataset
    logger.info("combining sales transaction and customer data")
    prod_window = Window.partition_by(col("PRODUCT")).order_by(col("PRODUCT"))
    
    prod_df = prod_df.select(col("PRODUCT"),col("TITLE"),col("BRAND"),col("MAIN_CATEGORY"),col("SUB_CATEGORY"),
                            row_number().over(prod_window).alias("ROW_NUM"))
    prod_master_df = prod_df.filter(col("ROW_NUM")==1)
    #prod_master_df = prod_df.select(col("PRODUCT"),first_value(col("TITLE")).over(prod_window).alias("TITLE"),
    #                        first_value(col("BRAND")).over(prod_window).alias("BRAND"),
    #                         first_value(col("MAIN_CATEGORY")).over(prod_window).alias("MAIN_CATEGORY"),
    #                         first_value(col("SUB_CATEGORY")).over(prod_window).alias("SUB_CATEGORY")).distinct()
    logger.info("created product master data")
    #Combine Product master data with Sales transactions
    logger.info("combining Sales data with product")
    sales_txn = sales_cust_df.join(prod_master_df,sales_cust_df.PRODUCT_ID==prod_master_df.PRODUCT,join_type="left")
    sales_txn = sales_txn.distinct()
    #write final df to table
    target_table = 'SUMMIT_23_CURATED_DB.SALES_PREDICTION.PRODUCT_SALES'
    logger.info("started writing final dataset to curated layer")
    
    sales_txn.write.mode("overwrite").save_as_table(target_table)
    logger.info(f'Finished writing data to table: {target_table}')
    email_list = 'naga.mahadevan@snowflake.com, paul.needleman@snowflake.com'
    subject = f'Curation: {target_table} table load completed'
    body = f'PRODUCT SALES table successfully loaded on {datetime.now()}'
    result = send_email(session,email_list,subject,body)    
    return f'{target_table} table loaded'
$$;

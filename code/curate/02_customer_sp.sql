CREATE OR REPLACE PROCEDURE SUMMIT_23_COMMON_DB.DL_CORE.PROCESSED_SP(inTableName VARCHAR, inSchema VARCHAR,inDatabase VARCHAR,outTableName VARCHAR, outSchema VARCHAR,outDatabase VARCHAR )
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,regexp_replace,split,lit,when,startswith,substr
from snowflake.snowpark.functions import call_function,length
import logging

logger = logging.getLogger("processed_logger")


def main(session: snowpark.Session,inTableName, inSchema,inDatabase,outTableName, outSchema,outDatabase ): 
    
    tableName = ".".join((inDatabase,inSchema,inTableName))
    logger.info(f'Started standardization process for {tableName}')
    cust_df = session.table(tableName)
    logger.info(f'Reading data from {tableName} table to dataframe')
    cust_df = cust_df.with_column("WORK_PHONE",when( startswith (regexp_replace(col("work_phone"),'[^0-9_]'),lit("1")),substr(regexp_replace(col("work_phone"),'[^0-9_]'),2,10)).otherwise(substr(regexp_replace(col("work_phone"),'[^0-9_]'),1,10)))
    cust_df = cust_df.with_column("CELL_PHONE",when( startswith (regexp_replace(col("cell_phone"),'[^0-9_]'),lit("1")),substr(regexp_replace(col("cell_phone"),'[^0-9_]'),2,10)).otherwise(substr(regexp_replace(col("cell_phone"),'[^0-9_]'),1,10)))
    cust_df = cust_df.with_column("HOME_PHONE",when( startswith (regexp_replace(col("home_phone"),'[^0-9_]'),lit("1")),substr(regexp_replace(col("home_phone"),'[^0-9_]'),2,10)).otherwise(substr(regexp_replace(col("home_phone"),'[^0-9_]'),1,10)))
    cust_df = cust_df.with_column("FIRST_NAME",substr(col("NAME"),1,call_function("position",lit(" "),col("NAME"))-1)) \
                     .with_column("LAST_NAME",substr(col("NAME"),call_function("position",lit(" "),col("NAME"))+1,length(col("NAME"))))
    cust_df = cust_df.with_column_renamed(col("POSTCODE"),"ZIP")
    
    final_df = cust_df.select(col("CUSTOMER_ID"),col("FIRST_NAME"),col("LAST_NAME"),col("DOB"),col("JOB_TITLE"),col("COMPANY"),col("STREET"),col("CITY"),col("STATE"),col("ZIP"),col("COUNTRY"),col("HOME_PHONE"),col("CELL_PHONE"),col("WORK_PHONE"))
    logger.info(f'final dataframe with standardized columns are created')
    
    # Write dataframe to table.
    target_table = ".".join((outDatabase,outSchema,outTableName))
    logger.info(f'Started writing to Processed layer table: {target_table}')
    final_df.write.mode("overwrite").save_as_table(target_table)
    logger.info(f'Finished writing to Processed layer table: {target_table}')
    # Return processing completion
    return "TABLE LOADED"
$$;

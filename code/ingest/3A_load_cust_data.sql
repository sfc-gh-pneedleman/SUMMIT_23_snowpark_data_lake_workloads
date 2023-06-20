
--once the data is loaded we can inter the schmea to create the table definition. 
--notice no DDL is provided as the table is created based on the data within the stage
BEGIN

CREATE OR REPLACE TABLE CUSTOMER_360.CUSTOMER USING TEMPLATE
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


END;
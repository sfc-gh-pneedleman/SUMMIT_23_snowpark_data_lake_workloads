BEGIN 

--create table from data within JSON files. Only need to sample 1,000 records to get the schema 
CREATE OR REPLACE TABLE SALES.TXN_HISTORY
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@TRANSACTION_DATA_STAGE/txn_data/',
          FILE_FORMAT=>'json_file_format'
        )
      )LIMIT 1000);

    --load the txn history data based on column name
    COPY INTO SALES.TXN_HISTORY
    from @TRANSACTION_DATA_STAGE/txn_data/
    file_format = 'json_file_format'
    match_by_column_name = CASE_INSENSITIVE;

END;
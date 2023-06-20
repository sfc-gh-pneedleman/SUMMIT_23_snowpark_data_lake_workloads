CREATE OR REPLACE TRANSIENT TABLE BILLING.PDF_RAW_TEXT AS (
select RELATIVE_PATH, pdf_to_text_udf(build_scoped_file_url(@BILLING.PDF_FILE_STAGE,RELATIVE_PATH)) as PDF_TEXT
from (
    select file_url,RELATIVE_PATH
    from directory(@BILLING.PDF_FILE_STAGE)
    group by file_url, RELATIVE_PATH
));
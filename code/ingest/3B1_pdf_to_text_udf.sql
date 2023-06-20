CREATE FUNCTION IF NOT EXISTS BILLING.pdf_to_text_udf(file_path string)
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
import os
import snowflake.connector
from os.path import join, dirname
from dotenv import load_dotenv

#ensure that dotenv is installed via pip install python-dotenv
# .env file configured in the same directory as this file with your snowflake connection params
#    e.g., SF_DATABASE   = 'SUMMIT_23_RAW_DB' 
dotenv_path = join(dirname(__file__),'.env')
load_dotenv(dotenv_path)

ctx = snowflake.connector.connect(
    authenticator="snowflake",
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    database=os.getenv("SF_DATABASE"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    role=os.getenv('SF_ROLE')
    )

#create a Connection from connector 
cs = ctx.cursor()

#function to execute scripts from a file 
def executeScriptsFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    try:
        #execute the script from the file
        cs.execute(sqlFile)        
    except Exception as e:
        raise e
            
    
## #######################################################
## Begin code execution 
## #######################################################

## #########################################################
## 1. ENV SETUP: create DB, Schemas and Warehouse and stages
## #########################################################
executeScriptsFromFile('1_env_setup.sql')
print ('Step 1: Env Steup Complete.')

## #######################################################
## 2. LOAD FILES: Move data from local directory to Stages 
## #######################################################
cust_file_path = '../../data/customer/*'
inv_file_path =  '../../data/invoice/*'
# put CUSTOMER files from local to the stage: note this will take some timee
print ('Begin Step 2A: Uploading customer files to stage. Note this will take some time (>20 min) due to the large volume of data')
customer_put_stmt = f'put file://{cust_file_path} @CUSTOMER_360.CUSTOMER_DATA_STAGE/customer_data/'
cs.execute(customer_put_stmt)
print ('Step 2A: Customer Data Successfully Loaded.')

# put INVOICE files from local to the stage: note this will take some timee
print ('Begin Step 2B: Uploading invoice PDF files to stage. Upload of PDFs will take > 10min due to 1GB of files')
inv_put_stmt = f'put file://{inv_file_path} @BILLING.pdf_file_stage AUTO_COMPRESS=false'
cs.execute(inv_put_stmt)
#since this is a directory stage used to spport unstructured data, we must refresh the directory structure 
cs.execute('ALTER STAGE BILLING.PDF_FILE_STAGE REFRESH')
print ('Step 2B: Invoice PDF Data Successfully Loaded.')

## #########################################################
## 3. LOAD DATA TO TABLES: Copy data from Stages into Tables
## #########################################################
## CUSTOMER ##
cs.execute("USE SCHEMA CUSTOMER_360")
print("Step 3A: Begin Load of Customer Data")
executeScriptsFromFile('3A_load_cust_data.sql')
print ('Step 3A: Customer Data Successfully Loadad into target table: CUSTOMER.')
## INVOICE ##
cs.execute("USE SCHEMA BILLING")
print("Step 3B: Begin Load of Invoice PDF Data Data")
#compile a Snowpark UDF to convert PDF files into text
executeScriptsFromFile('3B1_pdf_to_text_udf.sql')
#jump to a 2XL to scale the conversion of 1GB of files to text
cs.execute("USE WAREHOUSE COMPUTE_2XL_WH")
#save all the PDF data along with the file name into a new table in its text format 
executeScriptsFromFile("3B2_load_pdf_to_text.sql")
print ('Step 3B: Invoice PDF Data Successfully Loadad into target table: PDF_RAW_TEXT.')
#create a view to parse the PDF text
executeScriptsFromFile("3BC_create_raw_pdf_text_view.sql")
## SALE TRANSACTION HISTORY HISTORY ##
cs.execute("USE SCHEMA SALES")
print("Step 3C: Begin Load of Sale Transaction History Data")
executeScriptsFromFile('3C_load_txn_history.sql')
print ('Step 3C: Sale Transaction History Data Loadad into target table: TXN_HISTORY.')
#suspend the 2XL WH and scale down by using the XS WH
cs.execute("ALTER WAREHOUSE COMPUTE_2XL_WH SUSPEND")
cs.execute("USE WAREHOUSE COMPUTE_XS_WH")
# #########################################################
# 4. DATA VALIDATION
# #########################################################

#check row counts of newly created tables. should be 50M customers, 25k invoices and 500M txn
validation_sql = """SELECT  'CUSTOMER', COUNT(*) CNT FROM CUSTOMER_360.CUSTOMER 
                    UNION ALL 
                    SELECT  'INVOICE_PDFS', COUNT(*) CNT FROM BILLING.VW_PDF_RAW_TEXT 
                    UNION ALL
                    SELECT  'TXN_HISTORY', COUNT(*) CNT FROM SALES.TXN_HISTORY"""
cs.execute(validation_sql)
results = cs.fetchall()
for row in results:
    print ("Table and Row Count: " + str(row))

#print (results)

cs.close()
ctx.close()
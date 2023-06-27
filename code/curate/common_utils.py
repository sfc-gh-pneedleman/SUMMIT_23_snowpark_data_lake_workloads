import snowflake
from snowflake.snowpark.files import SnowflakeFile
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,row_number
from snowflake.snowpark.window import Window 
import re


def send_email(session,email_recipients, email_subject, email_body):
    email_int = 'notify_email'
    session.call('system$send_email',
        email_int,
        email_recipients,
        email_subject,
        email_body)
    return "email_sent"

def format_phone(in_phonenum):
    #strip non numeric charachters
    ph_num = re.sub('^[0-9]','',in_phonenum)
    clean_num = ph_num[1:10] if ph_num[0]== '1' else ph_num[0:9]
    return clean_num


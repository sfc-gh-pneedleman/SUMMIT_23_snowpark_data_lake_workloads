from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from datetime import datetime
import time
import random
import json
import string
from snowflake.snowpark import Session
from dotenv import load_dotenv
from os.path import join, dirname
import os


## connect to snowflake to get a list of customer_ids
dotenv_path = join(dirname(__file__),'../.env')
load_dotenv(dotenv_path)

connection_parameters = {
    "account": os.getenv("SF_ACCOUNT"),
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "role": os.getenv("SF_ROLE"),  
    "warehouse": os.getenv("SF_WAREHOUSE"),  
    "database": os.getenv("SF_DATABSE"), 
    "schema": "CUSTOMER_360",  
  }  

#create a session
session = Session.builder.configs(connection_parameters).create()  

#get list of customer IDs
df = session.sql("SELECT CUSTOMER_ID FROM SUMMIT_23_RAW_DB.CUSTOMER_360.CUSTOMER LIMIT 5000000")
df = df.to_pandas()
#convert df to list of customer ids
customer_list = df.values.tolist()

#create list of payment Methods 
payment_list = ['VISA',  'MASTERCARD', 'AMEX', 'DISCOVER', 'GIFT_CARD']

################################
## MESSAGE CONFIG SETTINGS 
##
## number of messages to push
num_messages = 10000
##
## delay between messages in sec
delay=.01 #10 MS or 100 per sec
##
##################################

if __name__ == '__main__':
     # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('topic_name', type=str)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    ## which topic to push to
    topic = args.topic_name

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        #else:
            #print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            #    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    #random date generator function
    def random_date(seed):
        random.seed(seed)
        d = random.randint(1, int(time.time()))
        return datetime.fromtimestamp(d).strftime('%Y-%m-%d')


    
    count = 0
    for _ in range(num_messages):

        json_raw = {
        "txn_id": ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() + "%0.11d" % random.randint(1,99999999999),
        "txn_date": datetime.now().strftime("%m/%d/%Y %I:%M:%S.%f %p"),
        "txn_quantity": random.randint(1, 30),
        "customer_id":  ''.join(random.choice(customer_list)),
        "product_id": ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() + str(random.randint(1, 9)) +'-' + "%0.7d" % random.randint(1,9999999) + ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() ,
        "product_unit_price": random.randint(100, 90000) / 100,
        "product_desc": ''.join(random.choices(string.ascii_lowercase, k=random.randint(6, 20))).capitalize(),
        "payment_method": ''.join(random.choice(payment_list)),
        }
        json_data = json.dumps(json_raw)
        key=str(count)
     
        producer.produce(topic, json_data, key, callback=delivery_callback)
        count += 1
        time.sleep(delay)


    # Block until the messages are sent.
    #producer.poll(10000)
    producer.flush()
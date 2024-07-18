import json
from data_access.mongodb import Mongo_Client
from constant import *
import boto3
from datetime import datetime
import logging
import requests
import uuid

data_source_url = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=<to_date>&date_received_min=<from_date>&field=all&format=json"

mongodb_collection = Mongo_Client()

s3 = boto3.resource('s3')

def get_from_to_date():

    latest_record = mongodb_collection.find_one(sort=[('_id', -1)])

    if latest_record is not None:
        from_date = latest_record['to_date']
        to_date = datetime.now().strftime("%Y-%m-%d")

    else:
        from_date = '2023-01-01'
        to_date = datetime.now().strftime("%Y-%m-%d")

    return from_date, to_date


def save_from_to_date(from_date,to_date):
    mongodb_collection.insert_one({'from_date': from_date, 'to_date': to_date})
    logging.info("Modifed from_date and to_date updated to Mongo DB")



def lambda_handler(event, context):

    from_date, to_date = get_from_to_date()
    logging.info(f"from_date: {from_date}, to_date: {to_date}")

    if from_date==to_date:
        return {
            'statusCode': 200,
            'body': json.dumps('The Data Pipeline has already run, No new data to load')
        }
    
    data_url = data_source_url.replace('<to_date>', to_date).replace('<from_date>', from_date)

    response = requests.get(data_url, params={'User-agent': f'your bot {uuid.uuid4()}'})

    finance_data = list(map(lambda x: x["_source"],filter(lambda x: "_source" in x.keys(),json.loads(response.content))))

    s3object = s3.Object(S3_BUCKET_NAME, f"data/{from_date.replace('-','_')}_{to_date.replace('-','_')}_finance_data.json")

    s3object.put(
        Body=(bytes(json.dumps(finance_data).encode('UTF-8')))
    )

    logging.info("uploaded the data into S3 Bucket")


    save_from_to_date(from_date, to_date)

    logging.info('Saved the latest from and to date into Mongo DB')


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

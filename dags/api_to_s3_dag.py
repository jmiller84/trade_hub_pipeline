import boto3
import os
import datetime
import sys
import yfinance as yf
parent_dir = "/Users/joemiller/Documents/data_engineering/trade_hub_pipeline/trade_hub_pipeline/dags"
parent_dir = os.path.dirname(os.path.dirname(parent_dir))
sys.path.append(parent_dir)
from trade_hub_pipeline.companies_list import companies_list


# Create a session using your AWS credentials
session = boto3.Session(
    os.environ.get('aws_access_key'),
    os.environ.get('aws_secret_key')
)

s3 = session.client('s3')
bucket_name = "trade-hub-bucket"


for company in companies_list:

    symbol = company[0]

    # Download the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=f"{symbol}.csv")
    file_content = response['Body'].read().decode('utf-8')
    last_line = file_content.splitlines()[-1]
    last_row = last_line.split(',')

    # Get the last recorded date and convert to date object 
    last_recorded_date = datetime.datetime.strptime(last_row[0], "%Y-%m-%d").date()
    last_recorded_plus_one = last_recorded_date + datetime.timedelta(days=1)
    # Get current date as date object
    today = datetime.datetime.today().date()
    tomorrow = today + datetime.timedelta(days=1)

    # Check last recorded date is less than current date and current date is a week day as markets are closed on weekends
    if last_recorded_date<today and today.weekday() <5:
        new_data  = yf.download(symbol, last_recorded_plus_one, tomorrow)
        # filter results to ensure we are not getting duplicate data
        new_data = new_data[new_data.index >= last_recorded_plus_one]
        # Append the new stock data to the csv file content
        file_content += ','.join(map(str, new_data)) + '\n'
        # Upload the updated CSV file back to S3
        s3.put_object(Bucket=bucket_name, Key=f"{symbol}.csv", Body=file_content)

    else:
        print("No new data to append")


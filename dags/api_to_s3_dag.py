import boto3
import os
import datetime
import sys
import yfinance as yf
import pandas as pd
import io
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

    if file_content:
            # Convert csv data into DataFrame
            df = pd.read_csv(io.StringIO(file_content), index_col=False)

            # Get the last recorded date and convert to date object
            last_recorded_date = pd.to_datetime(df['Date'].iloc[-1]).date()
            last_recorded_plus_one = last_recorded_date + datetime.timedelta(days=1)

            # Get current date as date object
            today = datetime.datetime.today().date()
            tomorrow = today + datetime.timedelta(days=1)

            # Check if new data needs to be appended
            # if last_recorded_date < today and today.weekday() < 5:
            if last_recorded_date < today:
                # Download new stock data
                new_data = yf.download(symbol, last_recorded_plus_one, tomorrow)

                # Filter new data to exclude duplicate entries
                new_data = new_data[new_data.index >= pd.Timestamp(last_recorded_plus_one)]
                new_data.reset_index(drop=False, inplace=True)
                new_data['Date'] = df['Date'] = df['Date'].astype('str')

                # Append new data to DataFrame
                df = pd.concat([df, new_data], ignore_index=True)

                # Convert DataFrame back to CSV format
                updated_csv = df.to_csv(index=False)

                # Upload the updated CSV file back to S3
                s3.put_object(Bucket=bucket_name, Key=f"{symbol}.csv", Body=updated_csv)
                print(f"New data appended to {symbol}.csv")
            else:
                print(f"No new data to append to {symbol}.csv")
    else:
        print(f"No existing data found for {symbol}.csv")





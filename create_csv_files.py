import boto3
import os
import datetime
from datetime import date
from companies_list import companies_list
import yfinance as yf
import pandas as pd
from loguru import logger

# Configure logging
logger.add("create_csv.log", rotation="500 MB", level="INFO") 

# Create a session using your AWS credentials
session = boto3.Session(
    os.environ.get('aws_access_key'),
    os.environ.get('aws_secret_key')
)

# Create an S3 client
s3 = session.client('s3')
bucket_name = 'trade-hub-bucket'


start_date = date.today() - datetime.timedelta(days=365)
end_date = date.today()


for company in companies_list:
    symbol = company[0]
    company_name = company[1]
    df  = yf.download(symbol,start_date, end_date)

    # Check that data has been downloaded
    if len(df) == 0:
        logger.error(f"{symbol} No stock data downloaded, symbol may be delisted")

    else:
        # Define the file name    
        file_name = f'{symbol}.csv'
        # Write df to CSV file
        df.to_csv(file_name, index=0)
        # Upload the file to S3, args= 1, path to csv file, 2, bucket name, file_name the csv will be saved as is in S3
        s3.upload_file(file_name, bucket_name, file_name)
        # Delete the local file after uploading it to S3
        os.remove(file_name)



# Testing script download stock data for single stock

# symbol = 'AAPL'
# company_name = 'Apple Stock'
# df  = yf.download(symbol,start_date, end_date)

# # Check that data has been downloaded
# if len(df) == 0:
#     logger.error(f"{symbol} No stock data downloaded, symbol may be delisted")

# else:
#     print(df)

import boto3
import os
import csv



# Create a session using your AWS credentials
session = boto3.Session(
    os.environ.get('aws_access_key'),
    os.environ.get('aws_secret_key')
)

# Create an S3 client
s3 = session.client('s3')

# Define your CSV data
csv_data = [
    {'Name': 'John', 'Age': 30},
    {'Name': 'Jane', 'Age': 25},
    {'Name': 'Alice', 'Age': 35}
]

# Define the bucket and file name
bucket_name = 'trade-hub-bucket'
file_name = 'example.csv'

# Write CSV data to a file
with open(file_name, 'w', newline='') as csvfile:
    fieldnames = ['Name', 'Age']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)

# Upload the file to S3
s3.upload_file(file_name, bucket_name, file_name)

# Delete the local file after uploading it to S3
os.remove(file_name)

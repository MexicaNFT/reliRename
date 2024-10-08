import os
import csv
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm  # Import tqdm for progress bars

# Load environment variables from .env file
load_dotenv()

# Setup logging for both INFO and ERROR levels
logging.basicConfig(filename='dynamo_s3_check.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# AWS credentials and table name from .env
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Define the folder path containing the CSV files
csv_folder_path = "../../csv_done"

# Initialize DynamoDB and S3 resources
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Access the DynamoDB table
table = dynamodb.Table(TABLE_NAME)

# Function to check if a file exists in the "txt" folder in S3
def check_file_in_s3(filename):
    try:
        # Files are stored under the 'txt/' folder in S3
        s3_key = f'txt/{filename}.txt'
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '403':
            logging.error(f"Access denied to S3 for file: {filename}")
        elif e.response['Error']['Code'] == '404':
            logging.error(f"Missing file in S3: {filename}.txt")
            return False
        else:
            logging.error(f"Error checking S3 for file {filename}: {e}")
        return False

# Function to check existence in DynamoDB and S3
def check_existence(csv_folder_path):
    csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith(".csv")]

    for csv_file_name in tqdm(csv_files, desc="Processing CSV files"):
        csv_file_path = os.path.join(csv_folder_path, csv_file_name)
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            row_num = 0

            # Count rows for tqdm to work properly
            row_count = sum(1 for _ in open(csv_file_path)) - 1  # Minus 1 for the header

            # Iterate through each row in the CSV
            for row in tqdm(reader, desc=f"Checking rows in {csv_file_name}", total=row_count):
                row_num += 1
                law_id = row['Id']  # Adjust based on your CSV headers

                # Check DynamoDB for the law ID
                try:
                    response = table.get_item(Key={'id': law_id})

                    if 'Item' not in response:
                        logging.error(f"File: {csv_file_name}, Row: {row_num} - Missing in DynamoDB: {law_id}")
                    else:
                        logging.info(f"File: {csv_file_name}, Row: {row_num} - Found in DynamoDB: {law_id}")

                    # Check S3 for the corresponding file
                    if not check_file_in_s3(law_id):
                        logging.error(f"File: {csv_file_name}, Row: {row_num} - Missing file in S3: {law_id}.txt")
                    else:
                        logging.info(f"File: {csv_file_name}, Row: {row_num} - Found file in S3: {law_id}.txt")

                except Exception as e:
                    logging.error(f"File: {csv_file_name}, Row: {row_num} - Error checking law ID: {law_id}: {e}")

# Run the check
check_existence(csv_folder_path)
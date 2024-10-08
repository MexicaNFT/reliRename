import os
import csv
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm  # Import tqdm for progress bars

# Load environment variables from .env file
load_dotenv()

# Set up logging for errors to file
file_logger = logging.getLogger('file_logger')
file_handler = logging.FileHandler('dynamo_s3_check.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))
file_logger.addHandler(file_handler)

# Set up logging for debug info to console
console_logger = logging.getLogger('console_logger')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))
console_logger.addHandler(console_handler)

# AWS credentials and table name from .env
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

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

# Function to log errors to file
def log_file_error(csv_file_name, row_num, message):
    file_logger.error(f"File: {csv_file_name}, Row: {row_num} - {message}")

# Function to log info to console
def log_console_info(message):
    console_logger.info(message)

# Function to check if a file exists in the "txt" folder in S3
def check_file_in_s3(filename):
    try:
        # Files are stored under the 'txt/' folder in S3
        s3_key = f'txt/{filename}.txt'
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '403':
            log_console_info(f"Access denied to S3 for file: {filename}")
            log_file_error("N/A", "N/A", f"Access denied to S3 for file: {filename}")
        elif e.response['Error']['Code'] == '404':
            return False
        else:
            log_console_info(f"Error checking S3 for file {filename}: {e}")
            log_file_error("N/A", "N/A", f"Error checking S3 for file {filename}: {e}")
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
                        log_console_info(f"Missing in DynamoDB: {law_id}")
                        log_file_error(csv_file_name, row_num, f"Missing in DynamoDB: {law_id}")
                    else:
                        log_console_info(f"Found in DynamoDB: {law_id}")

                    # Check S3 for the corresponding file
                    if not check_file_in_s3(law_id):
                        log_console_info(f"Missing file in S3: {law_id}.txt")
                        log_file_error(csv_file_name, row_num, f"Missing file in S3: {law_id}.txt")
                    else:
                        log_console_info(f"Found file in S3: {law_id}.txt")

                except Exception as e:
                    log_console_info(f"Error checking law ID: {law_id}: {e}")
                    log_file_error(csv_file_name, row_num, f"Error checking law ID: {law_id}: {e}")

# Define the folder path containing the CSV files
csv_folder_path = "./csv_done"

# Run the check
check_existence(csv_folder_path)
import os
import pandas as pd
from datetime import datetime
import re
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm  # Import the tqdm library for progress bars

# Load environment variables from .env file
load_dotenv()

# Setup logging with INFO level
logging.basicConfig(filename='csv_validation.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# AWS credentials and bucket name from .env
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Define the folder path as a constant
FOLDER_PATH = './csv'

# Expected columns
EXPECTED_COLUMNS = ['jurisdiction', 'source', 'last_reform_date', 'title', 'Id']

# Function to convert date from DD/MM/YYYY to YYYY-MM-DD
def convert_date(date_str, filepath, row_index):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        logging.error(f"File: {filepath}, Row: {row_index + 1} - Invalid date format: {date_str}")
        return None

# Function to check if the URL is valid
def is_valid_url(url):
    regex = re.compile(r'^(?:http|ftp)s?://'  # http:// or https://
                       r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                       r'localhost|'  # localhost...
                       r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
                       r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
                       r'(?::\d+)?'  # optional port
                       r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None

# Function to check if a string represents a valid float with exactly five decimal places
def is_valid_id(value):
    try:
        float_value = float(value)
        # Check for exactly 5 decimal places
        return re.match(r'^\d+\.\d{5}$', f'{float_value:.5f}') is not None
    except ValueError:
        return False

# Function to check if a file exists in the "txt" folder in S3
def check_file_in_s3(filename):
    try:
        # Files are stored under the 'txt/' folder in S3
        s3_key = f'txt/{filename}'
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        logging.error(f"Error checking S3 for file {filename}: {e}")
        return False

# Function to process each CSV file
def process_csv_file(filepath):
    try:
        df = pd.read_csv(filepath)

        # Check if all expected columns exist
        missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
        if missing_columns:
            logging.error(f"File: {filepath} - Missing columns: {', '.join(missing_columns)}")
            return
        
        # Show progress for rows in the current CSV file
        print(f"Processing file: {filepath}")
        for index, row in tqdm(df.iterrows(), total=len(df), desc=f'Checking rows in {os.path.basename(filepath)}'):
            # Check jurisdiction (non-empty)
            if pd.isna(row['jurisdiction']) or not isinstance(row['jurisdiction'], str):
                logging.error(f"File: {filepath}, Row: {index + 1} - Invalid jurisdiction")

            # Check source (URL)
            if pd.isna(row['source']) or not is_valid_url(row['source']):
                logging.error(f"File: {filepath}, Row: {index + 1} - Invalid source URL")

            # Check last_reform_date and convert to ISO 8601
            if pd.isna(row['last_reform_date']):
                logging.error(f"File: {filepath}, Row: {index + 1} - Missing last_reform_date")
            else:
                iso_date = convert_date(row['last_reform_date'], filepath, index)
                if iso_date:
                    df.at[index, 'last_reform_date'] = iso_date

            # Check title (must be in all caps)
            if pd.isna(row['title']):
                logging.error(f"File: {filepath}, Row: {index + 1} - Missing title")
            elif row['title'] != row['title'].upper():
                df.at[index, 'title'] = row['title'].upper()

            # Check Id (should be a float with exactly five decimal places)
            if pd.isna(row['Id']):
                logging.error(f"File: {filepath}, Row: {index + 1} - Missing Id")
            elif not is_valid_id(row['Id']):
                logging.error(f"File: {filepath}, Row: {index + 1} - Invalid Id format: {row['Id']}")
            else:
                # Ensure that the Id is saved as a float with 5 decimal places
                df.at[index, 'Id'] = f'{float(row["Id"]):.5f}'

                # Check if the corresponding file exists in S3
                s3_file = f'{df.at[index, "Id"]}.txt'
                if not check_file_in_s3(s3_file):
                    logging.error(f"File: {filepath}, Row: {index + 1} - Missing file in S3: {s3_file}")

        # Add the "file" column with the Id and ".txt" appended
        df['file'] = df['Id'].apply(lambda x: f'{x}.txt')

        # Sort columns alphabetically before saving the corrected file
        df = df[sorted(df.columns)]

        # Save the corrected file
        corrected_filepath = filepath.replace(".csv", "_corrected.csv")
        df.to_csv(corrected_filepath, index=False)
        print(f"Processed and saved corrected file: {corrected_filepath}")

    except Exception as e:
        logging.error(f"Error processing file {filepath}: {str(e)}")

# Function to process all CSV files in a folder
def process_folder(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    
    # Show progress for processing each file
    for filename in tqdm(csv_files, desc="Processing CSV files"):
        filepath = os.path.join(folder_path, filename)
        process_csv_file(filepath)

# Call the function to process the folder
process_folder(FOLDER_PATH)
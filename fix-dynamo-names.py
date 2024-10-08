import os
import boto3
import csv
import random
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS credentials and table name from .env
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME')
csv_folder_path = "./csv"

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Access the DynamoDB table
table = dynamodb.Table(TABLE_NAME)

# Log file to track changes and for rollback purposes
log_file = 'dynamo_update_log_final_run.txt'

def log_message(message):
    with open(log_file, 'a') as log:
        log.write(message + '\n')
    print(message)

# Function to perform final run and update titles
def final_run(csv_folder_path):
    log_message("Starting final run.")
    
    for csv_file_name in os.listdir(csv_folder_path):
        if csv_file_name.endswith(".csv"):
            csv_file_path = f"{csv_folder_path}/{csv_file_name}"
            with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                update_count = 0
                skipped_count = 0
                error_count = 0

                # Iterate through each row in the CSV
                for row in reader:
                    law_id = row['Id']  # Adjust based on your CSV headers
                    correct_title = row['title']  # Adjust based on your CSV headers

                    # Check if the title contains a comma
                    if ',' not in correct_title:
                        # Log that the row is being skipped based on the comma logic
                        skipped_count += 1
                        log_message(f"Skipped law ID: {law_id}, title has no commas: '{correct_title}'")
                        continue

                    try:
                        # Fetch the current item from DynamoDB
                        log_message(f"Querying DynamoDB for law ID: {law_id}")
                        response = table.get_item(Key={'id': law_id})

                        if 'Item' not in response:
                            log_message(f"Law ID {law_id} not found in DynamoDB")
                            continue

                        db_item = response['Item']
                        db_title = db_item['name']

                        # Log the current state for rollback purposes
                        log_message(f"Current state for rollback: Law ID: {law_id}, Original Title: '{db_title}'")

                        # Compare titles and perform the update if necessary
                        if db_title != correct_title:
                            log_message(f"Updating law ID: {law_id} from '{db_title}' to '{correct_title}'")

                            # Perform the update in DynamoDB
                            table.update_item(
                                Key={'id': law_id},
                                UpdateExpression='SET #n = :t',
                                ExpressionAttributeNames={'#n': 'name'},
                                ExpressionAttributeValues={':t': correct_title}
                            )

                            log_message(f"Updated law ID: {law_id} with correct title: '{correct_title}'")
                            update_count += 1

                    except Exception as e:
                        log_message(f"Error processing law ID: {law_id}: {e}")
                        error_count += 1

                log_message(f"File processed: {csv_file_name}. Updates: {update_count}, Skipped: {skipped_count}, Errors: {error_count}")

    log_message("Final run completed.")


final_run(csv_folder_path)
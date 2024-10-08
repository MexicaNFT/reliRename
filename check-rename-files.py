import csv
import os

csv_file_path = "/Users/pridapablo/Downloads/5.csv"  # Replace with your CSV file path
text_column = "text"
id_column = "Id"
file_directory = "/Users/pridapablo/Downloads/5"  # Replace with the directory where your .txt files are stored

skipped_files = set()

def rename_files(csv_file_path, text_column, id_column, file_directory):

    used_ids = set()

    with open(csv_file_path, "r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            old_filename = row[text_column]
            new_id = row[id_column]

            # Skip the file if text starts with a number (integer)
            if old_filename[0].isdigit():
                print(f"Skipping file: {old_filename} as it starts with a number")
                skipped_files.add(old_filename)
                continue

            if new_id in used_ids:
                print(f"Skipping duplicate ID: {new_id} for file: {old_filename}")
                continue

            new_filename = f"{new_id}.txt"

            old_file_path = os.path.join(file_directory, old_filename)
            new_file_path = os.path.join(file_directory, new_filename)

            if os.path.exists(old_file_path):
                try:
                    os.rename(old_file_path, new_file_path)
                    used_ids.add(new_id)
                except Exception as e:
                    print(f"Error renaming {old_filename}: {str(e)}")
            else:
                print(f"File not found: {old_filename}")

def verify_renaming(file_directory, id_column, csv_reader):
    print("\nVerifying renamed files:")
    for row in csv_reader:
        new_filename = f"{row[id_column]}.txt"
        if new_filename in skipped_files:
            continue
        else:
            new_file_path = os.path.join(file_directory, new_filename)
            if os.path.exists(new_file_path):
                continue
            else:
                print(f"File missing: {new_filename}")

# Run the rename process
rename_files(csv_file_path, text_column, id_column, file_directory)

# Verify renamed files
with open(csv_file_path, "r", encoding="utf-8") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    verify_renaming(file_directory, id_column, csv_reader)
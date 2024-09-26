import csv
import os


def rename_files(csv_file_path, text_column, id_column, file_directory):

    used_ids = set()

    with open(csv_file_path, "r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            old_filename = row[text_column]
            new_id = row[id_column]

            if new_id in used_ids:
                print(f"Skipping duplicate ID: {new_id} for file: {old_filename}")
                continue

            new_filename = f"{new_id}.txt"

            old_file_path = os.path.join(file_directory, old_filename)
            new_file_path = os.path.join(file_directory, new_filename)

            if os.path.exists(old_file_path):
                try:

                    os.rename(old_file_path, new_file_path)
                    print(f"Renamed: {old_filename} -> {new_filename}")
                    used_ids.add(new_id)
                except Exception as e:
                    print(f"Error renaming {old_filename}: {str(e)}")
            else:
                print(f"File not found: {old_filename}")


def verify_renaming(file_directory, id_column, csv_reader):
    print("\nVerifying renamed files:")
    for row in csv_reader:
        new_filename = f"{row[id_column]}.txt"
        new_file_path = os.path.join(file_directory, new_filename)
        if os.path.exists(new_file_path):
            print(f"File exists: {new_filename}")
        else:
            print(f"File missing: {new_filename}")


csv_file_path = "/Users/sushant/Downloads/newtest - In Production Embedding Sets - Compendio Leyes Federales.csv"  # Replace with your CSV file path
text_column = "text"
id_column = "Id"
file_directory = "/Users/sushant/Downloads/Lyes"  # Replace with the directory where your .txt files are stored

rename_files(csv_file_path, text_column, id_column, file_directory)


with open(csv_file_path, "r", encoding="utf-8") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    verify_renaming(file_directory, id_column, csv_reader)

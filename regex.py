import os

# Define the folder path as a constant
FOLDER_PATH = '/Users/pridapablo/Downloads/INCONSITUCIONALIDAD'  # Replace this with your folder path

# Define the expected range of file numbers
START_NUMBER = 1
END_NUMBER = 952
PREFIX = "7."

def rename_files():
    # Get the list of files in the directory
    files = os.listdir(FOLDER_PATH)

    # Filter files that match the pattern "7.xxx.txt"
    filtered_files = [f for f in files if f.startswith(PREFIX) and f.endswith(".txt")]
    
    # Create a set of all expected files (7.00001.txt to 7.00952.txt)
    expected_files = {f"{PREFIX}{i:05d}.txt" for i in range(START_NUMBER, END_NUMBER + 1)}

    # Rename the files by adding two zeros after the first dot
    for file in filtered_files:
        parts = file.split(".")
        if len(parts) == 3 and parts[0] == "7":  # Ensure it's in the correct format (7.xxx.txt)
            old_number = int(parts[1])  # Extract the number part
            new_name = f"{PREFIX}{old_number:05d}.{parts[2]}"  # Add leading zeros
            old_file_path = os.path.join(FOLDER_PATH, file)
            new_file_path = os.path.join(FOLDER_PATH, new_name)
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {file} -> {new_name}")

    # Check for missing files
    existing_files = {f for f in os.listdir(FOLDER_PATH) if f in expected_files}
    missing_files = expected_files - existing_files

    if missing_files:
        print(f"Missing files: {missing_files}")
    else:
        print("All files are present.")

if __name__ == "__main__":
    rename_files()
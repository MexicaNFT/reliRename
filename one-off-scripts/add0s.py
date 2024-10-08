import os

# Folder path constant
FOLDER = '/Users/pridapablo/Downloads/txt_files'  # Update this path to your target folder

def rename_files_in_folder(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".txt"):
            # Split the filename into the base and extension
            base, ext = os.path.splitext(filename)
            
            try:
                # Split the base on the dot
                integer_part, decimal_part = base.split('.')
                
                # Pad the decimal part with leading zeros so that the total length is 5 characters
                padded_decimal_part = decimal_part.zfill(5)
                
                # Combine the integer part and the padded decimal part
                new_base = f"{integer_part}.{padded_decimal_part}"
                
                # Construct new file name and full file paths
                new_filename = f"{new_base}{ext}"
                old_path = os.path.join(folder, filename)
                new_path = os.path.join(folder, new_filename)
                
                # Rename the file
                os.rename(old_path, new_path)
                print(f"Renamed '{filename}' to '{new_filename}'")
            
            except ValueError:
                # Handle cases where filename doesn't split properly
                print(f"Skipping '{filename}' - not a valid number format.")

# Run the renaming function
rename_files_in_folder(FOLDER)
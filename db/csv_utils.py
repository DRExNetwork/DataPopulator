import os
import csv

def file_exists(filename):
    """Check if the specified file exists."""
    return os.path.exists(filename)

def save_ids_to_csv(ids, column_name, table_name, filename="output/saved_ids.csv"):
    """Save the given IDs to a CSV file."""
    with open(filename, "a", newline='') as file:
        writer = csv.writer(file)
        
        # Convert a single ID into a list for uniformity in processing
        if not isinstance(ids, list):
            ids = [ids]
        
        # Write each ID, its column name, and the associated table name to the CSV
        for id in ids:
            writer.writerow([id, column_name, table_name])

def read_ids_from_csv(filename="output/saved_ids.csv"):
    """Read the IDs and their details from a CSV file."""
    if file_exists(filename):
        with open(filename, "r") as file:
            reader = csv.reader(file)
            return [(int(row[0]), row[1], row[2]) for row in reader]

def delete_file(filename="output/saved_ids.csv"):
    """Delete the specified file if it exists."""
    if file_exists(filename):
        os.remove(filename)
    else:
        print(f"The file {filename} does not exist.")
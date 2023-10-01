import os
import json
from db.db_utils import *
from db.csv_utils import *

def load_json_data(filename):
    """Load data from a JSON file located in the 'data' directory."""
    with open(f"data/{filename}", "r") as file:
        return json.load(file)

def main():
    """Main function to handle database operations based on JSON data."""
    
    # Try to read and delete records based on saved IDs from the CSV.
    ids_with_details = read_ids_from_csv()
    if ids_with_details and delete_records_by_ids(ids_with_details):
        delete_file()

    # Load user and location data from JSON files.
    data_users = load_json_data("users.json")
    data_locations = load_json_data("locations.json")

    # Prepare the location data for insertion into the database.
    locations = [
        (
            d["country"], d["city"], d["district"], d["postal_code"]
        )
        for d in data_locations
    ]
    location_id = create_locations(locations[0])

    # Prepare the user data for insertion. Associate each user with the location ID.
    users = [
        (
            location_id, d["code"], d["is_solar_dev_role"], d["is_financier_role"],
            d["is_sponsor_role"], d["is_sme_role"], d["is_admin"], d["id_type"], d["phone"],
            d["name"], d["email"], d["contact_email"], d["password"], d["contact_phone"],
            d["profile_picture"]
        )
        for d in data_users
    ]
    
    # Insert user data and retrieve the associated user IDs.
    user_ids = create_users(users, True)

    # Save the returned user and location IDs to a CSV file.
    save_ids_to_csv(user_ids, "user_id", "users") 
    save_ids_to_csv(location_id, "location_id", "locations")

if __name__ == "__main__":
    main()

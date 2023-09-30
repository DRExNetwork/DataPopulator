import os
import json
from db.db_utils import *
from db.csv_utils import *


def cargar_datos_json(nombre_archivo):
    with open(f"data/{nombre_archivo}", "r") as file:
        return json.load(file)

def main():
    ids_with_details = read_ids_from_csv()
    if ids_with_details and delete_records_by_ids(ids_with_details):
        eliminar_archivo()


    data_users = cargar_datos_json("users.json")
    data_locations = cargar_datos_json("locations.json")

    locations = [
        (
            d["country"], d["city"], d["district"], d["postal_code"]
        )
        for d in data_locations
    ]
    location_id = create_locations(locations[0])

    users = [
        (
            location_id, d["code"], d["is_solar_dev_role"], d["is_financier_role"],
            d["is_sponsor_role"], d["is_sme_role"], d["is_admin"], d["id_type"], d["phone"],
            d["name"], d["email"], d["contact_email"], d["password"], d["contact_phone"],
            d["profile_picture"]
        )
        for d in data_users
    ]
    
    user_id = create_users(users, True)

    save_ids_to_csv(user_id, "user_id", "users") 
    save_ids_to_csv(location_id, "location_id", "locations")
    

if __name__ == "__main__":
    main()

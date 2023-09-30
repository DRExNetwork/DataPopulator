import os
import json
from db_utils import *
from dotenv import load_dotenv

load_dotenv()


def cargar_datos_json(nombre_archivo):
    with open(f"data/{nombre_archivo}", "r") as file:
        return json.load(file)

def main():

    data_users = cargar_datos_json("users.json")
    data_locations = cargar_datos_json("locations.json")

    locations = [
        (
            d["country"], d["city"], d["district"], d["postal_code"]
        )
        for d in data_locations
    ]
    
    users = [
        (
            create_locations(locations[0]), d["code"], d["is_solar_dev_role"], d["is_financier_role"],
            d["is_sponsor_role"], d["is_sme_role"], d["is_admin"], d["id_type"], d["phone"],
            d["name"], d["email"], d["contact_email"], d["password"], d["contact_phone"],
            d["profile_picture"]
        )
        for d in data_users
    ]
    create_users(users, True)

if __name__ == "__main__":
    main()

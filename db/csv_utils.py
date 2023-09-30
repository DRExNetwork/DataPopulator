import os
import csv

def archivo_existe(filename):
    """Verifica si un archivo existe."""
    return os.path.exists(filename)


def save_ids_to_csv(ids, column_name, table_name, filename="output/saved_ids.csv"):
    """ Guarda IDs en un archivo CSV. """
    with open(filename, "a", newline='') as file:
        writer = csv.writer(file)
        
        # Si "ids" no es una lista, lo convertimos en una
        if not isinstance(ids, list):
            ids = [ids]
        
        for id in ids:
            writer.writerow([id, column_name, table_name])


def read_ids_from_csv(filename="output/saved_ids.csv"):
    """ Lee los IDs y detalles desde un archivo CSV. """
    if archivo_existe(filename):
        with open(filename, "r") as file:
            reader = csv.reader(file)
            return [(int(row[0]), row[1], row[2]) for row in reader]
    


def eliminar_archivo(filename="output/saved_ids.csv"):
    """ Elimina un archivo si existe. """
    if archivo_existe(filename):
        os.remove(filename)
    else:
        print(f"El archivo {filename} no existe.")
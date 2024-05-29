import glob
import json
import os
import time

from hfile import HFile

def list_tables():
    # Inicia a contar el tiempo
    start = time.time()

    print("\nTABLE")
    # Escanea el directorio 'db' y busca los archivos '.hfile.json'
    tables = glob.glob('db/*.hfile.json')
    for table in tables:
        # Imprime el nombre de la tabla (todo antes de la extensión '.hfile.json')
        print(os.path.basename(table).replace('.hfile.json', ''))
    print(f"{len(tables)} row(s)")

    end = time.time()
    print(f"Took {end - start:.4f} seconds")
    print("=> ", [os.path.basename(table).replace('.hfile.json', '') for table in tables])
    
def create_table(table_name, column_families):
    # Crear una nueva tabla como un HFile
    table = HFile(table_name, column_families)
    # Guardar la tabla en disco
    table.save_to_file()
    
def put(table_name, row_key, column_family, column, value):
    # Cargar la tabla desde el archivo
    
    start = time.time()
    
    try:
        table = HFile.load_from_file(table_name)
    except FileNotFoundError:
        print(f"Error: La tabla '{table_name}' no existe.")
        return
    # Agregar una nueva entrada a la tabla
    table.put(row_key, column_family, column, value)
    # Guardar la tabla en disco
    table.save_to_file()
    
    end = time.time()

    print(f"Took {end - start:.4f} seconds")
    

def addColumnFamily(table_name, column_family):
    try:
        table = HFile.load_from_file(table_name)
    except FileNotFoundError:
        print(f"Error: La tabla '{table_name}' no existe.")
        return
    table.column_families.append(column_family)
    table.data[column_family] = []
    table.save_to_file()

def removeColumnFamily(table_name, column_family):
    try:
        table = HFile.load_from_file(table_name)
    except FileNotFoundError:
        print(f"Error: La tabla '{table_name}' no existe.")
        return
    if column_family not in table.column_families:
        print(f"Error: La familia de columnas '{column_family}' no existe en la tabla '{table_name}'.")
        return
    table.column_families.remove(column_family)
    del table.data[column_family]
    table.save_to_file()

def drop_table(table_name):
    try:
        with open(f"db/{table_name}.hfile.json", 'r') as file:
            table = json.load(file)
        if table['enabled']:
            print(f"Error: La tabla '{table_name}' está habilitada.")
            return
        os.remove(f"db/{table_name}.hfile.json")
    except FileNotFoundError:
        print(f"Error: La tabla '{table_name}' no existe.")
        return
    print(f"Table '{table_name}' deleted.")

def drop_all_tables():
    tables = glob.glob('db/*.hfile.json')
    for table in tables:
        with open(table, 'r') as file:
            table_data = json.load(file)
        if table_data['enabled']:
            print(f"Error: La tabla '{os.path.basename(table)}' está habilitada.")
            continue
        os.remove(table)
    print(f"{len(tables)} tablas eliminadas.")

def get(table_name, row_key):
    start = time.time()
    try:
        table = HFile.load_from_file(table_name)
    except FileNotFoundError:
        print(f"Error: La tabla '{table_name}' no existe.")
        return
    table.get(row_key)
    end = time.time()
    print(f"Took {end - start:.4f} seconds")



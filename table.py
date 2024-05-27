import glob
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
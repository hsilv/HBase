import bisect
from datetime import time
import json
import time

class Entry(list):
    def __lt__(self, other):
        return self[0] < other[0]
class HFile:
    def __init__(self, table_name, column_families):
        self.table_name = table_name
        self.column_families = column_families
        self.data = {cf: [] for cf in column_families}
        self.enabled = True
        self.metadata = {
            'num_entries': 0,
            'file_size': 0,
            'column_families': column_families,
        }

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False
    
    def is_enabled(self):
        return self.enabled
    
    def put(self, row_key, column_family, column, value):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        if column_family not in self.metadata['column_families']:
            print(f"\033[31mError: The column family '{column_family}' does not exist in the table '{self.table_name}'.\033[0m")
            return
        timestamp = int(time.time() * 1000)
        entry = {'row_key': row_key, 'column': column, 'timestamp': timestamp, 'value': value}

        # Buscar si el row_key y la columna ya existen
        for i, existing_entry in enumerate(self.data[column_family]):
            if isinstance(existing_entry, dict) and existing_entry['row_key'] == row_key and existing_entry['column'] == column:
                # Si existe, actualizar el registro
                self.data[column_family][i] = entry
                break
        else:
            # Si no existe, insertar un nuevo registro
            bisect.insort(self.data[column_family], entry, key=lambda x: (x['row_key'], x['column']))
            self.metadata['num_entries'] += 1

        self.metadata['file_size'] += len(str(entry))

    def save_to_file(self):
        filename = f"db/{self.table_name}.hfile.json"
        with open(filename, 'w') as f:
            json.dump({
                'enabled': self.enabled,
                'table_name': self.table_name,
                'data': self.data,
                'metadata': self.metadata,
            }, f)

    @staticmethod
    def load_from_file(table_name):
        filename = f"db/{table_name}.hfile.json"
        with open(filename, 'r') as f:
            data = json.load(f)
            hfile = HFile(table_name, data['metadata']['column_families'])
            hfile.enabled = data['enabled']  # Cargar el valor de 'enabled' del archivo
            # Convertir todas las entradas a diccionarios
            for cf, entries in data['data'].items():
                hfile.data[cf] = [dict(entry) for entry in entries]
            hfile.metadata = data['metadata']
            return hfile
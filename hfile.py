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
    
    def get(self, row_key):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        print()
        print("{:<20} {:<20}".format("COLUMN", "CELL"))
        print()
        for column_family, entries in self.data.items():
            for entry in entries:
                if entry['row_key'] == row_key:
                    print("{:<20} {:<20}".format(column_family + ':' + entry['column'], 'timestamp=' + str(entry['timestamp'])+', ' + 'value='+ entry['value']))
                    
        print()
        return None
    
    def scan(self, limit=None, offset=0):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        print()
        print("{:<20} {:<20}".format("COLUMN", "CELL"))
        print()
        count = 0
        for column_family, entries in self.data.items():
            for entry in entries:
                if count >= offset:
                    if limit is not None and count >= (offset + limit):
                        return None
                    print("{:<20} {:<20}".format(column_family + ':' + entry['column'], 'timestamp=' + str(entry['timestamp'])+', ' + 'value='+ entry['value']))
                count += 1
        return None

    def delete_cell(self, row_key, column_family, column):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        if column_family in self.data:
            original_length = len(self.data[column_family])
            self.data[column_family] = [entry for entry in self.data[column_family] if not (entry['row_key'] == row_key and entry['column'] == column)]
            affected_rows = original_length - len(self.data[column_family])
            self.num_entries -= affected_rows
            if affected_rows > 0:
                print(f"Cell '{column}' in row '{row_key}' and column family '{column_family}' deleted.")
                print(f"\n{affected_rows} row(s)")
            else:
                print(f"Error: Column '{column}' does not exist in row '{row_key}'.")
        else:
            print(f"Error: Column Family '{column_family}' does not exist.")

    def delete_all(self, row_key):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        affected_rows = 0
        for column_family in self.data:
            original_length = len(self.data[column_family])
            self.data[column_family] = [entry for entry in self.data[column_family] if entry['row_key'] != row_key]
            affected_rows += original_length - len(self.data[column_family])
        self.num_entries -= affected_rows
        print(f"\n{affected_rows} row(s)")

    def delete_column_family_rows(self, column_family, row_key):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        if column_family in self.data:
            original_length = len(self.data[column_family])
            self.data[column_family] = [entry for entry in self.data[column_family] if entry['row_key'] != row_key]
            affected_rows = original_length - len(self.data[column_family])
            self.num_entries -= affected_rows
            if affected_rows > 0:
                print(f"All cells in row '{row_key}' and column family '{column_family}' deleted.")
                print(f"\n{affected_rows} row(s)")
            else:
                print(f"Error: Row '{row_key}' does not exist in column family '{column_family}'.")
        else:
            print(f"Error: Column Family '{column_family}' does not exist.")
            
    def count(self):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        return self.metadata['num_entries']

    def delete_column(self, column_family, column):
        if not self.enabled:
            print(f"\033[31mError: The table '{self.table_name}' is disabled.\033[0m")
            return
        for row_key in self.data[column_family]:
            if column in self.data[column_family][row_key]:
                del self.data[column_family][row_key][column]
        print(f"All cells in column '{column}' and column family '{column_family}' deleted.")
    
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
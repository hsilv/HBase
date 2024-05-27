import bisect
from datetime import time
import json

class HFile:
    def __init__(self, table_name, column_families):
        self.table_name = table_name
        self.column_families = column_families
        self.data = {cf: [] for cf in column_families}
        self.metadata = {
            'num_entries': 0,
            'file_size': 0,
        }

    def put(self, row_key, column_family, column, value):
        timestamp = int(time.time() * 1000)
        entry = (row_key, column_family, column, timestamp, value)
        bisect.insort(self.data[column_family], entry)
        self.metadata['num_entries'] += 1
        self.metadata['file_size'] += len(str(entry))

    def save_to_file(self):
        filename = f"db/{self.table_name}.hfile.json"
        with open(filename, 'w') as f:
            json.dump({
                'data': self.data,
                'metadata': self.metadata,
            }, f)

    @staticmethod
    def load_from_file(filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            hfile = HFile(data['table_name'], data['column_families'])
            hfile.data = data['data']
            hfile.metadata = data['metadata']
            return hfile
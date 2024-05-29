import cmd
import getpass
from datetime import datetime
import time
import shlex

from hfile import HFile
from table import addColumnFamily, create_table, drop_table, get, list_tables, put, removeColumnFamily, drop_all_tables
class MyShell(cmd.Cmd):
    intro = '\nHBase Shell\nEscribe "help" para ver la lista de comandos\nEscribe "exit" para salir de la shell\nv1.0\n'
    prompt = f'\033[1;32m{getpass.getuser()}\033[0m\033[1;37m:\033[0m\033[1;33m{datetime.now().strftime("%H:%M:%S")}\033[0m \033[1;37m> \033[0m'


    def do_alter(self, arg):
        'Modificar una tabla en la base de datos: alter [nombre_tabla] [familia_columnas1] [familia_columnas2] ...'
        start = time.time()
        args = arg.split()
        
        if len(args) < 4:
            print("Error: Debes proporcionar al menos un nombre de tabla y una familia de columnas.")
            return
        table_name = args[0]
        column_families = args[3:]
        print(column_families)
        if args[1] == "add" and args[2] == "column":
            for column_family in column_families:
                addColumnFamily(table_name, column_family)
        elif args[1] == "remove" and args[2] == "column":
            for column_family in column_families:
                removeColumnFamily(table_name, column_family)
        else:
            print("Error: Comando no reconocido.")
            return
        end = time.time()
        print(f"Took {end - start:.4f} seconds")
        
    def do_drop(self, arg):
        'Eliminar una tabla en la base de datos: drop [nombre_tabla]'
        start = time.time()
        args = arg.split()
        if len(args) < 1:
            print("\033[31mError: Debes proporcionar el nombre de la tabla.\033[0m")
            return
        table_name = args[0]
        try:
            HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"\033[31mError: The table '{table_name}' does not exists.\033[0m")
            return
        drop_table(table_name)
        print(f"Table '{table_name}' dropped.")
        end = time.time()
        print(f"Took {end - start:.4f} seconds")
        
    def do_drop_all(self, arg):
        'Eliminar todas las tablas en la base de datos: drop_all'
        start = time.time()
        drop_all_tables()
        end = time.time()
        print(f"Took {end - start:.4f} seconds")


    def do_exit(self, arg):
        'Cerrar shell'
        print('Cerrando shell...')
        return True

    def do_get(self, arg):
        'Obtener una entrada de una tabla: get [nombre_tabla] [row_key] [familia_columna] [columna]'
        args = shlex.split(arg)
        if len(args) < 1:
            print("Error: Debes proporcionar el nombre de la tabla, row key, familia de columnas y columna.")
            return
        table_name, row_key = args
        get(table_name, row_key)
        
    def do_scan(self, arg):
        'Obtener todas las entradas de una tabla: scan [nombre_tabla]'
        args = shlex.split(arg)
        if len(args) != 1:
            print("Error: Debes proporcionar el nombre de la tabla.")
            return
        table_name = args[0]
        try:
            table = HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"Error: La tabla '{table_name}' no existe.")
            return
        table.scan()
    
    def do_list(self, arg):
        'Listar tablas disponibles en la base de datos'
        list_tables()
        
    def do_put(self, arg):
        'Agregar una nueva entrada a una tabla: put [nombre_tabla] [row_key] [familia_columna] [columna] [valor]'
        args = shlex.split(arg)
        if len(args) != 5:
            print("Error: Debes proporcionar el nombre de la tabla, row key, familia de columnas, columna y valor.")
            return
        table_name, row_key, column_family, column, value = args
        put(table_name, row_key, column_family, column, value)
    
    def do_enable(self, arg):
        'Habilitar una tabla en la base de datos: enable [nombre_tabla]'
        start = time.time()
        args = arg.split()
        if len(args) != 1:
            print("\033[31mError: Debes proporcionar el nombre de la tabla.\033[0m")
            return
        table_name = args[0]
        try:
            table = HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"\033[31mError: The table '{table_name}' does not exists.\033[0m")
            return
        table.enable()
        table.save_to_file()
        print(f"Table '{table_name}' enabled.")
        end = time.time()
        print(f"Took {end - start:.4f} seconds")
        
    def do_disable(self, arg):
        'Deshabilitar una tabla en la base de datos: disable [nombre_tabla]'
        start = time.time()
        args = arg.split()
        if len(args) != 1:
            print("\033[31mError: Debes proporcionar el nombre de la tabla.\033[0m")
            return
        table_name = args[0]
        try:
            table = HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"\033[31mError: The table '{table_name}' does not exists.\033[0m")
            return
        table.disable()
        table.save_to_file()
        print(f"Table '{table_name}' disabled.")
        end = time.time()
        print(f"Took {end - start:.4f} seconds")
            
    def do_describe(self, arg):
        'Describir una tabla en la base de datos: describe [nombre_tabla]'
        start = time.time()
        args = arg.split()
        if len(args) != 1:
            print("\033[31mError: Debes proporcionar el nombre de la tabla.\033[0m")
            return
        table_name = args[0]
        try:
            table = HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"\033[31mError: The table '{table_name}' does not exists.\033[0m")
            return
        
        print(f"\nTable {table_name} is {'ENABLED' if table.enabled else 'DISABLED'}")
        print(table_name)
        print("COLUMN FAMILIES DESCRIPTION")
        for column_family in table.column_families:
            print("{", f"NAME => '{column_family}'", "BLOOMFILETER => 'ROW', IN_MEMORY => 'false', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', COMPRESSION => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => 'inf', REPLICATION_SCOPE => '0'", "}")
            
        print(f"\n{len(table.column_families)} row(s)")
        print("Quota is disabled")
        end = time.time()
        print(f"Took {end - start:.4f} seconds")
    
        
    def do_create(self, arg):
        'Crear una tabla en la base de datos: create [nombre_tabla] [familia_columnas1] [familia_columnas2] ...'
        args = arg.split()
        if len(args) < 2:
            print("Error: Debes proporcionar al menos un nombre de tabla y una familia de columnas.")
            return
        table_name = args[0]
        column_families = args[1:]
        create_table(table_name, column_families)
        
        
    def do_Is_enabled(self, arg):
        'Verificar si una tabla está habilitada: is_enabled [nombre_tabla]'
        start = time.time()
        args = arg.split()
        if len(args) != 1:
            print("\033[31mError: Debes proporcionar el nombre de la tabla.\033[0m")
            return
        table_name = args[0]
        try:
            table = HFile.load_from_file(table_name)
        except FileNotFoundError:
            print(f"\033[31mError: The table '{table_name}' does not exists.\033[0m")
            return
        print(table.is_enabled())
        end = time.time()
        print(f"Took {end - start:.4f} seconds")  
        
    def do_help(self, arg):
        'Muestra la documentación: help [comando] | ? [comando] | help'
        if arg:
            # Si se proporcionó un argumento, intenta mostrar la ayuda para ese comando
            try:
                doc = getattr(self, 'do_' + arg).__doc__
                if doc:
                    print('\n--------------------------')
                    print(f'Comando: {arg}')
                    print('Descripción: ', end='')
                    print(doc)
                    print('--------------------------')
                else:
                    print(f'No se encontró documentación para el comando: {arg}')
            except AttributeError:
                print(f'No existe el comando: {arg}')
        else:
            # Si no se proporcionó un argumento, muestra la ayuda para todos los comandos
            cmds = [name[3:] for name in dir(self.__class__) if name.startswith('do_')]
            for cmd in cmds:
                print('\n--------------------------')
                print(f'Comando: {cmd}')
                print('Descripción: ', end='')
                getattr(self, 'do_' + cmd).__doc__ and print(getattr(self, 'do_' + cmd).__doc__)
                print('--------------------------')

    def precmd(self, line):
        user = getpass.getuser()
        current_time = datetime.now().strftime('%H:%M:%S')
        self.prompt = f'\n\033[1;32m{user}\033[0m\033[1;37m:\033[0m\033[1;33m{current_time}\033[0m \033[1;37m> \033[0m'
        return line

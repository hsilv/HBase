import cmd
import getpass
from datetime import datetime

from table import create_table, list_tables, put
class MyShell(cmd.Cmd):
    intro = '\nHBase Shell\nEscribe "help" para ver la lista de comandos\nEscribe "exit" para salir de la shell\nv1.0\n'
    prompt = f'\033[1;32m{getpass.getuser()}\033[0m\033[1;37m:\033[0m\033[1;33m{datetime.now().strftime("%H:%M:%S")}\033[0m \033[1;37m> \033[0m'

    def do_exit(self, arg):
        'Cerrar shell'
        print('Cerrando shell...')
        return True
    
    
    def do_list(self, arg):
        'Listar tablas disponibles en la base de datos'
        list_tables()
        
    def do_put(self, arg):
        'Agregar una nueva entrada a una tabla: put [nombre_tabla] [row_key] [familia_columna] [columna] [valor]'
        args = arg.split()
        if len(args) != 5:
            print("Error: Debes proporcionar el nombre de la tabla, row key, familia de columnas, columna y valor.")
            return
        table_name, row_key, column_family, column, value = args
        put(table_name, row_key, column_family, column, value)
        
    def do_create(self, arg):
        'Crear una tabla en la base de datos: create [nombre_tabla] [familia_columnas1] [familia_columnas2] ...'
        args = arg.split()
        if len(args) < 2:
            print("Error: Debes proporcionar al menos un nombre de tabla y una familia de columnas.")
            return
        table_name = args[0]
        column_families = args[1:]
        create_table(table_name, column_families)
        
        
        
        
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

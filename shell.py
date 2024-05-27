import cmd
import getpass
from datetime import datetime
class MyShell(cmd.Cmd):
    intro = '\nHBase Shell\nEscribe "help" para ver la lista de comandos\nEscribe "exit" para salir de la shell\nv1.0\n'
    prompt = f'\033[1;32m{getpass.getuser()}\033[0m\033[1;37m:\033[0m\033[1;33m{datetime.now().strftime("%H:%M:%S")}\033[0m \033[1;37m> \033[0m'

    def do_exit(self, arg):
        'Cerrar shell'
        print('Cerrando shell...')
        return True
        
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

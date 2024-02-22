def print_info(msg: str):
    print('\x1b[0;37m[INFO]', msg, '\x1b[0m')


def print_info_2(msg: str, start: str = '', end: str = '\n'):
    print(f'\x1b[0;34m{start}[INFO]', msg, '\x1b[0m', end=end)


def print_warning(msg: str):
    print('\x1b[0;33m[WARNING]', msg, '\x1b[0m')


def print_error(msg: str):
    print('\x1b[0;31m[ERROR]', msg, '\x1b[0m')
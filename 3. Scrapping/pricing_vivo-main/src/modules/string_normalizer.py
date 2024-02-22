from re import sub
from unidecode import unidecode


def normalize_string(string: str, sep: str, show_debug_msg=True) -> str | None:
    '''
    Normalize string removing special characters and replace spaces with desired character
    :param string: String to be normalized
    :param sep: Character used to replace spaces
    :return: Normalized string or None if something goes wrong
    '''
    if show_debug_msg:
        print('[DEBUG]', f'Normalizing string "{string}"...')

    try:
        string = sub(
            pattern='[^A-Za-z0-9 ]+',
            repl='',
            string=unidecode(string)
        ).replace(' ', sep).lower()
    except Exception as err:
        print('[ERROR]', 'Unable to normalize string due to', str(err))
        return None

    return string

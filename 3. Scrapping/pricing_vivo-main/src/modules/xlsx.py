from zipfile import ZipFile
from re import findall, DOTALL


def get_names(path: str) -> list[str] | None:
    print('[INFO]', f'Retrieving products names from xlsx "{path}"...')

    try:
        zf_xlsx = ZipFile(
            file=path
        )
    except Exception as err:
        print('[ERROR]', f'Unable to open "{path}" due to', str(err))
        return None

    try:
        io_xml_index = zf_xlsx.open('xl/worksheets/sheet1.xml')
    except Exception as err:
        print('[ERROR]', 'Unable to open "sheet1.xml" due to', str(err))
        return None

    try:
        io_xml_strings = zf_xlsx.open('xl/sharedStrings.xml')
    except Exception as err:
        print('[ERROR]', 'Unable to open "sharedStrings.xml" due to', str(err))
        return None

    try:
        l_results_index = findall(
            pattern='<c r="A.+?" t="s"><v>(.+?)</v></c>',
            string=io_xml_index.read().decode('utf-8'),
            flags=DOTALL
        )
        io_xml_index.close()
    except Exception as err:
        print('[ERROR]', 'Unable to find indices with RegEx due to', str(err))
        return None

    if len(l_results_index) == 0:
        print('[ERROR]', 'RegEx did not find the indexes.')
        return None

    l_results_index = l_results_index[1:]

    try:
        l_results_strings = findall(
            pattern='<t>(.+?)</t>',
            string=io_xml_strings.read().decode('utf-8'),
            flags=DOTALL
        )
    except Exception as err:
        print('[ERROR]', 'Unable to find strings with RegEx due to', str(err))
        return None

    if len(l_results_strings) == 0:
        print('[ERROR]', 'RegEx did not find the products names.')
        return None

    l_return_list = []

    for i_index in l_results_index:
        l_return_list.append(l_results_strings[int(i_index)])

    return l_return_list

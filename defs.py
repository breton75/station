from datetime import datetime, date, time
import codecs, sys


def fields_str(fields_list, table_name):
    result = ""
    for fld in fields_list: result += "{:s}.{:s},".format(table_name, fld)

    return result[:-1]


def fields_upd_str(fields_list, table1_name, table2_name):
    result = ""
    for fld in fields_list: result += "{0}.{2} = {1}.{2},".format(table1_name, table2_name, fld)

    return result[:-1]


def value_str(value):
    if type(value) == type(None): return "NULL,"
    elif type(value) == str: return "'" + str(value) + "',"
    elif type(value) == datetime: return "'" + value.strftime('%Y/%m/%d %H:%M:%S')  + "',"
    elif type(value) == date: return "'" + value.strftime('%Y/%m/%d')  + "',"
    elif type(value == int): return str(value) + ","
    elif type(value) == float: return str(value) + ","
    else: # если встретится неизвестный тип данных, то эту колонку пропускаем
        return str(value)
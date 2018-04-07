from datetime import datetime, date, time
import codecs, sys


def fields_str(fields_list):
    result = ""
    for col in fields_list: result += col + ","

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
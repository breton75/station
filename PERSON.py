import defs


PERSON_FIELD_NAMES = ["PERSON.ID",
                     "PERSON.RESERVID",
                     "PERSON.TITLE",
                     "PERSON.BIRTHDATE",
                     "PERSON.NATIONID",
                     "PERSON.COUNTRY",
                     "PERSON.COUNTRY2",
                     "PERSON.CITY",
                     "PERSON.CITY2",
                     "PERSON.VIP",
                     "PERSON.LanguageID",
                     "LANGUAGE.NAME as LanguageName,"
                     "PERSON.LANGUAGEID2",
                     "PERSON.SEX",
                     "PERSON.REGULAR2",
                     "PERSON.ISBANK",
                     "PERSON.ISPAYMENTDEPARTMENT",
                     "PERSON.ISDEBITORPAYMENTDEPT",
                     "PERSON.SALDO",
                     "PERSON.BIRTHPLACE",
                     "PERSON.PROFESSION",
                     "PERSON.BIRTHDATE2",
                     "PERSON.ISSMOKER",
                     "PERSON.HASCRUISE"]


def select_for_insert_query(last_update):
        return "select " + defs.fields_str(PERSON_FIELD_NAMES) + " from Admin." + 'PERSON' + \
        " LEFT JOIN LANGUAGE on Admin.PERSON.LanguageID = Admin.LANGUAGE.ID "
        " where Admin.PERSON.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(PERSON_FIELD_NAMES) + " from Admin." + 'PERSON' + \
        " LEFT JOIN LANGUAGE on Admin.PERSON.LanguageID = Admin.LANGUAGE.ID "
        " where Admin.PERSON.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"
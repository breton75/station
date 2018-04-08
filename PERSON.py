import defs


PERSON_FIELD_NAMES = ["ID",
                      "RESERVID",
                      "TITLE",
                      "BIRTHDATE",
                      "NATIONID",
                      "COUNTRY",
                      "COUNTRY2",
                      "CITY",
                      "CITY2",
                      "VIP",
                      "LanguageID",
                      "LANGUAGEID2",
                      "SEX",
                      "REGULAR2",
                      "ISBANK",
                      "ISPAYMENTDEPARTMENT",
                      "ISDEBITORPAYMENTDEPT",
                      "SALDO",
                      "BIRTHPLACE",
                      "PROFESSION",
                      "BIRTHDATE2",
                      "ISSMOKER",
                      "HASCRUISE"]


def select_for_insert_query(last_update):
        return "select " + defs.fields_str(PERSON_FIELD_NAMES, 'PERSON') + " from Admin." + 'PERSON' + \
        " where Admin.PERSON.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(PERSON_FIELD_NAMES, 'PERSON') + " from Admin." + 'PERSON' + \
        " where Admin.PERSON.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"
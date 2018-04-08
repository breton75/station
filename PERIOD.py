import defs



PERIOD_FIELD_NAMES = ["ID",
                    "RESERVID",
                    "ROOMID",
                    "BEGINDATE",
                    "ENDDATE",
                    "TYPEID",
                    "LOADROW",
                    "SUPERPERIODID",
                    "ShowEndDate"]


def select_for_insert_query(last_update):
        return "select " + defs.fields_str(PERIOD_FIELD_NAMES, 'PERIOD') + " from Admin." + 'PERIOD' + \
        " where Admin.PERIOD.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(PERIOD_FIELD_NAMES, 'PERIOD') + " from Admin." + 'PERIOD' + \
        " where Admin.PERIOD.RESERVID in (select Admin.RESERVE.ID from Admin.RESERVE" + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "')"
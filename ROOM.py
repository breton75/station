import defs



FIELD_NAMES = ["ID",
               "\"NUMBER\"",
               "FLOOR",
               "FANTOM",
               "PRICE",
               "TYPEID",
               "ROOMSTATE",
               "StateChangeDate",
               "LOCKED",
               "SPECORDER",
               "SwitchON",
               "AutoCode",
               "UPDATESTATUSTIME",
               "BEDCOUNT",
               "SUPPLEMENTARY",
               "LIFEBOAT",
               "\"SECTION\"",
               "ISSUPER",
               "ISSUB",
               "IsExtraBed",
               "HasElectronicLock",
               "JURIDICALPERS",
               "TypeCleaning",
               "BedLinen",
               "WinWash",
               "BedLinenDate",
               "DryCleaningDate",
               "ASC_ROOMID"]


def select_for_insert_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, "ROOM") + " from Admin.ROOM"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, "ROOM") + " from Admin.ROOM"
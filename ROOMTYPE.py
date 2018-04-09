import defs


FIELD_NAMES = ["ID",
                      "NAME",
                      "SELECTED",
                      "SELECTEDINGRAPH",
                      "SELECTEDINSTATISTICS",
                      "SUPPLEMENTARY",
                      "MAINTYPEID",
                      "SuperRoomTypeID",
                      "IsExtraBed",
                      "CheckInTimeForRoomType",
                      "CheckOutTimeForRoomType",
                      "UseDefaultTime",
                      "isCRS",
                      "SetOfBedLinen",
                      "PeriodOfBedLinen",
                      "PeriodOfDryCleaning",
                      "AdultDef"]


def select_for_insert_query(last_update):
    return "select " + defs.fields_str(FIELD_NAMES, 'ROOMTYPE') + " from Admin.ROOMTYPE"


def select_for_update_query(last_update):
    return "select " + defs.fields_str(FIELD_NAMES, 'ROOMTYPE') + " from Admin.ROOMTYPE"
import defs

# RESERVE = 'RESERVE'

FIELD_NAMES = ["ApplicableDiscountCard", #:"bigint",
                       "Arrival", #:"date",
                       "ARRIVALTIME", #:"datetime",
                       "AUTOALLOC", #:"float",
                       "BOARDID", #:"bigint",
                       "BookingPaymentId", #:"varchar(max)",
                       "BookingResNum", #:"bigint",
                       "BookingRoomResNum", #:"bigint",
                       "BuchDate", #:"date",
                       "COMMSUM", #:"float",
                       "CREATEDLOGIN", #:"varchar(max)",
                       "CREATEDTIME", #:"datetime",
                       "CREATIONTIME", #:"datetime",
                       "CREATIONUSER", #:"varchar(max)",
                       "CURRENCYID", #:"bigint",
                       "DELETEDTIME", #:"datetime",
                       "DELETEDUSER", #:"varchar(max)",
                       "Departure", #:"date",
                       "DepartureTime", #:"datetime",
                       "DEPOSITDATE", #:"date",
                       "HIDE", #:"bigint",
                       "ID", #:"bigint",
                       "isCRS", #:"bigint",
                       "ISDELETED", #:"bigint",
                       "ISDELETEDTYPE", #:"bigint",
                       "PacketID", #:"bigint",
                       "PACKETNAME", #:"varchar(max)",
                       "PERSON_NAME", #:"varchar(max)",
                       "RATECODEID", #:"bigint",
                       "RECEIVEDDATE", #:"date",
                       "RESNUM", #:"bigint",
                       "respuserID", #:"float",
                       "ROOMPRICE", #:"float",
                       "SHORTRESNOTE2", #:"varchar(max)",
                       "ShowDepartureDate", #:"date",
                       "UPDATEDTIME"] #:"datetime"}
                        

def select_for_insert_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, 'RESERVE') + " from Admin." + 'RESERVE' + \
        " where CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, 'RESERVE') + " from Admin." + 'RESERVE' + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'"
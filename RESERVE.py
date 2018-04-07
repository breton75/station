import defs

# RESERVE = 'RESERVE'

RESERVE_FIELD_NAMES = ["RESERVE.ApplicableDiscountCard", #:"bigint",
                        "RESERVE.Arrival", #:"date",
                        "RESERVE.ARRIVALTIME", #:"datetime",
                        "RESERVE.AUTOALLOC", #:"float",
                        "RESERVE.BOARDID", #:"bigint",
                        "RESERVE.BookingPaymentId", #:"varchar(max)",
                        "RESERVE.BookingResNum", #:"bigint",
                        "RESERVE.BookingRoomResNum", #:"bigint",
                        "RESERVE.BuchDate", #:"date",
                        "RESERVE.COMMSUM", #:"float",
                        "RESERVE.CREATEDLOGIN", #:"varchar(max)",
                        "RESERVE.CREATEDTIME", #:"datetime",
                        "RESERVE.CREATIONTIME", #:"datetime",
                        "RESERVE.CREATIONUSER", #:"varchar(max)",
                        "RESERVE.CURRENCYID", #:"bigint",
                        "RESERVE.DELETEDTIME", #:"datetime",
                        "RESERVE.DELETEDUSER", #:"varchar(max)",
                        "RESERVE.Departure", #:"date",
                        "RESERVE.DepartureTime", #:"datetime",
                        "RESERVE.DEPOSITDATE", #:"date",
                        "RESERVE.HIDE", #:"bigint",
                        "RESERVE.ID", #:"bigint",
                        "RESERVE.isCRS", #:"bigint",
                        "RESERVE.ISDELETED", #:"bigint",
                        "RESERVE.ISDELETEDTYPE", #:"bigint",
                        "RESERVE.PacketID", #:"bigint",
                        "RESERVE.PACKETNAME", #:"varchar(max)",
                        "RESERVE.PERSON_NAME", #:"varchar(max)",
                        "RESERVE.RATECODEID", #:"bigint",
                        "RESERVE.RECEIVEDDATE", #:"date",
                        "RESERVE.RESNUM", #:"bigint",
                        "RESERVE.respuserID", #:"float",
                        "RESERVE.ROOMPRICE", #:"float",
                        "RESERVE.SHORTRESNOTE2", #:"varchar(max)",
                        "RESERVE.ShowDepartureDate", #:"date",
                        "RESERVE.UPDATEDTIME"] #:"datetime"}
                        

def select_for_insert_query(last_update):
        return "select " + defs.fields_str(RESERVE_FIELD_NAMES) + " from Admin." + 'RESERVE' + \
        " where CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'"


def select_for_update_query(last_update):
        return "select " + defs.fields_str(RESERVE_FIELD_NAMES) + " from Admin." + 'RESERVE' + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'"
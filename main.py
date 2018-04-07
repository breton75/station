import pypyodbc
import datetime

# from oauth2client.service_account import ServiceAccountCredentials
# import gspread
import logging
import sys
import numbers
# import xlwt
# import copy

logger = logging.getLogger('dashboard_4_0')
hdlr = logging.FileHandler('dashboard_4_0.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)

logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

# scope = ['https://spreadsheets.google.com/feeds']
# credentials = ServiceAccountCredentials.from_json_keyfile_name('access.json', scope)
# gc = gspread.authorize(credentials)
# sh = gc.open_by_key('10qDyRLqNOnlRTL3-lrHzofnBWd_9D232ViaYJa996yI')

# if not sh:
#     print
#     "Cannot open destinatino Google Spreadsheet"
#     exit(-1)

import defs



def connectMSSQL() : #data, table, schema=None
    server = 'shotels.database.windows.net'
    database = 'test'
    username = 'StationHotels'
    password = 'Station12345'
    driver= '{ODBC Driver 13 for SQL Server}'
    driver= 'ODBC+Driver+13+for+SQL+Server'

    addr = "mssql+pyodbc://"+username+ ":"+password+"@"+server+"/"+database+"?driver="+driver+"&convert_unicode=True"
    print (addr)
    cnxn = pypyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    if not cnxn:
        return 0

    return cnxn.cursor()


    # engine = sqlalchemy.create_engine(addr)
    # #cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
    # #data.to_sql(name=table, con=cnxn, if_exists = 'append', index=False)
    # data.to_sql(name=table, con=engine, schema=schema, if_exists = 'replace', index=False, dtype = sqlcol(data))
#

def connectEdelweis():
    edel = pypyodbc.connect('DSN=Edel')

    if not edel:
        print ("Cannot connect to Edelweiss Database")
        return 0

    return edel.cursor()




mssql = connectMSSQL()
if not mssql:
    print("Cannot connect to MSSQL Database")
    exit(-1)

print("Connected to MSSQL Database")

edel = connectEdelweis()
if not edel:
    print("Cannot connect to Edelweiss Database")
    mssql.close()
    exit(-1)
print ("Connected to Edelweiss Database")


# for s in TABLES:
#     q = "insert into sv.tbl_upd (tbl_name) values ('{:s}');".format(s)
#     print(q)
#     mssql.cursor().execute("insert into sv.tbl_upd (tbl_name) values (?); commit;", (s, ))

mssql.execute("select tbl_name, last_update from sv.tbl_upd")

for row in mssql.fetchall():
    if not row:
        print("Error in MSSQL Database")
        continue

    tbl_name = row['tbl_name']
    last_update = row['last_update']

    if last_update == None:
        print("full update for {:s}".format(tbl_name))
        mssql.execute("EXEC	dbo.upload_PERSON 777; EXEC	dbo.upload_PERSON 1101; EXEC	dbo.upload_PERSON 1202")
        #
        # if tbl_name == defs.RESERVE_TABLE:
        #
        #     mssql.execute("delete from table sv.{:s}".format(tbl_name))
        #
        #     edel.execute("select count(id) as cnt from Admin.{:s};".format(tbl_name)) #, defs.TABLE_FIELDS[tbl_name])
        #     print(edel.fetchone()["cnt"])
        #     mssql.execute("update sv.tbl_upd set last_update = SYSDATETIME() where tbl_name = '{:s}'".format(tbl_name))
        mssql.commit()

    else:
        print("partial update for {:s}".format(tbl_name))

# mssql.cursor().close()
mssql.close()

edel.close()


# edel.execute()



print("OK")




# ws = sh.worksheet("#ReservationsByDay")
#
# if not ws:
#     print
#     "Cannot find worksheet"
#     logger.debug('Cannot find worksheet')
#     exit(-1)

#
# def BeginImport():
#     logger.debug('====== Import started ======')
#     print
#     "Start..."
#     return True
#
#
# def EndImport():
#     logger.debug('====== Import finished ======')
#     return
#
#
# def DecodeString1251(s):
#     # print s, type(s)
#
#     if s is None:
#         return ""
#
#     if isinstance(s, numbers.Integral) or isinstance(s, numbers.Number):
#         return s
#
#         # data.append(str(row[9]).replace(".", ","))   # сумма
#
#     if isinstance(s, datetime.date):
#         return s  # .strftime("%d.%m.%Y")
#
#     if isinstance(s, datetime.time):
#         return s  # .strftime("%d.%m.%Y")
#
#     return s.decode('cp1251', 'ignore')
#
#
# def BatchUpdateCells(ws, cells):
#     while True:
#         try:
#             for i in xrange(0, len(cells), 750):
#                 ws.update_cells(cells[i:i + 750])
#             return
#
#         except:
#             print
#             "Unexpected upate cell error:", sys.exc_info(), " retrying ..."
#             logger.debug('Unexpected error: ' + str(sys.exc_info()))
#
#
# Hotels = [
#     "DUM",
#     "G73",
#     "K43",
#     "Z12",
#     "M19",
#     "A1",
#     "S10"
# ]
#
# HotelsMainType = {
#     1: "Z12",
#     2: "K43",
#     3: "G73",
#     4: "M19",
#     6: "A1",
#     7: "DUM",
#     8: "S10",
#     9: "S13",
#     10: "L1"
# }
#
#
# def GetHotelMainType(hotel):
#     for h in HotelsMainType:
#         if HotelsMainType[h] == hotel:
#             return h
#
#     return -1
#
#
# def PushDataToGSheet(ws, data, num_cols, empty_cells):
#     empty_space = len(empty_cells) / num_cols
#     numTransactions = len(data) / num_cols
#
#     if numTransactions > empty_space:
#         rowsNeeded = numTransactions - empty_space
#         lastRow = ws.row_count
#         ws.add_rows(rowsNeeded)
#         empty_cells.extend(ws.range(lastRow + 1, 1, lastRow + rowsNeeded, num_cols))
#
#     # print "PushDataToGSheet"
#
#     toUpdate = []
#     i = 0
#     for d in data:
#         empty_cells[i].value = d
#         toUpdate.append(empty_cells[i])
#         i += 1
#
#     # print "Publish"
#
#     # Update in batch
#     BatchUpdateCells(ws, toUpdate)
#
#     return empty_cells[i:]
#
#
# RAW_DATA_NUM_COLS = 7
#
# '''
#     request = SELECT SUM(RESERVE.roomprice), COUNT(RESERVE.roomprice), PERSONSEGMENT.NAME, ROOMTYPE.maintypeid, RESERVE.resnum \
# FROM {oj ((((((((("RESERVE" "RESERVE" LEFT OUTER JOIN "PERIOD" "PERIOD" ON "RESERVE"."ID"="PERIOD"."RESERVID") \
# LEFT  OUTER JOIN "PERSON" "PERSON" ON "RESERVE"."ID"="PERSON"."RESERVID") \
# INNER JOIN "PERMANENTAGENT" "PERMANENTAGENT" ON "RESERVE"."TRAVELAGENTID"="PERMANENTAGENT"."ID") \
# LEFT OUTER JOIN "GROUPNAME" "GROUPNAME" ON "RESERVE"."GROUPID"="GROUPNAME"."id") \
# LEFT OUTER JOIN "Vouchers" "Vouchers" ON "PERSON"."ID"="Vouchers"."PersonID") \
# LEFT OUTER JOIN PERSONSEGMENTRELATION ON PERSON.ID = PERSONSEGMENTRELATION.PERSONID) \
# LEFT OUTER JOIN PERSONSEGMENT ON PERSONSEGMENT.ID = PERSONSEGMENTRELATION.SEGMENTID) \
# INNER JOIN "PERMANENTCOMPANY" "PERMANENTCOMPANY" ON "PERSON"."PAYINGCOMPANYID"="PERMANENTCOMPANY"."ID") \
# INNER JOIN "ROOM" "ROOM" ON "PERIOD"."ROOMID"="ROOM"."ID") \
# INNER JOIN "ROOMTYPE" "ROOMTYPE" ON "ROOM"."TYPEID"="ROOMTYPE"."ID"}
# '''
#
#
# def ImportTransactions(cursor, ws, empty_cells, date):
#     d = date.strftime("%d.%m.%Y")
#     # print d1, d2
#
#     ###request = SELECT SUM(RESERVE.roomprice), COUNT(RESERVE.roomprice), MAX(PERSONSEGMENT.NAME), ROOMTYPE.maintypeid, RESERVE.resnum
#
#     request = '''select SUM(ROOM_PR), COUNT(ROOM_PR), SEG_NM, ROOM_MRT, CHANEL, RES FROM\
# ( \
# SELECT RESERVE.resnum AS RES, MAX(RESERVE.roomprice) AS ROOM_PR, MAX(PERSONSEGMENT.NAME) AS SEG_NM, ROOMTYPE.maintypeid AS ROOM_MRT, MAX(PERSON.SURNAME2) AS CHANEL \
# FROM {oj ((((((((("RESERVE" "RESERVE" LEFT OUTER JOIN "PERIOD" "PERIOD" ON "RESERVE"."ID"="PERIOD"."RESERVID") \
# LEFT  OUTER JOIN "PERSON" "PERSON" ON "RESERVE"."ID"="PERSON"."RESERVID") \
# INNER JOIN "PERMANENTAGENT" "PERMANENTAGENT" ON "RESERVE"."TRAVELAGENTID"="PERMANENTAGENT"."ID") \
# LEFT OUTER JOIN "GROUPNAME" "GROUPNAME" ON "RESERVE"."GROUPID"="GROUPNAME"."id") \
# LEFT OUTER JOIN "Vouchers" "Vouchers" ON "PERSON"."ID"="Vouchers"."PersonID") \
# LEFT OUTER JOIN PERSONSEGMENTRELATION ON PERSON.ID = PERSONSEGMENTRELATION.PERSONID) \
# LEFT OUTER JOIN PERSONSEGMENT ON PERSONSEGMENT.ID = PERSONSEGMENTRELATION.SEGMENTID) \
# INNER JOIN "PERMANENTCOMPANY" "PERMANENTCOMPANY" ON "PERSON"."PAYINGCOMPANYID"="PERMANENTCOMPANY"."ID") \
# INNER JOIN "ROOM" "ROOM" ON "PERIOD"."ROOMID"="ROOM"."ID") \
# INNER JOIN "ROOMTYPE" "ROOMTYPE" ON "ROOM"."TYPEID"="ROOMTYPE"."ID"} '''
#
#     request += 'WHERE "RESERVE"."arrival"<={d ' + date.strftime(
#         "%Y-%m-%d") + '} AND "RESERVE"."departure">{d ' + date.strftime("%Y-%m-%d") + '} '
#     # "PERSON"."saldochanged">0
#     # request += 'AND ROOMTYPE."maintypeid"=' + str(GetHotelMainType("A1")) + ' '
#     # request += 'AND ROOMTYPE."maintypeid"<>' + str(GetHotelMainType("DUM")) + ' '
#     request += 'AND RESERVE.stateid in(0,1,2,4) '
#     request += 'AND RESERVE.isdeleted=0 '
#     request += 'AND Reserve.Hide = 0 '
#     request += 'GROUP BY RESERVE.resnum, ROOMTYPE.maintypeid '
#     request += ') AS SUB '
#     request += 'GROUP by SEG_NM, ROOM_MRT, CHANEL, RES '
#
#     # paypercent saldochanged
#     # print request
#
#     print
#     "Processing: " + d
#     # logger.debug('Processing ' + period)
#
#     curs.execute(request)
#     res = curs.fetchall()
#
#     data = []
#
#     for row in res:
#         for c in row:
#             value = DecodeString1251(c)
#             # print value,
#
#         # print HotelsMainType[row[3]],
#         # print ""
#
#         data.append(d)
#         data.append(str(DecodeString1251(row[0])).replace(".", ","))  #
#         data.append(DecodeString1251(row[1]))
#         data.append(DecodeString1251(row[2]))
#         data.append(HotelsMainType[row[3]])
#         data.append(DecodeString1251(row[4]))
#         data.append(DecodeString1251(row[5]))
#
#         # print len(data)
#
#     return PushDataToGSheet(ws, data, RAW_DATA_NUM_COLS, empty_cells)
#
#
# def Process(cursor):
#     nowDate = datetime.datetime.today().date()
#     endDate = datetime.datetime(nowDate.year, nowDate.month, nowDate.day, 0, 0, 0) - datetime.timedelta(days=1)
#     startDate = endDate - datetime.timedelta(days=2)
#
#     startDate = datetime.datetime(2017, 12, 31, 0, 0, 0)
#     endDate = datetime.datetime(2018, 6, 30, 0, 0, 0)
#
#     print
#     startDate
#     print
#     endDate
#
#     # clean up
#     dates = ws.col_values(1)
#     empty_cells = []
#     first_empty_row = last_empty_row = -1
#
#     for i in xrange(1, len(dates)):  # preserve first row
#         date = datetime.datetime(1900, 1, 1, 0, 0, 0)
#
#         try:
#             date = datetime.datetime.strptime(dates[i], '%d.%m.%Y')
#         except ValueError:
#             date = startDate
#
#         if date >= startDate and date <= endDate:
#             last_empty_row = i + 1
#             if first_empty_row < 0:
#                 first_empty_row = i + 1
#             continue
#
#         if first_empty_row > 0:
#             print
#             "Empty range", first_empty_row, " ", last_empty_row
#             empty_cells.extend(ws.range(first_empty_row, 1, last_empty_row, RAW_DATA_NUM_COLS))
#             first_empty_row = -1
#
#     if first_empty_row > 0:
#         print
#         "Empty range", first_empty_row, " ", last_empty_row
#         empty_cells.extend(ws.range(first_empty_row, 1, last_empty_row, RAW_DATA_NUM_COLS))
#         first_empty_row = -1
#
#     for c in empty_cells:
#         c.value = ''
#
#     BatchUpdateCells(ws, empty_cells)
#
#     date = startDate
#     while date <= endDate:
#         empty_cells = ImportTransactions(cursor, ws, empty_cells, date)
#         date += datetime.timedelta(days=1)
#
#     # mainsheet = sh.worksheet(u"#TimeStamp")
#     # if mainsheet :
#     #    mainsheet.update_acell('B1', datetime.datetime.now().strftime("%H:%M %d.%m.%Y"))
#     #    #mainsheet.update_acell('B2', startDate.strftime("%d.%m.%Y"))
#     #    mainsheet.update_acell('C2', endDate.strftime("%d.%m.%Y"))
#
#
# if BeginImport():
#
#     try:
#         curs = conn.cursor()
#         Process(curs);
#         EndImport()
#
#     except:
#         print
#         "Unexpected error:", sys.exc_info()
#         logger.debug('Unexpected error: ' + str(sys.exc_info()))
#         EndImport()
#         raise
#
# curs.close()
# conn.close()

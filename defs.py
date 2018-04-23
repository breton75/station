from datetime import datetime, date, time
import codecs, sys



F_ReservId =    u"КодБрони"
F_Segment =     u"Сегмент"
F_Source =      u"Источник"
F_Hotel =       u"Отель"
F_Room =        u"НомерКомнаты"
F_Fio =         u"ФИО"
F_Sex =         u"Пол"
F_Country =     u"Страна"
F_Email =       u"ЭлектроннаяПочта"
F_Birthdate =   u"ДатаРождения"
F_Sum =         u"Сумма"
F_CreatedTime = u"ВремяСоздания"
F_UpdatedTime = u"ВремяОбновления"
F_PeriodBegin = u"НачалоПериода"
F_PeriodEnd =   u"КонецПериода"
F_Date =        u"Дата"
F_Chanel =      u"Chanel"
F_IsDeleted =   u"Удалено"   


MS_ReservesByDays_FIELDS = {F_Date : "DATE",
                            F_Sum : "MONEY",
                            F_ReservId : "INTEGER"}


MS_Reserves_FIELDS =       {F_ReservId : "INTEGER",
                            F_Segment : "NVARCHAR(100)",
                            F_Source : "NVARCHAR(100)",
                            F_Hotel : "NVARCHAR(100)",
                            F_Room : "NVARCHAR(100)",
                            F_Fio : "NVARCHAR(100)",
                            F_Sex : "INTEGER",
                            F_Country : "NVARCHAR(100)",
                            F_Email : "NVARCHAR(100)",
                            F_Chanel : "NVARCHAR(100)",
                            F_IsDeleted : "INTEGER"}


def select_query():
  return  "WITH P as (" + "\n" + \
           "    SELECT " + "\n" + \
           "    Admin.PERSON.RESERVID as ReservId, " + "\n" + \
           "    MIN( Admin.PERSON.ID ) as PersonId " + "\n" + \
           "  FROM " + "\n" + \
           "    Admin.PERSON " + "\n" + \
           "  GROUP BY " + "\n" + \
           "    RESERVID " + "\n" + \
           "), " + "\n" + \
           "Segment AS( " + "\n" + \
           "  SELECT " + "\n" + \
           "    Admin.PERSONSEGMENTRELATION.SEGMENTID AS Id, " + "\n" + \
           "    Admin.PERSONSEGMENT.NAME as Name, " + "\n" + \
           "    Admin.PERSONSEGMENT.SHORTNAME as ShortName, " + "\n" + \
           "    Admin.PERSONSEGMENTRELATION.PERSONID as PersonId " + "\n" + \
           "  FROM " + "\n" + \
           "    Admin.PERSONSEGMENTRELATION " + "\n" + \
           "  LEFT JOIN Admin.PERSONSEGMENT on " + "\n" + \
           "    Admin.PERSONSEGMENT.ID = Admin.PERSONSEGMENTRELATION.SEGMENTID " + "\n" + \
           "), " + "\n" + \
           "Person AS(" + "\n" + \
           "  SELECT" + "\n" + \
           "    P.PersonId as Id," + "\n" + \
           "    P.ReservId as ReservId," + "\n" + \
           "    (" + "\n" + \
           "      Admin.PERSON.SURNAME + ' ' + Admin.PERSON.FIRSTNAME" + "\n" + \
           "    ) as Name," + "\n" + \
           "    Admin.PERSON.SURNAME2 AS Chanel," + "\n" + \
           "    Admin.PERSON.BIRTHDATE AS Birthdate," + "\n" + \
           "    Admin.PERSON.COUNTRY AS Country," + "\n" + \
           "    Admin.PERSON.SEX AS Sex," + "\n" + \
           "    Admin.PERSON.EMAIL AS Email," + "\n" + \
           "    Segment.Id AS SegmentId," + "\n" + \
           "    Segment.Name AS SegmentName," + "\n" + \
           "    Segment.ShortName AS SegmentShortName" + "\n" + \
           "  FROM" + "\n" + \
           "    P" + "\n" + \
           "  LEFT JOIN Admin.PERSON on" + "\n" + \
           "    P.PersonId = Admin.PERSON.ID" + "\n" + \
           "  LEFT JOIN Segment ON" + "\n" + \
           "    P.PersonId = Segment.PersonId" + "\n" + \
           ")," + "\n" + \
           "R AS(" + "\n" + \
           "  SELECT " + "\n" + \
           "    Admin.ROOMTYPE.ID AS TypeId," + "\n" + \
           "    Admin.MAINROOMTYPE.NAME AS HotelName" + "\n" + \
           "  FROM" + "\n" + \
           "    Admin.ROOMTYPE" + "\n" + \
           "  LEFT JOIN Admin.MAINROOMTYPE ON" + "\n" + \
           "    Admin.MAINROOMTYPE.ID = Admin.ROOMTYPE.MAINTYPEID" + "\n" + \
           ")," + "\n" + \
           "Hotel AS(" + "\n" + \
           "  SELECT" + "\n" + \
           "    R.HotelName AS Name," + "\n" + \
           "    Admin.ROOM.\"NUMBER\" AS Room," + "\n" + \
           "    Admin.PERIOD.RESERVID AS ReserveId" + "\n" + \
           "  FROM" + "\n" + \
           "    Admin.ROOM" + "\n" + \
           "  LEFT JOIN Admin.PERIOD ON" + "\n" + \
           "    Admin.PERIOD.ROOMID = Admin.ROOM.ID" + "\n" + \
           "  LEFT JOIN R ON" + "\n" + \
           "    Admin.ROOM.TYPEID = R.TypeId" + "\n" + \
           ") SELECT" + "\n" + \
           "  Admin.RESERVE.ID as " +           F_ReservId + "," + "\n" + \
           "  Admin.RESERVE.CREATEDTIME AS " +  F_CreatedTime + "," + "\n" + \
           "  Admin.RESERVE.UPDATEDTIME AS " +  F_UpdatedTime + "," + "\n" + \
           "  Admin.RESERVE.ROOMPRICE AS " +    F_Sum + "," + "\n" + \
           "  Admin.RESERVE.ISDELETED AS " +    F_IsDeleted + "," + "\n" + \
           "  Person.SegmentName AS " +         F_Segment + "," + "\n" + \
           "  Hotel.Name AS " +                 F_Hotel + "," + "\n" + \
           "  Hotel.Room AS " +                 F_Room + "," + "\n" + \
           "  Person.Name AS " +                F_Fio + "," + "\n" + \
           "  Person.Sex AS " +                 F_Sex + "," + "\n" + \
           "  Person.Country AS " +             F_Country + "," + "\n" + \
           "  Person.Email AS " +               F_Email + "," + "\n" + \
           "  Person.Chanel AS " +              F_Chanel + "," + "\n" + \
           "  Person.Birthdate AS " +           F_Birthdate + "," + "\n" + \
           "  Admin.PERIOD.BEGINDATE AS " +     F_PeriodBegin + "," + "\n" + \
           "  Admin.PERIOD.ENDDATE AS " +       F_PeriodEnd + "\n" + \
           "FROM" + "\n" + \
           "  Admin.RESERVE" + "\n" + \
           "LEFT JOIN Person ON" + "\n" + \
           "  Admin.RESERVE.ID = Person.ReservId" + "\n" + \
           "LEFT JOIN Hotel ON" + "\n" + \
           "  Admin.RESERVE.ID = Hotel.ReserveId" + "\n" + \
           "LEFT JOIN Admin.PERIOD ON" + "\n" + \
           "  Admin.PERIOD.RESERVID = Admin.RESERVE.ID" + "\n" + \
           "LEFT JOIN Admin.PERMANENTAGENT ON" + "\n" + \
           "  Admin.PERMANENTAGENT.ID = Admin.RESERVE.TRAVELAGENTID" + "\n"


def select_for_insert_query(last_update):
  
  return select_query() + \
          "WHERE" + "\n" + \
          "  Admin.RESERVE.CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "' AND " + \
          "  Admin.RESERVE.ISDELETED = 0 " + \
          "ORDER BY Admin.RESERVE.CREATEDTIME ASC"



def select_for_update_query(last_update):
  
  return select_query() + \
          "WHERE" + "\n" + \
          "  Admin.RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "' AND " + \
          "  Admin.RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "' AND " + \
          "  Admin.RESERVE.ISDELETED = 0 " + \
          "ORDER BY Admin.RESERVE.UPDATEDTIME ASC"





# ----------------------------------------------------- #
# ----------- ВСЯКИЕ СЛУЖЕБНЫЕ ФУНКЦИИ ---------------- #
# ----------------------------------------------------- #

def fields_str(fields_list, table_name = ""):
    result = ""
    if table_name != "":
        for fld in fields_list: result += "{:s}.{:s},".format(table_name, fld)
    else:
        for fld in fields_list: result += "{:s},".format(fld)

    return result[:-1]


def fields_upd_str(fields_list, table1_name, table2_name):
    result = ""
    for fld in fields_list: result += "{0}.{2} = {1}.{2},".format(table1_name, table2_name, fld)

    return result[:-1]


def fields_types_str(fields_dict):
    result = ""
    for fld in fields_dict.keys(): result += "%s %s," % (fld, fields_dict[fld])

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
    
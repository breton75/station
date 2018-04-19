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
                "CREATEDTIME"]
                        
MS_Reserves_FIELDS = ["ReservId",
                      "CreatedTime",
                      "UpdatedTime",
                      "PersonId",
                      "PersonName",
                      "PersonBirthdate",
                      "PersonCountry",
                      "PersonSex",
                      "SegmentId",
                      "SegmentName",
                      "SegmentShortName",
                      "PeriodBegin",
                      "PeriodEnd",
                      "HotelName",
                      "HotelRoom"]

MS_ReservesByDays_FIELDS = ["КодБронирования",
                      "Сегмент",
                      "Источник",
                      "Отель",
                      "НомерКомнаты",
                      "ФИО",
                      "Пол",
                      "Страна",
                      "ЭлектроннаяПочта"]

def select_for_insert_query(last_update):
  script = "WITH P as (" + "\n" + \
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
           "    Admin.PERSON.PERSONNUM AS Num," + "\n" + \
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
           "  Admin.RESERVE.ID as КодБронирования," + "\n" + \
           "  Person.SegmentName AS Сегмент," + "\n" + \
           "  \"kk\" AS Источник," + "\n" + \
           "  Hotel.Name AS Отель," + "\n" + \
           "  Hotel.Room AS НомерКомнаты," + "\n" + \
           "  Person.Name AS ФИО," + "\n" + \
           "  Person.Sex AS Пол," + "\n" + \
           "  Person.Country AS Страна," + "\n" + \
           "  Person.Email AS ЭлектроннаяПочта," + "\n" + \
           "  Admin.RESERVE.CREATEDTIME AS CreatedTime," + "\n" + \
           "  Admin.RESERVE.UPDATEDTIME AS UpdatedTime," + "\n" + \
           "  Admin.RESERVE.ROOMPRICE AS Сумма," + "\n" + \
           "  Person.Birthdate AS PersonBirthdate," + "\n" + \
           "  Admin.PERIOD.BEGINDATE AS PeriodBegin," + "\n" + \
           "  Admin.PERIOD.ENDDATE AS PeriodEnd" + "\n" + \
           "FROM" + "\n" + \
           "  Admin.RESERVE" + "\n" + \
           "LEFT JOIN Person ON" + "\n" + \
           "  Admin.RESERVE.ID = Person.ReservId" + "\n" + \
           "LEFT JOIN Hotel ON" + "\n" + \
           "  Admin.RESERVE.ID = Hotel.ReserveId" + "\n" + \
           "LEFT JOIN Admin.PERIOD ON" + "\n" + \
           "  Admin.PERIOD.RESERVID = Admin.RESERVE.ID" + "\n" + \
           "LEFT JOIN Admin.PERMANENTAGENT ON" + "\n" + \
           "  Admin.PERMANENTAGENT.ID = Admin.RESERVE.TRAVELAGENTID" + "\n" + \
           "WHERE" + "\n" + \
           "  Admin.RESERVE.CREATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'" + "\n" + \
           "ORDER BY Admin.RESERVE.CREATEDTIME ASC"

  return script


def select_for_update_query(last_update):
        return "select " + defs.fields_str(FIELD_NAMES, 'RESERVE') + " from Admin." + 'RESERVE' + "\n" + \
        " where RESERVE.CREATEDTIME < '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "\n" + \
        "' and RESERVE.UPDATEDTIME > '" + last_update.strftime('%Y/%m/%d %H:%M:%S') + "'"
WITH P as(
	SELECT
		Admin.PERSON.RESERVID as ReservId,
		MIN( Admin.PERSON.ID ) as PersonId
	FROM
		Admin.PERSON
	GROUP BY
		RESERVID
),
Segment AS(
	SELECT
		Admin.PERSONSEGMENTRELATION.SEGMENTID AS Id,
		Admin.PERSONSEGMENT.NAME as Name,
		Admin.PERSONSEGMENT.SHORTNAME as ShortName,
		Admin.PERSONSEGMENTRELATION.PERSONID as PersonId
	FROM
		Admin.PERSONSEGMENTRELATION
	LEFT JOIN Admin.PERSONSEGMENT on
		Admin.PERSONSEGMENT.ID = Admin.PERSONSEGMENTRELATION.SEGMENTID
),
Person AS(
	SELECT
		P.PersonId as Id,
		P.ReservId as ReservId,
		(
			Admin.PERSON.SURNAME + ' ' + Admin.PERSON.FIRSTNAME
		) as Name,
		Admin.PERSON.PERSONNUM AS Num,
		Admin.PERSON.BIRTHDATE AS Birthdate,
		Admin.PERSON.COUNTRY AS Country,
		Admin.PERSON.SEX AS Sex,
		Admin.PERSON.EMAIL AS Email,
		Segment.Id AS SegmentId,
		Segment.Name AS SegmentName,
		Segment.ShortName AS SegmentShortName
	FROM
		P
	LEFT JOIN Admin.PERSON on
		P.PersonId = Admin.PERSON.ID
	LEFT JOIN Segment ON
		P.PersonId = Segment.PersonId
),
R AS(
	SELECT
		Admin.ROOMTYPE.ID AS TypeId,
		Admin.MAINROOMTYPE.NAME AS HotelName
	FROM
		Admin.ROOMTYPE
	LEFT JOIN Admin.MAINROOMTYPE ON
		Admin.MAINROOMTYPE.ID = Admin.ROOMTYPE.MAINTYPEID
),
Hotel AS(
	SELECT
		R.HotelName AS Name,
		Admin.ROOM."NUMBER" AS Room,
		Admin.PERIOD.RESERVID AS ReserveId
	FROM
		Admin.ROOM
	LEFT JOIN Admin.PERIOD ON
		Admin.PERIOD.ROOMID = Admin.ROOM.ID
	LEFT JOIN R ON
		Admin.ROOM.TYPEID = R.TypeId
) SELECT
--	count()
 Admin.RESERVE.ID as ReservId,
	Admin.RESERVE.CREATEDTIME AS CreatedTime,
	Admin.RESERVE.UPDATEDTIME AS UpdatedTime,
	Person.Id AS PersonId,
	Person.Name AS PeronName,
	Person.Birthdate AS PersonBirthdate,
	Person.Country AS PersonCountry,
	Person.Sex AS PersonSex,
	Person.SegmentId AS SegmentId,
	Person.SegmentName AS SegmentName,
	Person.SegmentShortName AS SegmentShortName,
	Admin.PERIOD.BEGINDATE AS PeriodBegin,
	Admin.PERIOD.ENDDATE AS PeriodEnd,
	Hotel.Name AS HotelName,
	Hotel.Room AS HotelRoom
FROM
	Admin.RESERVE
LEFT JOIN Person ON
	Admin.RESERVE.ID = Person.ReservId
LEFT JOIN Hotel ON
	Admin.RESERVE.ID = Hotel.ReserveId
LEFT JOIN Admin.PERIOD ON
	Admin.PERIOD.RESERVID = Admin.RESERVE.ID
LEFT JOIN Admin.PERMANENTAGENT ON
	Admin.PERMANENTAGENT.ID = Admin.RESERVE.TRAVELAGENTID
WHERE
	Admin.RESERVE.CREATEDTIME > '2018-04-01 00:00:00'
 --	and 
-- Admin.RESERVE.RESNUM = 145780 --Admin.RESERVE.ID
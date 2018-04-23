WITH P as(
	SELECT
		Admin.PERSON.RESERVID as ReservId,
		MIN( Admin.PERSON.ID ) as PersonId
	FROM
		Admin.PERSON
	WHERE
		Admin.PERSON.RESERVID IS NOT NULL
	GROUP BY
		RESERVID
),
PSR AS(
	SELECT
		Admin.PERSONSEGMENTRELATION.PERSONID as PersonId,
		MIN( Admin.PERSONSEGMENTRELATION.SEGMENTID ) AS SegmentId
	FROM
		Admin.PERSONSEGMENTRELATION
	WHERE
		PersonId IS NOT NULL
	GROUP BY
		PersonId
),
Segment AS(
	SELECT
		PSR.PersonId AS PersonId,
		Admin.PERSONSEGMENT.NAME as Name,
		Admin.PERSONSEGMENT.SHORTNAME as ShortName
	FROM
		PSR
	LEFT JOIN Admin.PERSONSEGMENT on
		Admin.PERSONSEGMENT.ID = PSR.SegmentId
),
Person AS(
	SELECT
		P.PersonId as Id,
		P.ReservId as ReservId,
		(
			Admin.PERSON.SURNAME + ' ' + Admin.PERSON.FIRSTNAME
		) as Name,
		Admin.PERSON.SURNAME2 AS Chanel,
		Admin.PERSON.BIRTHDATE AS Birthdate,
		Admin.PERSON.COUNTRY AS Country,
		Admin.PERSON.SEX AS Sex,
		Admin.PERSON.EMAIL AS Email,
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
	count(DISTINCT Admin.RESERVE.ID ) --             Admin.RESERVE.ID as F_ReservId,
 --             Admin.RESERVE.CREATEDTIME AS  F_CreatedTime,
 --             Admin.RESERVE.UPDATEDTIME AS  F_UpdatedTime ,
 --             Admin.RESERVE.ROOMPRICE AS  F_Sum ,
 --             Admin.RESERVE.ISDELETED AS F_IsDeleted ,
 --             Person.SegmentName AS F_Segment ,
 --             Hotel.Name AS  F_Hotel ,
 --             Hotel.Room AS  F_Room ,
 --             Person.Name AS  F_Fio ,
 --             Person.Sex AS  F_Sex ,
 --             Person.Country AS  F_Country ,
 --             Person.Email AS  F_Email ,
 --             Person.Chanel AS  F_Chanel ,
 --             Person.Birthdate AS F_Birthdate ,
 --             Admin.PERIOD.BEGINDATE AS F_PeriodBegin ,
 --             Admin.PERIOD.ENDDATE AS F_PeriodEnd 
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
where
	Admin.RESERVE.ISDELETED = 0
	and Admin.RESERVE.ISDELETED is not NULL
	and Admin.RESERVE.id is not NULL
	and Admin.RESERVE.ID > 0
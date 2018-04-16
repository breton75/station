WITH aaa as(
	SELECT
		Admin.PERSON.RESERVID as rid,
		MIN( Admin.PERSON.ID ) as pid
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
		aaa.pid as Id,
		aaa.rid as ReservId,
		(
			Admin.PERSON.SURNAME + ' ' + Admin.PERSON.FIRSTNAME
		) as Name,
		Admin.PERSON.PERSONNUM AS Num,
		Admin.PERSON.BIRTHDATE AS Birthdate,
		Admin.PERSON.COUNTRY AS Country,
		Admin.PERSON.SEX AS Sex,
		Segment.Id AS SegmentId,
		Segment.Name AS SegmentName,
		Segment.ShortName AS SegmentShortName
	FROM
		aaa
	LEFT JOIN Admin.PERSON on
		aaa.pid = Admin.PERSON.ID
	LEFT JOIN Segment ON
		aaa.pid = Segment.PersonId
) SELECT
	--	count()
	Admin.RESERVE.ID as ReservId,
	Admin.RESERVE.CREATEDTIME AS CreatedDate,
	Person.Id,
	Person.Name,
	Person.Birthdate,
	Person.Country,
	Person.Sex,
	Person.SegmentId,
	Person.SegmentName,
	Person.SegmentShortName,
	Admin.PERIOD.BEGINDATE,
	Admin.PERIOD.ENDDATE
--	Admin.PERIOD
FROM
	Admin.RESERVE
LEFT JOIN Person ON
	Admin.RESERVE.ID = Person.ReservId
LEFT JOIN Admin.PERIOD ON
	Admin.PERIOD.RESERVID = Admin.RESERVE.ID
WHERE
	--	Admin.RESERVE.CREATEDTIME > '2018-04-01 00:00:00'
 --	and 
 Admin.RESERVE.RESNUM = 145780 --Admin.RESERVE.ID
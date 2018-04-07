import pandas as pd
import pyodbc
import pypyodbc
import sqlalchemy
import codecs, sys
import etl_sql
from datetime import datetime, date, time
import os

import RESERVE
import PERSON
import PERIOD

import defs

bcp_cmd = "bcp.exe {:s}.{:s} in {:s} -d {:s} -S {:s} -t \"{:s}\" -c -C 65001 -m 0 -U {:s} -P {:s}"
bcp_file = "{:s}{:s}.bcp"
separator = '|'

mssql_server = 'shotels.database.windows.net'
mssql_database = 'test'
mssql_username = 'StationHotels'
mssql_password = 'Station12345'
mssql_schema = 'sv'
# mssql_driver= '{ODBC Driver 13 for SQL Server}'
mssql_driver= 'ODBC+Driver+13+for+SQL+Server'

# edel_server = '172.16.0.12'
# edel_database = 'test'
# edel_username = 'StationHotels'
# edel_password = 'Station12345'
# edel_driver= '{ODBC Driver 13 for SQL Server}'
# edel_driver= 'ODBC+Driver+13+for+SQL+Server'




def connectMSSQL() :

	# addr = "mssql+pyodbc://" + mssql_username + ":" + mssql_password+"@" + mssql_server + "/" + mssql_database + "?driver=" + mssql_driver + "&convert_unicode=True"
	cnxn = pypyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + mssql_server + ';DATABASE=' + mssql_database + ';UID=' + mssql_username + ';PWD=' + mssql_password)
	if not cnxn:
		return None

	return cnxn


def connectEdelweis():
	edel = pypyodbc.connect('DSN=Edel')

	if not edel:
		return None

	return edel





def import_table_data(edel_connection, query, table_name, fields_list, schema_name) :

	result = []

	# print (query)
	cur = edel_connection.cursor()
	cur.execute(query) #pd.read_sql(query, edel_connection)


	for row in cur.fetchall():
		
		if not row:
			return None
		
		vals = ""
		for i in range(len(fields_list)):
			vals += defs.value_str(row[i])

		result.append("insert into " + schema_name + "." + table_name + " (" + defs.fields_str(fields_list) + ") values(" + vals[:-1] + ")")
		# print(4)
	
	cur.close()    	

	return result

	# print(query_insert)


def create_csv(filename):
	f = open(filename, "w", encoding="utf8")



# def export_RESERVE(mssql_connection, data, table, schema='sv') :
# 	#charset=utf8
# 	addr = "mssql+pyodbc://"+username+ ":"+password+"@"+server+"/"+database+"?driver="+driver+"&convert_unicode=True"
# 	# print(addr)
# 	engine = sqlalchemy.create_engine(addr)
# 	#cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
# 	#data.to_sql(name=table, con=cnxn, if_exists = 'append', index=False)
# 	data.to_sql(name=table, con=engine, schema=schema, if_exists = 'replace', index=False), dtype = sqlcol(data))


def get_tables_last_updates(mssql_connection):

	tbl_list = {}
	cur = mssql_connection.cursor()

	cur.execute("select tbl_name, last_update from sv.tbl_upd")

	for row in cur.fetchall():
		if not row:
			return None

		tbl_list.update({row['tbl_name']: row['last_update']})


	return tbl_list


# ***************  НАЧАЛО КОДА  ****************** #

try:
	edel = connectEdelweis()
	if not edel: raise Exception('connectEdelweis()')
	print("connected Edelweis")

	mssql = connectMSSQL()
	if not mssql: raise Exception('connectMSSQL()')
	print('connected MSSQL')
	
	mssql.cursor().execute("CREATE TABLE test.sv.PERSON_upd (ID int, RESERVID int, TITLE varchar(10))")
	print('connected MSSQL666')

	tables_last_updates = get_tables_last_updates(mssql)
	if tables_last_updates == None: raise Exception('get_tables_last_updates()')
	
	cwd = os.getcwd().replace('\u005c', '/')
	if len(cwd) > 0 and cwd[-1] != '/': cwd += '/'


	# # ----------- RESERVE ------------- #
	TBLNAME = "RESERVE"

	last_update = tables_last_updates[TBLNAME]
	if last_update == None: raise Exception("Last update value for table {} not found".format(TBLNAME))
	print("last update {:s} {:s}".format(TBLNAME, last_update.strftime('%Y/%m/%d %H:%M:%S')))

	current_time = datetime.now()

	# добавление
	edel_RESERVE_data = pd.read_sql(RESERVE.select_for_insert_query(last_update), edel)
	print("{:s} ins readed from Edelweis".format(TBLNAME))

	edel_RESERVE_data.to_csv(path_or_buf=bcp_file.format(cwd, TBLNAME), index=False, sep=separator)
	print("{:d} records for insert to {:s}".format(len(edel_RESERVE_data), TBLNAME))
	
	cmd = str(bcp_cmd).format(mssql_schema, TBLNAME, bcp_file.format(cwd, TBLNAME), mssql_database, mssql_server, separator, mssql_username, mssql_password)
	os.system(cmd)
	print(cmd)


	# обновление
	edel_RESERVE_data = pd.read_sql(RESERVE.select_for_update_query(last_update), edel)
	print("{:s} upd readed from Edelweis".format(TBLNAME))


	mssql.cursor().execute("select * into {:0}.{:1}_upd from {:0}.{:1} where 0 = 1".format(mssql_schema, TBLNAME))
	mssql.commit()
	
	TBLNAME_upd = TBLNAME + "_upd"
	
	edel_RESERVE_data.to_csv(path_or_buf=bcp_file.format(cwd, TBLNAME_upd), index=False, sep=separator)
	print("{:d} records for update to {:s}".format(len(edel_RESERVE_data), TBLNAME_upd))
	
	cmd = str(bcp_cmd).format(mssql_schema, TBLNAME, bcp_file.format(cwd, TBLNAME_upd), mssql_database, mssql_server, separator, mssql_username, mssql_password)
	os.system(cmd)
	print(cmd)


	"UPDATE {0}.{1} SET RESERVID = sv.PERSON.RESERVID,
TITLE = sv.PERSON.TITLE
FROM sv.PERSON
WHERE sv.PERSON.ID = sv.PERSON_upd.ID

	mssql.cursor().execute("select * into {:0}.{:1}_upd from {:0}.{:1} where 0 = 1".format(mssql_schema, TBLNAME))
	mssql.commit()


	# # # ----------- PERSON ------------- #
	# last_update = tables_last_updates['PERSON']
	# if last_update == None: raise Exception("Last update value for table PERSON not found")
	# print("last update PERSON {:s}".format(last_update.strftime('%Y/%m/%d %H:%M:%S')))

	# edel_PERSON_data = pd.read_sql(PERSON.select_for_insert_query(last_update), edel)
	# print("PERSON readed from Edelweis")

	# edel_PERSON_data.to_csv(path_or_buf=bcp_file.format(cwd, "PERSON"), index=False, sep=separator)
	# print("{:d} records for insert to PERSON".format(len(edel_PERSON_data)))
	
	# print(str(bcp_cmd).format(mssql_schema, "PERSON", bcp_file.format(cwd, "PERSON"), mssql_database, mssql_server, separator, mssql_username, mssql_password))
	# os.system(str(bcp_cmd).format(mssql_schema, "PERSON", bcp_file.format(cwd, "PERSON"), mssql_database, mssql_server, separator, mssql_username, mssql_password))


	# # # ----------- PERIOD ------------- #
	# last_update = tables_last_updates['PERIOD']
	# if last_update == None: raise Exception("Last update value for table PERIOD not found")
	# print("last update PERIOD {:s}".format(last_update.strftime('%Y/%m/%d %H:%M:%S')))

	# edel_PERIOD_data = pd.read_sql(PERIOD.select_for_insert_query(last_update), edel)
	# print("PERIOD readed from Edelweis")

	# edel_PERIOD_data.to_csv(path_or_buf=bcp_file.format(cwd, "PERIOD"), index=False, sep=separator)
	# print("{:d} records for insert to PERIOD".format(len(edel_PERSON_data)))
	
	# print(str(bcp_cmd).format(mssql_schema, "PERIOD", bcp_file.format(cwd, "PERIOD"), mssql_database, mssql_server, separator, mssql_username, mssql_password))
	# os.system(str(bcp_cmd).format(mssql_schema, "PERIOD", bcp_file.format(cwd, "PERIOD"), mssql_database, mssql_server, separator, mssql_username, mssql_password))




	# export_RESERVE(mssql, edelRESERVEdata, 'RESERVE', 'sv')
	edel.close()
	mssql.cursor().close()
	mssql.close()

except Exception as E:
	print('error in main function: %s' % E, file=sys.stderr)

	if edel: edel.close()
	if mssql: mssql.close()

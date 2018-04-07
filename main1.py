import pandas as pd
import pyodbc
import pypyodbc
import sqlalchemy
import codecs, sys
import etl_sql
from datetime import datetime, date, time

import RESERVE
import PERSON
import PERIOD

import defs

bcp_cmd = "bcp.exe {:s} in {:s} -d {:s} -S {:s} -t \"{:s}\" -c -C 65001 -m 0 -U {:s} -P {:s}"
bcp_file = "{:s}.bcp"
separator = '|'

mssql_server = 'shotels.database.windows.net'
mssql_database = 'test'
mssql_username = 'StationHotels'
mssql_password = 'Station12345'
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
	


	tables_last_updates = get_tables_last_updates(mssql)
	if tables_last_updates == None: raise Exception('get_tables_last_updates()')
	

	# # ----------- RESERVE ------------- #
	last_update = tables_last_updates['RESERVE']
	if last_update == None: raise Exception("Last update value for table RESERVE not found")
	print("last update RESERVE {:s}".format(last_update.strftime('%Y/%m/%d %H:%M:%S')))

	# edel_RESERVE_data = import_TABLE_DATA_for_insert(edel, RESERVE.select_for_insert_query(last_update), 'RESERVE', RESERVE.RESERVE_FIELD_NAMES, "sv")
	
	edel_RESERVE_data = pd.read_sql(PERIOD.select_for_insert_query(last_update), edel)
	print("RESERVE readed from Edelweis")

	edel_RESERVE_data.to_csv(path_or_buf=bcp_file.format("RESERVE"), index=False, sep=separator)
	print("{:d} records for insert to RESERVE".format(len(edel_RESERVE_data)))
	
	os.system(bcp_cmd.format("RESERVE", bcp_file.format("RESERVE"), mssql_database, mssql_server, separator, mssql_username, mssql_password).encode("cp1251"))

	# for x in edel_RESERVE_data:
	# 	mssql.cursor().execute(x)

	# mssql.commit()
	# print("RESERVE insert commited\n")




	# # ----------- PERSON ------------- #
	# last_update = tables_last_updates['PERSON']
	# if last_update == None: raise Exception("Last update value for table PERSON not found")
	# print("last update PERSON {:s}".format(last_update.strftime('%Y/%m/%d %H:%M:%S')))

	# edel_PERSON_data = import_TABLE_DATA_for_insert(edel, PERSON.select_for_insert_query(last_update), 'PERSON', PERSON.PERSON_FIELD_NAMES, "sv")
	# print("{:d} records for insert PERSON".format(len(edel_PERSON_data)))

	# for x in edel_PERSON_data:
	# 	mssql.cursor().execute(x)

	# mssql.commit()
	# print("PERSON insert commited\n")



	# # ----------- PERIOD ------------- #
	# last_update = tables_last_updates['PERIOD']
	# if last_update == None: raise Exception("Last update value for table PERIOD not found")
	# print("last update PERIOD {:s}".format(last_update.strftime('%Y/%m/%d %H:%M:%S')))

	# edel_PERIOD_data = import_TABLE_DATA_for_insert(edel, PERIOD.select_for_insert_query(last_update), 'PERIOD', PERIOD.PERIOD_FIELD_NAMES, "sv")
	# print("{:d} records for insert PERIOD".format(len(edel_PERIOD_data)))

	# for x in edel_PERIOD_data:
	# 	mssql.cursor().execute(x)

	# mssql.commit()
	# print("PERIOD insert commited\n")



	# export_RESERVE(mssql, edelRESERVEdata, 'RESERVE', 'sv')
	ed.close()
	mssql.cursor().close()
	mssql.close()

except Exception as E:
	print('error in main function: %s' % E, file=sys.stderr)

	if ede: ed.close()
	if mssql: mssql.close()

import pandas as pd
import pyodbc
import pypyodbc
import sqlalchemy
import codecs, sys
import etl_sql
from datetime import datetime, date, time
import subprocess
import os

import RESERVE
import PERSON
import PERIOD

import defs

bcpCMD = "bcp.exe {:s}.{:s} in {:s} -d {:s} -S {:s} -t \"{:s}\" -c -C 65001 -m 0 -U {:s} -P {:s}"
bcpFILE = "{:s}{:s}.bcp"
bcpSEP = '|'
sUPD = "_upd"

minDATE = datetime(2010, 1, 1)

mssql_SERVER = 'shotels.database.windows.net'
mssql_DATABASE = 'test'
mssql_USERNAME = 'StationHotels'
mssql_PASSWORD = 'Station12345'
mssql_SCHEMA = 'sv'
mssql_DRIVER= 'ODBC+Driver+13+for+SQL+Server'
mssql_TABLE_UPDATES = "tbl_upd"

# edel_server = '172.16.0.12'
# edel_database = 'test'
# edel_username = 'StationHotels'
# edel_password = 'Station12345'
# edel_driver= '{ODBC Driver 13 for SQL Server}'
# edel_driver= 'ODBC+Driver+13+for+SQL+Server'




def connectMSSQL() :

	# addr = "mssql+pyodbc://" + mssql_USERNAME + ":" + mssql_PASSWORD+"@" + mssql_SERVER + "/" + mssql_DATABASE + "?driver=" + mssql_driver + "&convert_unicode=True"
	cnxn = pypyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + mssql_SERVER + ';DATABASE=' + mssql_DATABASE + ';UID=' + mssql_USERNAME + ';PWD=' + mssql_PASSWORD)
	if not cnxn:
		return None

	return cnxn


def connectEdelweis():
	edel = pypyodbc.connect('DSN=Edel')

	if not edel:
		return None

	return edel


def get_tables_last_insertions(mssql_connection):

	tbl_list = {}
	cur = mssql_connection.cursor()

	cur.execute("select tbl_name, last_insert from %s.%s" % (mssql_SCHEMA, mssql_TABLE_UPDATES))

	for row in cur.fetchall():
		if not row:
			return None

		tbl_list.update({row['tbl_name']: row['last_insert']})


	return tbl_list


def get_tables_last_updates(mssql_connection):

	tbl_list = {}
	cur = mssql_connection.cursor()

	cur.execute("select tbl_name, last_update from %s.%s" % (mssql_SCHEMA, mssql_TABLE_UPDATES))

	for row in cur.fetchall():
		if not row:
			return None

		tbl_list.update({row['tbl_name']: row['last_update']})


	return tbl_list


def get_last_insert(mssql, tables_last_insertions, tableNAME):

	last_insert = tables_last_insertions[tableNAME]

	if last_insert == None: 
		last_insert = minDATE # raise Exception("Last insert value for table {} not found".format(tableNAME))
				
		mssql.cursor().execute("delete from %s.%s" % (mssql_SCHEMA, tableNAME))
		mssql.commit()
	
	print("last insert %s %s" % (tableNAME, last_insert.strftime('%Y/%m/%d %H:%M:%S')))

	return last_insert

def get_last_update(mssql, tables_last_updates, tableNAME):

	last_update = tables_last_updates[tableNAME]

	if last_update == None: 
		last_update = minDATE # raise Exception("Last insert value for table {} not found".format(tableNAME))
	
	print("last update %s %s" % (tableNAME, last_update.strftime('%Y/%m/%d %H:%M:%S')))

	return last_update


def insert_update(tableNAME, CWD, currentTIME, mssql, edel, select_for_insert_query, select_for_update_query, donotUpdate):
	try:
	
		# добавление
		edel_data = pd.read_sql(select_for_insert_query, edel)
	
		edel_data.to_csv(path_or_buf=bcpFILE.format(CWD, tableNAME), index=False, sep=bcpSEP, header=False)
		print("{:d} records for insert to {:s}".format(len(edel_data), tableNAME))
			
		cmd = str(bcpCMD).format(mssql_SCHEMA, tableNAME, bcpFILE.format(CWD, tableNAME), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd) #, shell=True, stdin=PIPE, stdout=PIPE, stderr=subprocess.STDOUT, close_fds=True)
		print(cmd)
	
		mssql.cursor().execute("update %s.%s set last_insert = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), tableNAME))
		if donotUpdate:
			mssql.cursor().execute("update %s.%s set last_update = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), tableNAME))
			print("%s updated" % (tableNAME))
	
		mssql.commit()

		print("{:d} records inserted to {:s}".format(len(edel_data), tableNAME))
	
		if donotUpdate:
			return

		
		# обновление
		edel_data = pd.read_sql(select_for_update_query, edel)
		print("%s updates readed from Edelweis" % (tableNAME))
	
			
		tableNAME_upd = tableNAME + sUPD
			
		edel_data.to_csv(path_or_buf=bcpFILE.format(CWD, tableNAME_upd), index=False, sep=bcpSEP, header=False)
		print("%d records for update to %s" % (len(edel_data), tableNAME_upd))
		
		try:
			mssql.cursor().execute("drop table %s.%s" % (mssql_SCHEMA, tableNAME_upd))
		except Exception as E:
			pass

		mssql.cursor().execute("select * into {0}.{1} from {0}.{2} where 0 = 1".format(mssql_SCHEMA, tableNAME_upd, tableNAME))
		mssql.commit()
			
		cmd = str(bcpCMD).format(mssql_SCHEMA, tableNAME, bcpFILE.format(CWD, tableNAME_upd), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd)
		print(cmd)
	
		fupdstr = defs.fields_upd_str(RESERVE.RESERVE_FIELD_NAMES, tableNAME, tableNAME_upd)
		mssql.cursor().execute("UPDATE {0}.{1} SET {3} from {0}.{2} where {0}.{1}.ID = {0}.{2}.ID ".format(mssql_SCHEMA, tableNAME, tableNAME_upd, fupdstr))
		mssql.cursor().execute("update {0}.{1} set last_update = '{2}' where tbl_name = '{3}'".format(mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), tableNAME))
		mssql.cursor().execute("drop table %s.%s" % (mssql_SCHEMA, tableNAME_upd))
		mssql.commit()

		print("{:d} records updated in {:s}".format(len(edel_data), tableNAME))


	except Exception as E:
		print('error on update %s: %s' % (tableNAME, E), file=sys.stderr)



# ***************  НАЧАЛО КОДА  ****************** #

try:
	edel = connectEdelweis()
	if not edel: raise Exception('connectEdelweis()')
	print("connected Edelweis")

	mssql = connectMSSQL()
	if not mssql: raise Exception('connectMSSQL()')
	print('connected MSSQL')

	tables_last_insertions = get_tables_last_insertions(mssql)
	if tables_last_insertions == None: raise Exception('get_tables_last_insertions()')
	
	tables_last_updates = get_tables_last_updates(mssql)
	if tables_last_updates == None: raise Exception('get_tables_last_updates()')
	
	CWD = os.getcwd().replace('\u005c', '/')
	if len(CWD) > 0 and CWD[-1] != '/': CWD += '/'
	
	currentTIME = datetime.now()


	# # ----------- RESERVE ------------- #
	tableNAME = "RESERVE"
	last_insert = get_last_insert(mssql, tables_last_insertions, tableNAME)
	last_update = get_last_update(mssql, tables_last_insertions, tableNAME)
	select_for_insert_query = RESERVE.select_for_insert_query(last_insert)
	select_for_update_query = RESERVE.select_for_update_query(last_update)

	insert_update(tableNAME, CWD, currentTIME, mssql, edel, select_for_insert_query, select_for_update_query, last_insert == minDATE)


	# # # ----------- PERSON ------------- #
	tableNAME = "PERSON"
	last_insert = get_last_insert(mssql, tables_last_insertions, tableNAME)
	last_update = get_last_update(mssql, tables_last_insertions, tableNAME)
	select_for_insert_query = PERSON.select_for_insert_query(last_insert)
	select_for_update_query = PERSON.select_for_update_query(last_update)

	insert_update(tableNAME, CWD, currentTIME, mssql, edel, select_for_insert_query, select_for_update_query, last_insert == minDATE)

	# # # ----------- PERIOD ------------- #
	tableNAME = "PERIOD"
	last_insert = get_last_insert(mssql, tables_last_insertions, tableNAME)
	last_update = get_last_update(mssql, tables_last_insertions, tableNAME)
	select_for_insert_query = PERIOD.select_for_insert_query(last_insert)
	select_for_update_query = PERIOD.select_for_update_query(last_update)

	insert_update(tableNAME, CWD, currentTIME, mssql, edel, select_for_insert_query, select_for_update_query, last_insert == minDATE)


	# export_RESERVE(mssql, edelRESERVEdata, 'RESERVE', 'sv')
	edel.close()
	mssql.cursor().close()
	mssql.close()

except Exception as E:
	print('error in main function: %s' % E, file=sys.stderr)

	if edel: edel.close()
	if mssql: mssql.close()

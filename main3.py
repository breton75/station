# -*- coding: utf-8 -*-

import pandas as pd
import pyodbc
import pypyodbc
import sqlalchemy
import codecs, sys
import etl_sql
from datetime import datetime, date, time, timedelta
import subprocess
import os

# import ReservesByDays
# import Reserves


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
		
		try:
			mssql.cursor().execute("delete from %s.%s" % (mssql_SCHEMA, tableNAME))
			mssql.commit()
		except:
			pass
	
	# print("last insert %s %s" % (tableNAME, last_insert.strftime('%Y/%m/%d %H:%M:%S')))

	return last_insert


def get_last_update(mssql, tables_last_updates, tableNAME):

	last_update = tables_last_updates[tableNAME]

	if last_update == None: 
		last_update = minDATE # raise Exception("Last insert value for table {} not found".format(tableNAME))
	
	# print("last update %s %s" % (tableNAME, last_update.strftime('%Y/%m/%d %H:%M:%S')))

	return last_update


def insert_update_Reserves(CWD, currentTIME, mssql, edel, last_insert, last_update, FIELD_NAMES, fullUpdate):
	try:
		
		T_Reserves 		= "Reserves"
		T_ReservesByDays = "ReservesByDays"
		T_Reserves_upd = T_Reserves + sUPD
		T_ReservesByDays_upd = T_ReservesByDays + sUPD

		low_fields = []
		for fld in defs.MS_Reserves_FIELDS.keys(): low_fields.append(fld.lower())

		if fullUpdate:
			try:
				mssql.cursor().execute("DROP TABLE %s.%s" % (mssql_SCHEMA, T_Reserves))
			except Exception as E:
				pass

			try:
				mssql.cursor().execute("DROP TABLE %s.%s" % (mssql_SCHEMA, T_ReservesByDays))
			except Exception as E:
				pass

		try:
			mssql.cursor().execute("CREATE TABLE %s.%s (%s)" % (mssql_SCHEMA, T_Reserves, defs.fields_types_str(defs.MS_Reserves_FIELDS)))
			mssql.commit()
		except Exception as E:
			pass

		try:
			mssql.cursor().execute("CREATE TABLE %s.%s (%s)" % (mssql_SCHEMA, T_ReservesByDays, defs.fields_types_str(defs.MS_ReservesByDays_FIELDS)))
			mssql.commit()
		except Exception as E:
			pass

		# добавление
		edel_data = pd.read_sql(defs.select_for_insert_query(last_insert), edel) #, index_col=ReservesByDays.MS_ReservesByDays_FIELDS)

		# Брони
		edel_data.to_csv(path_or_buf=bcpFILE.format(CWD, T_Reserves), index=False, sep=bcpSEP, header=False, columns=low_fields)
		print("%d records written to %s" % (len(edel_data), bcpFILE.format(CWD, T_Reserves)))

		# БрониПоДням
		if not write_ReservesByDays_to_bcp(edel_data, T_ReservesByDays):
			raise Exception("file not written")


		cmd = str(bcpCMD).format(mssql_SCHEMA, T_Reserves, bcpFILE.format(CWD, T_Reserves), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd) #, shell=True, stdin=PIPE, stdout=PIPE, stderr=subprocess.STDOUT, close_fds=True)
		print(cmd)

		cmd = str(bcpCMD).format(mssql_SCHEMA, T_ReservesByDays, bcpFILE.format(CWD, T_ReservesByDays), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd) #, shell=True, stdin=PIPE, stdout=PIPE, stderr=subprocess.STDOUT, close_fds=True)
		print(cmd)

		mssql.cursor().execute("update %s.%s set last_insert = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), T_Reserves))
		mssql.cursor().execute("update %s.%s set last_insert = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), T_ReservesByDays))
		if fullUpdate:
			mssql.cursor().execute("update %s.%s set last_update = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), T_Reserves))
			mssql.cursor().execute("update %s.%s set last_update = '%s' where tbl_name = '%s'" % (mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), T_ReservesByDays))
			print("%s, %s updated" % (T_Reserves, T_ReservesByDays))
	
		mssql.commit()

		print("{:d} records inserted to {:s}\n".format(len(edel_data), T_Reserves))
	
		if fullUpdate:
			return
		
		# обновление
		edel_data = pd.read_sql(defs.select_for_update_query(last_update), edel)
			
		# Брони
		edel_data.to_csv(path_or_buf=bcpFILE.format(CWD, T_Reserves_upd), index=False, sep=bcpSEP, header=False, columns=low_fields)
		print("%d records written to %s" % (len(edel_data), bcpFILE.format(CWD, T_Reserves_upd)))

		# БрониПоДням
		if not write_ReservesByDays_to_bcp(edel_data, T_ReservesByDays_upd):
			raise Exception("file not written")


		try:
			mssql.cursor().execute("drop table %s.%s" % (mssql_SCHEMA, T_Reserves_upd))
		except Exception as E:
			pass

		try:
			mssql.cursor().execute("drop table %s.%s" % (mssql_SCHEMA, T_ReservesByDays_upd))
		except Exception as E:
			pass

		mssql.cursor().execute("select * into {0}.{1} from {0}.{2} where 0 = 1".format(mssql_SCHEMA, T_Reserves_upd, T_Reserves))
		mssql.cursor().execute("select * into {0}.{1} from {0}.{2} where 0 = 1".format(mssql_SCHEMA, T_ReservesByDays_upd, T_ReservesByDays))
		mssql.commit()
			
		cmd = str(bcpCMD).format(mssql_SCHEMA, T_Reserves_upd, bcpFILE.format(CWD, T_Reserves_upd), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd)
		print(cmd)
			
		cmd = str(bcpCMD).format(mssql_SCHEMA, T_ReservesByDays_upd, bcpFILE.format(CWD, T_ReservesByDays_upd), mssql_DATABASE, mssql_SERVER, bcpSEP, mssql_USERNAME, mssql_PASSWORD)
		p = subprocess.run(cmd)
		print(cmd)

		res_id_lst = ""
		for idx in range(len(edel_data.values)):
			res_id_lst += str(edel_data.loc[idx, defs.F_ReservId.lower()]) + ","

		fupdstr1 = defs.fields_upd_str(defs.MS_Reserves_FIELDS, T_Reserves, T_Reserves_upd)
		# fupdstr2 = defs.fields_upd_str(defs.MS_ReservesByDays_FIELDS, T_ReservesByDays, T_ReservesByDays_upd)

		mssql.cursor().execute("UPDATE {0}.{1} SET {3} FROM {0}.{2} WHERE {0}.{1}.{4} = {0}.{2}.{4} ".format(mssql_SCHEMA, T_Reserves, T_Reserves_upd, fupdstr1, defs.F_ReservId))
		mssql.cursor().execute("DELETE FROM {0}.{1} WHERE {2} IN ({3})".format(mssql_SCHEMA, T_ReservesByDays, defs.F_ReservId, res_id_lst[:-1]))
		mssql.cursor().execute("INSERT INTO {0}.{1} ({3}) SELECT {3} FROM {0}.{2}".format(mssql_SCHEMA, T_ReservesByDays, T_ReservesByDays_upd, defs.fields_str(defs.MS_ReservesByDays_FIELDS.keys())))
		mssql.cursor().execute("DELETE FROM {0}.{1} WHERE {2} IN (SELECT {2} FROM {0}.{3} WHERE {4} = 1)".format(mssql_SCHEMA, T_ReservesByDays, defs.F_ReservId, T_Reserves, defs.F_IsDeleted))
		mssql.cursor().execute("DELETE FROM {0}.{1} WHERE {2} = 1".format(mssql_SCHEMA, T_Reserves, defs.F_IsDeleted))
		mssql.cursor().execute("UPDATE {0}.{1} SET last_update = '{2}' where tbl_name = '{3}'".format(mssql_SCHEMA, mssql_TABLE_UPDATES, currentTIME.strftime('%Y/%m/%d %H:%M:%S'), T_Reserves))
		mssql.cursor().execute("DROP TABLE %s.%s" % (mssql_SCHEMA, T_Reserves_upd))
		mssql.cursor().execute("DROP TABLE %s.%s" % (mssql_SCHEMA, T_ReservesByDays_upd))
		mssql.commit()

		print("{:d} records updated in {:s}\n".format(len(edel_data), T_Reserves))


	except Exception as E:
		print('error on update %s: %s\n' % (T_Reserves, E), file=sys.stderr)


def write_ReservesByDays_to_bcp(data, filename):
	try:
    	# разбираем по отдельным дням и пишем в файл
		f = open(bcpFILE.format(CWD, filename), "w", encoding="utf8")
		if not f: raise Exception("cant open file %s.bcp for write" % filename)

		r_count = 0
		print("len", len(data.values))
		for idx in range(len(data.values)):
			dtb = data.loc[idx, defs.F_PeriodBegin.lower()]
			dte = data.loc[idx, defs.F_PeriodEnd.lower()]
			while dtb <= dte:
				f.write(dtb.isoformat() + bcpSEP + str(data.loc[idx, defs.F_Sum.lower()]) + bcpSEP + str(data.loc[idx, defs.F_ReservId.lower()]) + "\n")
				dtb += timedelta(days=1)
				r_count += 1

		f.close()

		print("%d records written to %s" % (r_count, bcpFILE.format(CWD, filename)))

		return True

	except Exception as E:
		print('error on write_ReservesByDays_to_bcp: %s\n' % (E), file=sys.stderr)
		return None



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


	'''\n----------- БрониПоДням ----------'''
	tableNAME = "Reserves"
	last_insert = get_last_insert(mssql, tables_last_insertions, tableNAME)
	last_update = get_last_update(mssql, tables_last_insertions, tableNAME)
	# select_for_insert_query = defs.select_for_insert_query(last_insert)
	# select_for_update_query = "" # RESERVE.select_for_update_query(last_update)

	insert_update_Reserves(CWD, currentTIME, mssql, edel, last_insert, last_update, defs.MS_ReservesByDays_FIELDS.keys(), last_insert == minDATE)


	# # # ----------- Брони ------------- #
	# tableNAME = "Reserves"
	# print("\n----------- %s ----------" % tableNAME)
	# last_insert = get_last_insert(mssql, tables_last_insertions, tableNAME)
	# last_update = get_last_update(mssql, tables_last_insertions, tableNAME)
	# select_for_insert_query = PERSON.select_for_insert_query(last_insert)
	# select_for_update_query = PERSON.select_for_update_query(last_update)

	# insert_update(tableNAME, CWD, currentTIME, mssql, edel, select_for_insert_query, select_for_update_query, PERSON.FIELD_NAMES, last_insert == minDATE)




	# export_RESERVE(mssql, edelRESERVEdata, 'RESERVE', 'sv')
	edel.close()
	mssql.cursor().close()
	mssql.close()

except Exception as E:
	print('error in main function: %s' % E, file=sys.stderr)

	if edel: edel.close()
	if mssql: mssql.close()

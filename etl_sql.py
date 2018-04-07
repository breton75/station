# -----------------------------------------------------------------------------
# coding: utf-8
# etl_csv.py
# -----------------------------------------------------------------------------
import pandas as pd
import pypyodbc
import sqlalchemy
import codecs, sys

#sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

server = 'shotels.database.windows.net'
database = 'test'
username = 'StationHotels'
password = 'Station12345'
driver= '{ODBC Driver 13 for SQL Server}'
driver= 'ODBC+Driver+13+for+SQL+Server'

def sqlcol(dfparam):    
  dtypedict = {}
  for i,j in zip(dfparam.columns, dfparam.dtypes):
    if "object" in str(j):
      dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
    if "datetime" in str(j):
      dtypedict.update({i: sqlalchemy.types.DateTime()})
    if "float" in str(j):
      dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
    if "int" in str(j):
      dtypedict.update({i: sqlalchemy.types.INT()})
  return dtypedict

  
def ExportMSSQL(data, table, schema=None) :
  #charset=utf8
  addr = "mssql+pyodbc://"+username+ ":"+password+"@"+server+"/"+database+"?driver="+driver+"&convert_unicode=True"
  print(addr)
  engine = sqlalchemy.create_engine(addr)
  #cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
  #data.to_sql(name=table, con=cnxn, if_exists = 'append', index=False)
  data.to_sql(name=table, con=engine, schema=schema, if_exists = 'replace', index=False, dtype = sqlcol(data))
    
    
# Connection function to use for sqlalchemy
"""
def Connection():
    #MDB = 'C:\\database.mdb'
    #DRV = '{Microsoft Access Driver (*.mdb)}'
    #connection_string = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=%s' % MDB
    #return pyodbc.connect('DRIVER={};DBQ={}'.format(DRV,MDB))
    return pyodbc.connect(r'DSN=MSAccessRevit') # ;UID=1;PWD=1
"""



def Import(name) :  
#    MDB = 'C:\\database.mdb'
#    DRV = 'Microsoft Access Driver (*.mdb)'
#    connection_string = 'Driver={Microsoft Access Driver (*. mdb)};DBQ=%s' % MDB
 #   return pyodbc.connect('DRIVER={};DBQ={}'.format(DRV,MDB)) 
 # connection = pyodbc.connect(r'DSN=MSAccessRevit;UID=1;PWD=1')
  engine = sqlalchemy.create_engine('mysql+pyodbc://', creator=Connection)
  
  #engine = sqlalchemy.create_engine('access+fix://admin@/%s'%("C:\\Users\\STATION-99\\Documents\\Revit.accdb"))
  #engine = sqlalchemy.create_engine(name)
  
  data = pd.read_sql_table("numbers", con=engine)
 
  return data 
  
#for x in pyodbc.drivers() :
#  print x    

def ImportTable(connection, table_name, columns) :
  col_names = u''
  for c in columns :
    if len(col_names) :
      col_names += ","
    col_names += c
    
  sql = u"SELECT " + col_names + u" FROM " + table_name 
  print (sql)
  data = pd.read_sql(sql, connection)
  return data
  
def ImportQuery(connection, query) :
  sql = query;
  print (sql)
  data = pd.read_sql(sql, connection)
  return data



def Connect() :
  conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=data\Revit.accdb;'  #    r'DBQ=C:\Users\STATION-99\Documents\Revit.accdb;'
  )
  cnxn = pyodbc.connect(conn_str)
  return cnxn
  
  
def Test(cnxn) :
  ""
  #ExportMSSQL(all, "Revit")

    
def Disconnect(cnxn) :
  pass
    
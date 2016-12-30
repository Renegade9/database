import datetime
import decimal
import pymssql

# Doc:  https://azure.microsoft.com/en-us/documentation/articles/sql-database-develop-python-simple/
# Doc:  https://msdn.microsoft.com/library/mt715796.aspx
# sp_columns documentation:  https://msdn.microsoft.com/en-us/library/ms176077.aspx

def get_table_list(conn):
  table_list=[]
  cursor = conn.cursor()
  cursor.execute('sp_tables @table_owner=dbo');
  for row in cursor:
    (table_qualifier, table_owner, table_name, table_type, remarks) = row
    table_list.append(table_name);
  return table_list

def get_col_sql_for_table(conn, table_name):
  column_sql_list=[]
  cursor = conn.cursor()
  cursor.execute("sp_columns @table_name='%s'" % table_name);
  for row in cursor:
    # print str(row[0]) + " " + str(row[1]) + " " + str(row[2])     
    (table_qualifier, table_owner, table_name, column_name, data_type, type_name,
     precision, length, scale, radix, nullable, remarks, column_def, sql_data_type,
     sql_datetime_sub, char_octet_length, ordinal_position, is_nullable, ss_data_type) = row 

    # Build column definition
    null_opt = 'NULL'
    if is_nullable == 'NO':
      null_opt = 'NOT NULL'

    autoinc = ''
    if type_name == 'int identity':
      type_name = 'int'
      autoinc = 'auto_increment'

    if type_name == 'nvarchar':  # unicode
      type_name = 'varchar'  # mysql it's a db setting not column setting
    if type_name == 'ntext':  # unicode
      type_name = 'text'  # mysql it's a db setting not column setting
    if type_name == 'image':
      type_name = 'blob'
    if type_name == 'sysname':  # sysname is functionally the same as nvarchar(128)
      type_name = 'varchar'
      length = 128
    if type_name == 'datetime2':
      type_name = 'datetime'

    if type_name == 'datetime' or type_name == 'text' or type_name == 'blob':
      # no length
      column_sql = "%s %s %s" % (column_name, type_name, null_opt)
    else:
      column_sql = "%s %s(%d) %s %s" % (column_name, type_name, length, null_opt, autoinc)
    column_sql_list.append(column_sql)
  return column_sql_list

def spit_out_csv(conn, table_name, datafile):
  cursor = conn.cursor()
  cursor.execute("select * from \"%s\"" % table_name);
  for row in cursor:
    # print row
    cols = list(row)
    firstcol = True
    for col in cols:
      if firstcol:
        firstcol = False
      else:
        datafile.write(',')
      # print "col type: " + str(type(col))
      # print "col >>>: %r" % col
      if col is not None:
        if type(col) == int:
          datafile.write(str(col))
        elif type(col) == unicode:
          datafile.write('"' + col.encode('utf8') + '"')
        elif type(col) == datetime.datetime:
          datafile.write('"' + str(col) + '"')
        elif type(col) == bool:
          if col == True:
            datafile.write('\x01')
          else:
            datafile.write('\x00')
        else:
          datafile.write('"' + str(col) + '"')
    datafile.write("\n")

conn = pymssql.connect(server='host', user='user', password='password', database='database')

for table_name in get_table_list(conn):
  file_name = "importdata/data_%s.csv" % table_name

  datafile = open(file_name, 'w')
  datafile.truncate()

  print "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % (file_name, table_name)
  print "FIELDS TERMINATED BY ','"
  print "ENCLOSED BY '\"';"

  spit_out_csv(conn, table_name, datafile)

  datafile.close()

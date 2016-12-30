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

def get_pk_sql(conn, table_name):
  cursor = conn.cursor()
  cursor.execute("sp_pkeys @table_name='%s'" % table_name);
  # ow = cursor.fetchone()
  cols = []
  for row in cursor:
    # (table_qualifier, table_owner, table_name, column_name, key_seq, pk_name) = cursor.fetchone()
    (table_qualifier, table_owner, table_name, column_name, key_seq, pk_name) = row
    cols.append(column_name)

  # if no PK defined, then we must implicitly create one if there are any autoincrement columns
  if not cols:
    cursor.execute("sp_columns @table_name='%s'" % table_name)
    for row in cursor:
      (table_qualifier, table_owner, table_name, column_name, data_type, type_name,
       precision, length, scale, radix, nullable, remarks, column_def, sql_data_type,
       sql_datetime_sub, char_octet_length, ordinal_position, is_nullable, ss_data_type) = row 

      if type_name == 'int identity':
        cols.append(column_name)

  cols_csv = ",".join(cols)
  if cols_csv:
    return "primary key (%s)" % cols_csv
  else:
    return None

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

conn = pymssql.connect(server='host', user='user', password='password', database='database')

for table_name in get_table_list(conn):
  print "\n\nCREATE TABLE %s (" % table_name

  column_sql_list = get_col_sql_for_table(conn, table_name)
  if column_sql_list:
    column_sql_all = ",\n".join(column_sql_list)
    print column_sql_all

  pk = get_pk_sql(conn, table_name)
  if pk:
    print ", " + pk

  print ");"

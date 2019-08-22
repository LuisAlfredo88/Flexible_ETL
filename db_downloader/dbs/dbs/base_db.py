#import pyodbc
import ceODBC as pyodbc
from db_downloader import settings

class BaseDb():
	connection = None
	schema = None
	db_connection_driver = ''
	downloaded_records_qty = 0
	expected_records_qty = 0

	allowed_callbacks = {
		'ERROR_LOG' : None
	}
			
	def get_connection(self):		
		connection_string = ''
		#Getting string connection. This could be ODBC or Native connection
		connection_string = 'DSN={dsn};UID={user};PWD={pwd}' if self.schema.connection.connection_type.name == 'ODBC' else 'DRIVER={{' + self.db_connection_driver + '}};SERVER={host};DATABASE={db};UID={user};PWD={pwd}'
		#Replacing variables required for connection
		connection_string = connection_string.format(**{
			'dsn': self.schema.connection.dsn, 
			'user': self.schema.connection.user, 
			'pwd': self.schema.connection.password if self.schema.connection.password else '',
			'host': self.schema.connection.host,
			'db': self.schema.name
		})

		try:
			return pyodbc.connect(connection_string)
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return None		

	def connect(self):
		self.connection = self.get_connection()
		return self.connection != None	

	def close(self):
		self.connection.close()

	#Getting all tables from current connection
	def get_tables(self, table_name = ''):
		#Creating a connection cursor
		cursor = self.connection.cursor()
		#Getting tables from the database
		cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}' {1} ORDER BY table_name ASC ".format(
			self.schema.name,
			"AND table_name ='{0}'".format(table_name) if not table_name == '' else '')
		)

		tables = cursor.fetchall()
		cursor.close()
		return [x[0] for x in tables]

	def get_table_fields(self, table_name):
		cursor = self.connection.cursor()
		#Getting the first record of the table to know the table structure
		first_row = self.query('SELECT * FROM {0}.{1} '.format(self.schema.name, table_name))
		if not first_row:
			return []

		return first_row.description

	def query(self, query_string):
		cursor = self.connection.cursor()
		
		try:
			cursor.execute(query_string)
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return None

		return cursor

	def query_with_no_log(self, query_string):
		cursor = self.connection.cursor()	
		cursor.execute(query_string)		
		return cursor

	def rename_table(self):
		pass

	def insert_from_cursor(self):
		pass

	def count_records(self, table_name, filter, download_script):
		self.expected_records_qty = 0

		schema_name = self.schema.name + '.'
		
		#In SQL Server it is not necesary to specify the schema	
		if self.schema.connection.db_type.key == 'SQLSERVER':
			schema_name = ''

		if download_script:
			query_string = 'SELECT COUNT(*) FROM '+ download_script.upper().split(' FROM ')[1]	
		else:
			query_string = 'SELECT COUNT(*) FROM ' +'{}{} {}'.format(				
				schema_name,
				table_name,
				'WHERE ' + filter if (filter and filter.strip() != '') else ''
			)

		try:
			cursor = self.connection.cursor()
			cursor.execute(query_string)
			self.expected_records_qty = int(cursor.fetchone()[0])
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)
			
		return self.expected_records_qty
	
	def set_download_error_callback(self, callback):
		self.allowed_callbacks['ERROR_LOG'] = callback


	def __get_field_name(self, field_name):
		alias_array = ['as', 'AS']
		for alias in alias_array:
			#Getting field name if the field is computes. Example CONCAT(NAME, LAST_NAME) AS FULL_NAME
			field_index = field_name.split(' {0} '.format(alias))
			if len(field_index) > 1:
				return field_index[-1].strip()

		return field_name

	def get_db_table_fields(self, table_fields):
		is_date_field = lambda x: x in settings.DB_SOPORTED_DATE
		is_float = lambda x: x in settings.DB_SOPORTED_FLOAT
		
		#Getting a single field depending of its type. For example: FLOAT(5,10) INTEGER(5) DATE DATETIME VARCHAR(5)
		construct_field = lambda x: "{0} {1}{2}{3}".format(
			self.__get_field_name(x.field_name),
			x.data_type.name,
			'' if is_date_field(x.data_type.name) else  '({0}'.format(x.length),
			',{0})'.format(x.decimal_precision) if is_float(x.data_type.name) else ('' if is_date_field(x.data_type.name) else  ')')
		)
		
		#Getting table fields
		fields = list(map(lambda f: construct_field(f), table_fields))
		#Getting and building fields index on the table
		#----------------------------------------------------
		index_list = []

		for field in table_fields:
			if field.is_indexed:
				field_name = self.__get_field_name(field.field_name)
				index_list.append('INDEX `{0}` ({1})'.format(field_name, field_name))

		#Adding index
		fields = fields + index_list 
		#----------------------------------------------------

		return fields
		
	def set_downloaded_records_qty(self, rows_qty):
		self.downloaded_records_qty += rows_qty 

	def get_downloaded_records_count(self):
		return self.downloaded_records_qty

	def get_expected_records_count(self):
		return self.expected_records_qty	
	
	def process_dynamic_filter(self, dynamic_filter):
		return ''
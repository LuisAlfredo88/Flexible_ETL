from db_downloader import settings
from .base_db import BaseDb
import psycopg2


class Postgres(BaseDb):
	def __init__(self, connection_object):
		#Getting driver instaled to control the DB
		self.connection_string = ''
		self.schema = connection_object
		self.connection_object = self.schema.connection

	def get_connection(self):
		if self.schema.connection.connection_type.name == 'ODBC':
			return super(Postgres, self).get_connection()

		try:
			return psycopg2.connect(
				user=self.connection_object.user,
	            password=self.connection_object.password if self.connection_object.password else '',
	            dbname=self.schema.name,
	            host=self.connection_object.host,
			)
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG']( 'get_connection: ' + str(e))		

			return None

	def table_exists(self, table_name):
		return len(self.get_tables(table_name)) > 0

	def drop_table(self, table_name):
		try:
			cursor = self.connection.cursor()
			cursor.execute('DROP TABLE IF EXISTS '+ table_name +';')
			self.connection.commit()
		except Exception as e:
			return False

		return True

	def create_table(self, table_name, fields):
		#Creating table
		create_statement = "CREATE TABLE {0} ({1})".format(table_name, ','.join(fields))
		print(create_statement)
		try:
			cursor = self.connection.cursor()
			cursor.execute(create_statement)
			self.connection.commit()
			return True
		except Exception as e:

			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG']('create_table: ' + str(e) + '=> ' + create_statement)			

			return False

	def insert_from_cursor(self, table_name, cursor, fetch_qty = 1000):
		#Avoid to execute this method if there is not any cursor
		if not cursor:
			return False

		total_rows = len(cursor.description)
		insert_string = 'INSERT INTO {0} VALUES({1})'.format(table_name, ",".join(["?" for x in range(0,total_rows)]))
		success = True

		#Fetching all records
		while True:

			try:
				#We create a new instance of connection to avoid memory overflow
				connection = self.get_connection()
				local_db_cursor = connection.cursor()

				rows = cursor.fetchmany(fetch_qty)
				if len(rows) == 0:
					break

				#Inserting the data
				local_db_cursor.executemany(insert_string, rows)
				connection.commit()
				connection.close()
			except Exception as e:

				if self.allowed_callbacks['ERROR_LOG']:
					self.allowed_callbacks['ERROR_LOG']('create_table: ' + str(e) + '=> ' + create_statement)	

				success = False 
				break
				

		#Closing cursor
		cursor.close()
		return success

	def rename_table(self, old_table_name, new_table_name):
		#Creating table
		try:
			cursor = self.connection.cursor()
			create_statement = "ALTER TABLE {0} RENAME TO {1};".format(old_table_name, new_table_name)
			cursor.execute(create_statement)
			self.connection.commit()
			return True
		except Exception as e:

			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG']('create_table: ' + str(e) + '=> ' + create_statement)			

			return False

	def get_table_fields_for_creating_db(self, table_set):
		is_date_field = lambda x: x in ['DATE', 'DATETIME']
		is_float = lambda x: x == 'FLOAT'
		is_char = lambda x: x == 'VARCHAR'
		
		#Getting a single field depending of its type. For example: FLOAT(5,10) INTEGER(5) DATE DATETIME VARCHAR(5)
		construct_field = lambda x: "{0} {1}{2}".format(
			self.get_real_field_name(x.field_name),
			x.data_type.name,
			'' if not is_char(x.data_type.name) else  '({0})'.format(x.length)
		)
		
		#Getting table fields
		fields = list(map(lambda f: construct_field(f), table_set))
		#Getting and building fields index on the table
		#----------------------------------------------------
		index_list = []

		for field in table_set:
			if field.is_indexed:
				field_name = self.get_real_field_name(field.field_name)
				index_list.append('INDEX `{0}` ({1})'.format(field_name, field_name))

		#Adding index
		fields = fields + index_list 
		#----------------------------------------------------

		return fields

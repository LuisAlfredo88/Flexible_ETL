from db_downloader import settings
from .base_db import BaseDb
import MySQLdb
import MySQLdb.cursors as cursors

class Maria(BaseDb):
	def __init__(self, connection_object):
		#Getting driver instaled to control the DB
		self.db_connection_driver = settings.DB_DRIVERS_FOR_CONNECTION['MYSQL']
		self.schema = connection_object
		self.connection_object = self.schema.connection

	def get_connection(self):
		if self.schema.connection.connection_type.name == 'ODBC':
			return super(Maria, self).get_connection()
			
		try:
			return MySQLdb.connect(
				user = self.connection_object.user, 
	            passwd = self.connection_object.password  if self.connection_object.password else '',
	            db=self.schema.name,
	            host = self.connection_object.host,
	            charset='utf8',
	            port = self.connection_object.port,
	            cursorclass = cursors.SSCursor
			)
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return None		

	def table_exists(self, table_name):
		return len(self.get_tables(table_name)) > 0

	def create_table(self, table_name, fields):		
		#Creating table
		try:
			cursor = self.connection.cursor()
			create_statement = "CREATE TABLE `{0}` ({1}) ENGINE = MyISAM".format(table_name, ','.join(fields))
			cursor.execute(create_statement)
			self.connection.commit()
			return True
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return False

	def insert_from_cursor(self, table_name, cursor, fetch_qty = 1000):
		#Avoid to execute this method if there is not any cursor
		if not cursor:
			return False

		total_rows = len(cursor.description)
		insert_string = 'INSERT INTO {0} VALUES({1})'.format(table_name, ",".join(["%s" for x in range(0,total_rows)]))

		success = True
		#Fetching all records
		while True:
			
			try:
				#We create a new instance of connection to avoid memory overflow

				connection = self.get_connection()
				local_db_cursor = connection.cursor()
				rows = cursor.fetchmany(fetch_qty)
				rows_qty = len(rows)

				if rows_qty == 0:
					break
		
				local_db_cursor.executemany(insert_string, rows)
				connection.commit()				
				local_db_cursor.close()
				connection.close()

				self.set_downloaded_records_qty(rows_qty)

			except Exception as e:
				if self.allowed_callbacks['ERROR_LOG']:
					self.allowed_callbacks['ERROR_LOG'](e)

				success = False
				break

		#Closing cursor
		cursor.close()
		return success

	def rename_table(self, old_table_name, new_table_name):
		#Creating table
		try:
			cursor = self.connection.cursor()
			create_statement = "RENAME TABLE {0} TO {1};".format(old_table_name, new_table_name)
			cursor.execute(create_statement)
			self.connection.commit()
			return True
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return False		

	def drop_table(self, table_name):
		#Droping table
		try:
			connection = self.get_connection()
			cursor = connection.cursor()
			create_statement = "DROP TABLE IF EXISTS {0};".format(table_name)
			cursor.execute(create_statement)
			connection.commit()
			connection.close()
			return True
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return False		
	
	def insert_temporary_data(self, temporary_table, table_name):

		try:
			connection = self.get_connection()
			cursor = connection.cursor()
			cursor = self.query("INSERT INTO {} SELECT * FROM {};".format(table_name, temporary_table))
			connection.commit()
			connection.close()
			return True
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return False

	def process_dynamic_filter(self, dynamic_filter):
		try:
			connection = self.get_connection()
			cursor = connection.cursor()
			cursor = self.query(dynamic_filter)
			connection.close()
			return cursor.fetchone()[0]
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)

			return ''
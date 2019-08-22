from db_downloader import settings
from .base_db import BaseDb

class SqlServer(BaseDb):
	def __init__(self, connection_object):
		self.db_connection_driver = settings.DB_DRIVERS_FOR_CONNECTION['SQLSERVER']
		self.schema = connection_object
		self.connection_object = self.schema.connection

	#Getting all tables from current connection
	def get_tables(self, table_name = ''):
		#Creating a connection cursor
		cursor = self.connection.cursor()
		#Getting tables from the database
		sql = """
             select name
				from sys.all_objects {0}	
		"""
		cursor.execute(sql.format("WHERE name ='{0}'".format(table_name) if not table_name == '' else ''))
		tables = cursor.fetchall()
		cursor.close()
		return [x[0] for x in tables]

	def get_table_fields(self, table_name):
		cursor = self.connection.cursor()
		#Getting the first record of the table to know the table structure
		first_row = self.query('SELECT TOP 1 * FROM {0} '.format(table_name))
		if not first_row:
			return []

		return first_row.description

from db_downloader import settings
from .base_db import BaseDb
import cx_Oracle

class Oracle(BaseDb):
	def __init__(self, connection_object):
		#Getting driver instaled to control the DB
		self.connection_string = settings.DB_DRIVERS_FOR_CONNECTION['ORACLE']
		self.schema = connection_object
		self.connection_object = self.schema.connection

	def get_connection(self):		
		if self.schema.connection.connection_type.name == 'ODBC':
			return super(Oracle, self).get_connection()

		try:
			return cx_Oracle.connect(
				#dsn=self.connection_object.host, 
				dsn=self.connection_object.dsn, 
				user=self.connection_object.user,
	            password=self.connection_object.password if self.connection_object.password else '',
	            #dbname=self.connection_object.db
	            encoding = 'UTF-8', 
	            nencoding = 'UTF-8'
			)
		except Exception as e:
			if self.allowed_callbacks['ERROR_LOG']:
				self.allowed_callbacks['ERROR_LOG'](e)
			
			return None

	#Getting all tables from current connection
	def get_tables(self, table_name = ''):
		#Creating a connection cursor
		cursor = self.connection.cursor()
		#Getting tables from the database
		cursor.execute("SELECT DISTINCT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = '{0}' {1} ORDER BY OBJECT_NAME ASC ".format(
			self.schema.name,
			"AND OBJECT_NAME ='{0}'".format(table_name) if not table_name == '' else '')
		)

		tables = cursor.fetchall()
		cursor.close()
		return [x[0] for x in tables]
#Importing database managers
from .dbs.oracle import Oracle
from .dbs.mysql import Mysql
from .dbs.sqlserver import SqlServer
from .dbs.db2 import Db2
from .dbs.maria import Maria
from .dbs.postgres import Postgres

class DbFactory():
	def get_db(self, connection_object):
		return {
			'ORACLE': Oracle, 
			'SQLSERVER': SqlServer, 
			'DB2': Db2, 
			'MariaDb': Maria,
			'PostgreSQL':Postgres
		}.get(connection_object.connection.db_type.key,  Mysql)(connection_object)
		
class Db():
	connection = None
	
	def __init__(self, connection_object):
		factory = DbFactory()
		self.connection = factory.get_db(connection_object)

	def get(self, callbacks = None):
		#Getting initialized table
		#from ipdb import set_trace; set_trace()
		if callbacks and callbacks['ERROR_LOG']:
			self.connection.set_download_error_callback(callbacks['ERROR_LOG'])

		if self.connection.connect():
			return self.connection
		else:
			return None
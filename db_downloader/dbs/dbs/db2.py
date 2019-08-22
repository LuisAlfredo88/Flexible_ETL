from db_downloader import settings
from .base_db import BaseDb

class Db2(BaseDb):
	def __init__(self, connection_object):
		self.connection_string = ''
		self.schema = connection_object
		self.connection_object = self.schema.connection

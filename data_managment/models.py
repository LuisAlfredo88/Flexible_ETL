from django.db import models
from db_downloader import settings
from db_downloader.utility import Db, get_data_type
from datetime import datetime
from threading import Thread, BoundedSemaphore

NUMBER_OF_PARALLEL_DOWNLOADS = settings.NUMBER_OF_PARALLEL_DOWNLOADS
threadLimiter = BoundedSemaphore(NUMBER_OF_PARALLEL_DOWNLOADS)

class BaseModel(models.Model):

	class Meta(object):
		abstract = True

class ConnectionType(BaseModel):
	name = models.CharField(max_length=50, null=False)

	def __str__(self):
		return (f'{self.name}')

class DbType(BaseModel):
	name = models.CharField(max_length=50, null=False)
	key = models.CharField(max_length=50, null=False)

	def __str__(self):
		return (f'{self.name}')

class Connection(BaseModel):
	name = models.CharField(max_length=50, null=False)
	host = models.CharField(max_length=20, null=True)
	dsn = models.CharField(max_length=100, null=True)
	user = models.CharField(max_length=100, null=False)
	password = models.CharField(max_length=300, null=True)
	db_type = models.ForeignKey(DbType, on_delete=models.PROTECT, db_index=True)
	connection_type = models.ForeignKey(ConnectionType, on_delete=models.PROTECT, db_index=True)
	is_active = models.BooleanField(default=True)
	port = models.IntegerField(null=True)

	@staticmethod
	def get_all():
		connections_list = Connection.objects.all()
		connections = [
			{
				'id': con.id,
				'name': con.name,
				'schemas': [{'id': x.id, 'name': x.name} for x in con.dbschema_set.all()]
			} 
			for con in connections_list
		]

		return connections

	def __str__(self):
		return (f'{self.name}')

class DbSchema(BaseModel):
	name = models.CharField(max_length=50, null=False)
	connection = models.ForeignKey(Connection, on_delete=models.PROTECT)

	def query(self, query_string):
		connection = Db(self).get()

		cursor = connection.query_with_no_log(query_string)
		return {'records': cursor.fetchmany(100), 'columns': cursor.description, 'error': False}
	
	def get_table_fields(self, table_name):
		fields = []
		cursor_description = []
		connection = Db(self).get()

		#If there is not connection, return empty array
		if connection:
			cursor_description = connection.get_table_fields(table_name)

		#Getting field information from cursor
		fields_constructor = lambda f : {
			'name': f[0], 
			'data_type': get_data_type(str(f[1])),
			'length': f[4] if f[4] else f[2],
			'precision': f[5]
		} 

		return [fields_constructor(field) for field in cursor_description]		

	def get_tables(self):
		tables = []
		connection = Db(self).get()

		if connection:
			tables = connection.get_tables()

		return tables

	def __str__(self):
		return (f'{self.name}')

class DataTypes(BaseModel):
	name = models.CharField(max_length=50, null=False)

	@staticmethod
	def get_all():
		#Getting all data types
		return [{'id': t.id, 'name': t.name} for t in DataTypes.objects.all()]

	def __str__(self):
		return (f'{self.name}')

class Table(BaseModel):
	name = models.CharField(max_length=100, null=False)
	description = models.TextField(max_length=400000, null=False)
	download_name = models.CharField(max_length=100, null=False)
	schedule = models.TextField(max_length=400000, null=False)
	is_updating = models.BooleanField(default=False)
	pending_to_download = models.BooleanField(default=False)
	is_active = models.BooleanField(default=True)
	allow_append = models.BooleanField(default=True)
	last_update_date = models.DateTimeField(null=True)
	last_access_date = models.DateTimeField(null=True)
	next_update_date = models.DateField(null=True)
	download_filter  = models.TextField(max_length=2000)
	dynamic_download_filter = models.TextField(max_length=2000)
	db_schema = models.ForeignKey(DbSchema, on_delete=models.PROTECT)
	download_order = models.IntegerField()
	download_script = models.TextField(max_length=400000, null=False)
	start_datetime = None
	local_connection = None
	table_connection = None

	def __get_temporary_table_name(self):
		return '_tmp_{}'.format(self.download_name)

	def __setup_download_start(self):		
		self.start_datetime = datetime.now()
		self.is_updating = 1
		self.pending_to_download = 0
		self.save()

	def __prepare_data(self):
		table_exists = self.local_connection.table_exists(self.download_name)
		temporary_table_name = self.__get_temporary_table_name()

		# Updating table records
		if self.allow_append and table_exists:			
			data_was_inserted = self.local_connection.insert_temporary_data(temporary_table_name, self.download_name)

			if data_was_inserted:
				delete_temporary_table_name = 'DELETE_' + temporary_table_name
				self.local_connection.rename_table(temporary_table_name, delete_temporary_table_name)
				self.local_connection.drop_table(delete_temporary_table_name)
		else: 
			if table_exists:
				self.local_connection.rename_table(self.download_name, 'DELETE_' + self.download_name)
			
			self.local_connection.rename_table(temporary_table_name, self.download_name)
			self.local_connection.drop_table('DELETE_' + self.download_name)
	
	def __finish_download(self):
		downloaded_records_count = self.local_connection.get_downloaded_records_count()
		expected_records_count = self.table_connection.get_expected_records_count()

		# Comparing expected records to download vs downloaded records
		if downloaded_records_count >= expected_records_count:
			self.__prepare_data()
		else:
			self.__set_download_error("__finish_download: La cantidad de registros descargados es inferior a lo esperado. Esperado: {}, Descargado: {}".format(
				expected_records_count,
				downloaded_records_count
			))

		self.last_update_date = datetime.now()
		self.is_updating = 0
		self.pending_to_download = 0
		self.save()

		# Saving success download
		TableDownloadLog.set_log(self, True, self.start_datetime)

	def __clean_before_download(self):
		success = True
		if not self.allow_append:
			success = self.local_connection.drop_table(self.__get_temporary_table_name())
			if not success:
				self.__set_download_error("__clean_before_download: Ocurrió un error al tratar de eliminar la tabla temporal al iniciar el proceso de descarga")

		return success

	def __clean_after_download(self):
		self.local_connection.drop_table()
	
	def __set_download_error(self, error_description):
		DbErrorLog.set_error(error_description, self)
		TableDownloadLog.set_log(self, False)

		self.is_updating = 0
		self.pending_to_download = 0
		self.save()

	def __create_temporary_table(self):
		success = False
		# Avoid to create table if there is no fields asociated
		if len(self.tablefields_set.all()) == 0:
			self.__set_download_error("__create_temporary_table: No hay campos asociados a esta tabla")
			return False

		temporary_table_name = self.__get_temporary_table_name()
		table_fields = self.local_connection.get_db_table_fields(self.tablefields_set.all())

		if self.local_connection.drop_table(temporary_table_name):		
			success =  self.local_connection.create_table(temporary_table_name, table_fields)

		if not success:
			self.__set_download_error("__create_temporary_table: Ocurrió un error al crear la tabla")
		
		return success
	
	def __download_information(self):
		condition = ''
		if self.dynamic_download_filter:
			condition = self.self.local_connection.process_dynamic_filter(dynamic_download_filter)
			if not condition:
				return False
		else:
			if self.download_filter:
				condition = self.download_filter		

		#Building query to download the table data
		query_string = "SELECT {fields} FROM {schema}{table} {condition}".format(**{
			'fields': ','.join([x.field_name for x in self.tablefields_set.all()]),
			'schema': ''  if self.db_schema.connection.db_type.key == 'SQLSERVER' else self.db_schema.name + '.',
			'table': self.name,
			'condition': '' if self.download_filter == '' else 'WHERE ' + self.download_filter
		}) if (not self.download_script or self.download_script.strip() == '') else self.download_script	

		#Number of row downloaded for everty iteration
		fetch_rows_qty = settings.MAXIMUM_FECTH_DOWNLOAD
		#Getting data from the server and inserting on local db

		cursor_data = self.table_connection.query(query_string)		
		#Inserting data into local db
		return self.local_connection.insert_from_cursor(self.__get_temporary_table_name(), cursor_data, fetch_rows_qty)
	
	def controled_download(self):
		#Mark as pending
		table = Table.objects.get(id = self.id)
		table.pending_to_download = 1
		table.save()

		def _run_download():
			with threadLimiter:		
				self.download()
		
        # create thread
		thread = Thread(target=_run_download, args=())
		thread.start()		

	def download(self):
		# Avoid to update table more than once
		if(self.is_updating or self.pending_to_download): return False

		try:
			# Defining callbacks
			callbacks = {
				'ERROR_LOG' : self.__set_download_error
			}

			self.local_connection = Db(DbSchema.objects.get(name="AUDIT_DATA_MINING")).get(callbacks)
			self.table_connection = Db(self.db_schema).get(callbacks)
		except Exception as e:
			self.__set_download_error('download: ' + str(e))
			return False

		if not self.local_connection or not self.table_connection:
			connection_type = 'Local' if not self.local_connection else 'Servidor'
			self.__set_download_error('download: Error iniciando la conexión a la base de datos: ' + connection_type)
			return False
		
		self.__setup_download_start()

		# Getting the number of records pending to download
		self.table_connection.count_records(self.name, self.download_filter, self.download_script)
		
		# Cleaning DB
		if not self.__clean_before_download(): return False
		# Creating temporary table to download data
		if not self.__create_temporary_table(): return False		
		# Downloading information
		if not self.__download_information(): return False		
		
		self.__finish_download()

		return True

	@staticmethod
	def get_all():
		#Getting saved tables
		get_table_data = lambda t : {
			'id': t.id,
			'name': t.name,
			'download_name': t.download_name,
			'is_updating': t.is_updating,
			'last_update_date': t.last_update_date,
			'connection_name': t.db_schema.connection.name,
			'description': t.description,
			'schema_id': t.db_schema.id,
			'download_filter': t.download_filter,
			'is_active': t.is_active,
			'download_script': t.download_script,
			'dynamic_download_filter': t.dynamic_download_filter,
			'allow_append': t.allow_append,
			'pending_to_download': t.pending_to_download
		} 

		return [get_table_data(t) for t in Table.objects.all()]


	def __str__(self):
		return (f'{self.name}')

class TableFields(BaseModel):
	field_name = models.CharField(max_length=600, null=False)
	field_description = models.TextField(max_length=300, null=False)
	decimal_precision = models.IntegerField()
	length = models.IntegerField()
	table = models.ForeignKey(Table, on_delete=models.PROTECT)
	data_type = models.ForeignKey(DataTypes, on_delete=models.PROTECT)
	is_indexed = models.BooleanField(default=False)

	def __str__(self):
		return (f'{self.name}: field {self.field_name}')

class TableDownloadLog(BaseModel):
	table = models.ForeignKey(Table, on_delete=models.PROTECT)
	start_date = models.DateTimeField()
	end_date = models.DateTimeField()
	success = models.BooleanField(default=False)

	def __str__(self):
		return (f'{self.name}: {self.success}')

	@staticmethod
	def set_log(table, success, start_date = None):		
		download_log = TableDownloadLog()
		download_log.start_date = datetime.now() if not start_date else start_date
		download_log.end_date = datetime.now()
		download_log.success = success
		download_log.table = table
		download_log.save()

class DbErrorLog(BaseModel):
	date  = models.DateTimeField()
	error_description = models.TextField(max_length=5000, null=False)
	table = models.ForeignKey(Table, on_delete=models.PROTECT, null=True)

	def __str__(self):
		return self.error_description

	@staticmethod
	def set_error(error_description, table):
		db_error = DbErrorLog()
		db_error.date = datetime.now()
		db_error.error_description = error_description 
		db_error.table = table
		db_error.save()

class DownloadSchedule(BaseModel):
	class Meta:
		verbose_name = 'Calendario de Descarga'
		verbose_name_plural = 'Calendarios de Descargas'

	title = models.TextField(max_length=100, null=False)
	description = models.TextField(max_length=250, null=False)
	schedule = models.TextField(max_length=400000, null=False)
	is_active = models.BooleanField(default=True)
	next_run_date = models.DateTimeField(null=True)
	last_run_date = models.DateTimeField(null=True)

	def __str__(self):
		return self.title

class ScheduledTables(BaseModel):
	class Meta:
		verbose_name = 'Tabla Candelarizada'
		verbose_name_plural = 'Tablas Calendarizadas'

	table = models.ForeignKey(Table, on_delete=models.PROTECT,
		related_name = 'table_schedules'
	)
	schedule = models.ForeignKey(DownloadSchedule, on_delete=models.PROTECT,
		related_name = 'schedule_tables'
	)

	def __str__(self):
		return (f'{self.schedule.title}-{self.name}')


class DbQueryLog(BaseModel):
	date  = models.DateTimeField()
	query = models.TextField(max_length=400000, null=False)
	user =  models.TextField(max_length=50, null=False)

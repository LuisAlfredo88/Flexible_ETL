from django.shortcuts import render
from http import HTTPStatus
from db_downloader import settings
from db_downloader.extension import JsonResponse, BadRequest
from django.views.decorators.http import require_http_methods
from json import loads as json_decode
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime

from data_managment.models import Table, DbSchema, Connection, DataTypes, TableFields, DbQueryLog

# Create your views here.
@csrf_exempt
def download_scheduled_tables(request, token):
	#Avoid to update tables if it is a wrong token
	if not token == settings.DOWNLOAD_FIXED_TOKEN:
		return BadRequest()	

	tables = Table.objects.filter(	
		is_active = 1, 
		is_updating = 0, 
		pending_to_download = 0,
		db_schema__connection__is_active = 1
	).order_by('download_order').all()

	for table in tables:
		table.controled_download()

	return JsonResponse({'success': True, 'msg': 'Cantidad de tablas a descargar: {} '.format(len(tables))}, safe=False, status=HTTPStatus.OK)

@require_http_methods(["POST"])
@csrf_exempt
def get_table_fields(request):
	form = json_decode(request.body.decode("utf-8"))
	table_name = form['table_name']
	schema_id = form['schema_id']

	schema = DbSchema.objects.get(id = schema_id)
	#Getting table fields from schema and table name
	fields = schema.get_table_fields(table_name)
	return JsonResponse({'fields': fields})

@require_http_methods(["POST"])
@csrf_exempt
def add_or_update_field(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	form = json_decode(request.body.decode("utf-8"))
	table_field = TableFields.objects.filter(id = form['id']).first()

	if not table_field:
		table_field = TableFields()

	table_field.field_name = form['field']
	table_field.field_description = form['description']
	table_field.decimal_precision = form['decimals']
	table_field.length = form['length']
	table_field.is_indexed = form['is_indexed']
	table_field.data_type_id = form['data_type']
	table_field.table_id = form['table_id']

	# Saving table field
	success = table_field.save()

	return JsonResponse({'success': success, 'message': ('Datos guardados satisfactoriamente' if success else 'Error al intentar guardar la información')})

@require_http_methods(["POST"])
@csrf_exempt
def get_saved_table_fields(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	form = json_decode(request.body.decode("utf-8"))
	table_id = form['table_id']
	#Getting saved table fields from table
	get_table_fields = lambda f : {
		'id': f.id,
		'name': f.field_name,
		'description': f.field_description,
		'decimal_precision': f.decimal_precision,
		'length': f.length,
		'is_indexed': f.is_indexed,
		'data_type': f.data_type.name
	} 

	fields = [get_table_fields(f) for f in Table.objects.get(id = table_id).tablefields_set.all()]	

	return JsonResponse({'fields': fields})

@csrf_exempt
@require_http_methods(["GET"])
def get_tables_from_schema(request, schema_id):
	#Getting tables from connection	

	tables = DbSchema.objects.get(id = schema_id).get_tables()
	return JsonResponse({'tables': tables})

@csrf_exempt
def get_saved_tables(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	return JsonResponse({'tables': Table.get_all()})

@csrf_exempt
def get_data_types(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	return JsonResponse({'data_types': DataTypes.get_all()})

@csrf_exempt
def get_connections(request):
	#Getting saved tables
	return JsonResponse({'connections': Connection.get_all()})

@require_http_methods(["POST"])
@csrf_exempt
def query(request):
	information = []
	column_list = []
	form = json_decode(request.body.decode("utf-8"))
	schema_id = form['schema']	
	query_string = form['query']	
	user = form['user']	

	log = DbQueryLog(
		date = datetime.now(),
		query = query_string,
		user = user
	)

	log.save()

	#Getting saved tables
	schema  = DbSchema.objects.get(id=schema_id)
	query_result = schema.query(query_string)



	if(query_result['error']):
		return JsonResponse({'success': False, 'error': query_result['error_msg']})

	for column in query_result['columns']:
		column_list.append({'title': column[0]})

	for record in query_result['records']:
		data = []
		for record_data in record:
			data.append(str(record_data))

		information.append(data)

	return JsonResponse({"table_data": information, 'columns': column_list, 'success': True})

@require_http_methods(["POST"])
@csrf_exempt
def update_tables(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	table_names = []
	form = json_decode(request.body.decode("utf-8"))

	tables = Table.objects.filter(
		id__in = form['tables_ids'], 
		is_updating = 0,
		pending_to_download = 0
	).all()

	for table in tables:
		table.controled_download()
		table_names.append(table.name)
	
	return JsonResponse({"downloaded_tables": table_names, 'success': True})

@require_http_methods(["POST"])
@csrf_exempt
def add_or_update_table(request):
	if not request.user.is_authenticated:
		return BadRequest()	

	form = json_decode(request.body.decode("utf-8"))
	table = Table.objects.filter(id = form['id']).first()

	if not table:
		table = Table()

	table.name = form['name']
	table.description = form['description']
	table.download_name = form['download_name']
	table.schedule = form['schedule']
	table.is_active = int(form['is_active'])
	table.download_filter = form['download_filter']
	table.db_schema_id = int(form['schema_id'])
	table.download_script = form['download_script']
	table.dynamic_download_filter = form['dynamic_download_script']
	table.allow_append = int(form['allow_append'])
	table.download_order = 0

	#Saving table data
	success = table.save()

	return JsonResponse({'success': success, 'message': ('Datos guardados satisfactoriamente' if success else 'Error al intentar guardar la información')})



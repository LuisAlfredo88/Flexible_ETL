from django.urls import path, re_path
from . import views

urlpatterns = [
	re_path(r'download_scheduled_tables/(?P<token>[\w\-]+)/?$', views.download_scheduled_tables, name='download_scheduled_tables'),
	re_path('get_saved_tables', views.get_saved_tables, name='get_saved_tables'),
	re_path('get_table_fields', views.get_table_fields, name='get_table_fields'),
	re_path('get_data_types', views.get_data_types, name='get_data_types'),
	re_path('add_or_update_field', views.add_or_update_field, name='add_or_update_field'),
	re_path('get_saved_table_fields', views.get_saved_table_fields, name='get_saved_table_fields'),
	re_path('get_connections', views.get_connections, name='get_connections'),
	re_path('query', views.query, name='query'),
	re_path('update_tables', views.update_tables, name='update_tables'),
	re_path('add_or_update_table', views.add_or_update_table, name='add_or_update_table'),
	re_path(r'^get_tables_from_schema/(?P<schema_id>\d+)/?$', views.get_tables_from_schema, name='get_tables_from_schema'),
]

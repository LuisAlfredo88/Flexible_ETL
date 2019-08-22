from datetime import date
from django.contrib.auth.models import User
from django.core.management.base import BaseCommand, CommandError
from data_managment.models import Table
from db_downloader import settings
from datetime import datetime

class Command(BaseCommand):
    help = 'Execute all tasks for today'

    def add_arguments(self, parser):
        parser.add_argument('--only-schemas', type=str)
        parser.add_argument('--exclude-schemas', type=str)

    def download_all(self, tables):
        for table in tables:
            table.controled_download()  

    def download_exclude(self, exclude_schemas: []):

        tables = Table.objects.filter(	
            is_active = 1, 
            is_updating = 0, 
            pending_to_download = 0,
            db_schema__connection__is_active = 1
        ).extra(where=["db_schema_id NOT IN ({})".format(','.join(exclude_schemas))])

        self.download_all(tables)

    def download_only(self, only_schemas: []):

        tables = Table.objects.filter(	
            is_active = 1, 
            is_updating = 0, 
            pending_to_download = 0,
            db_schema__connection__is_active = 1,
            db_schema_id__in = only_schemas
        ).order_by('download_order').all()

        self.download_all(tables)

    def handle(self, *args, **options):

        if datetime.now().day in settings.NOT_RUN_ON_DAYS:
            print('Este día ha sido excluido de la descarga automática')
            return

        if options['only_schemas']:
            self.download_only(options['only_schemas'].split(','))
        
        if options['exclude_schemas']:
            self.download_exclude(options['exclude_schemas'].split(','))

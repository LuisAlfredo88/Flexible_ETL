from django.contrib import admin
from .models import *

admin.site.register(ConnectionType)
admin.site.register(DbType)
admin.site.register(Connection)
admin.site.register(DbSchema)
admin.site.register(DataTypes)
admin.site.register(Table)
admin.site.register(TableFields)
admin.site.register(TableDownloadLog)
admin.site.register(DbErrorLog)
admin.site.register(DownloadSchedule)
admin.site.register(ScheduledTables)

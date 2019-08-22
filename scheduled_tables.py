#!F:\INSTALLATIONS\db_downloader\.env\Scripts\python.exe
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "db_downloader.settings")
import django
django.setup()

from data_managment.db_download_managment.download_manager import DbDownloadManager


def download_scheduled_tables():
    db_manager = DbDownloadManager()
    db_manager.download_scheduled_tables()

download_scheduled_tables()
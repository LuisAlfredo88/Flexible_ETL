DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'HOST': 'localhost',
        'USER' : 'root',
        'PASSWORD' : 'toor',
        'NAME' : 'db_downloader_prod',
        'PORT': '3307'
    }
}

NUMBER_OF_PARALLEL_DOWNLOADS = 10
MAXIMUM_FECTH_DOWNLOAD = 10000
DOWNLOAD_FIXED_TOKEN = '3iWt0nB4Xo01KS88Cfjqg2V31XUPRxWH'

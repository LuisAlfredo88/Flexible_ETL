DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'HOST': 'localhost',
        'USER' : 'root',
        'NAME' : 'db_downloader_prod',
        'PORT': '3306'
    }
}

NUMBER_OF_PARALLEL_DOWNLOADS = 10
MAXIMUM_FECTH_DOWNLOAD = 10000
DOWNLOAD_FIXED_TOKEN = '3iWt0nB4Xo01KS88Cfjqg2V31XUPRxWH'


CORS_ORIGIN_WHITELIST = (
    's460-aud04:8000'
)
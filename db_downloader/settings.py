from .env_settings.defaults import *

DJANGO_ENVIRONMENT = os.environ.get('DJANGO_ENVIRONMENT')

if DJANGO_ENVIRONMENT == "develop":
    from .env_settings.develop import *

elif DJANGO_ENVIRONMENT == "production":
    from .env_settings.production import *

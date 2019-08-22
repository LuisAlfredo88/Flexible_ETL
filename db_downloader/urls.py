"""db_downloader URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from data_managment import views as dt_m
from django.views.generic import TemplateView
from django.contrib.auth.decorators import login_required
from audcont_dev.modules.django_auth import django_admin, authenticate

urlpatterns = [
    path('admin/', django_admin()),
  #  path('admin/', admin.site.urls),
    path('auth_login/<str:username>/<str:login_hash>/', authenticate),
    #Data magment urls
    path(r'data_managment/', include('data_managment.urls'), name='data_managment'),
    path('', login_required(TemplateView.as_view(template_name='index.html'), login_url="/admin"), name='index'),
]

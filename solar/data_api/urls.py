# data_api/urls.py
from django.urls import path
from .views import *

urlpatterns = [
    path('active-power-min/', active_power_api, name='active_power_api'),
    path('dc-power-min/', dc_power_api, name='dc_power_api'),
    path('todays-gen-min/', todays_gen_api, name='todays_gen_api'),
    path('active-power-hourly/', active_power_hourly_api, name='active_power_hourly_api'),
    path('dc-power-hourly/', dc_power_hourly_api, name='dc_power_hourly_api'),
    path('todays-gen-hourly/', todays_gen_hourly_api, name='todays_gen_hourly_api'),
]

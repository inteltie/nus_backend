# data_api/urls.py
from django.urls import path
from .views import active_power_api, dc_power_api, todays_gen_api

urlpatterns = [
    path('active-power-min/', active_power_api, name='active_power_api'),
    path('dc-power-min/', dc_power_api, name='dc_power_api'),
    path('todays-gen-min/', todays_gen_api, name='todays_gen_api'),
]

from django.urls import path
from .views import *

urlpatterns = [
    path('data/', generalized_data_api, name='generalized_data_api'),
    # path('current-active-power/', current_active_power_api, name='current_active_power_api'),
    # path('current-dc-power/', current_dc_power_api, name='current_dc_power_api'),
    # path('current-todays-gen/', current_todays_gen_api, name='current_todays_gen_api'),
    path('settings/', settings_page, name='settings_page'),
    path('save-settings/', save_settings, name='save_settings'),
]

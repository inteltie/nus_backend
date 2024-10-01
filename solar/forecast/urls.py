from django.urls import path
from .views import upload_and_predict, download_forecast, compare_power_output, download_sample

urlpatterns = [
    path('upload/', upload_and_predict, name='upload_and_predict'),
    path('download/', download_forecast, name='download_forecast'),
    path('download-sample/', download_sample, name='download_sample'),
    path('compare-power-output/', compare_power_output, name='compare_power_output'),
]

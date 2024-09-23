from django.urls import path
from .views import upload_and_predict, download_forecast

urlpatterns = [
    path('upload/', upload_and_predict, name='upload_and_predict'),
    path('download/', download_forecast, name='download_forecast'),
]

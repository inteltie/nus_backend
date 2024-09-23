from django.urls import path
from .views import *

urlpatterns = [
    path('derived_data/', derived_data, name='derived_data'),
]
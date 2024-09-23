from django.apps import AppConfig


class DerivedConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'derived'

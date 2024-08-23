from django.contrib import admin
from .models import CountryStats


# Register your models here.
@admin.register(CountryStats)
class CountryStatsAdmin(admin.ModelAdmin):
    list_display = (
    'country', 'most_sold_product', 'most_sold_price', 'least_sold_product', 'least_sold_price', 'total_sold')
    readonly_fields = (
    'country', 'most_sold_product', 'most_sold_price', 'least_sold_product', 'least_sold_price', 'total_sold')

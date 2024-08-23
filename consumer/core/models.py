from django.db import models


# Create your models here.
class CountryStats(models.Model):
    country = models.CharField(max_length=100)
    most_sold_product = models.CharField(max_length=255)
    most_sold_price = models.DecimalField(max_digits=10, decimal_places=2)
    least_sold_product = models.CharField(max_length=255)
    least_sold_price = models.DecimalField(max_digits=10, decimal_places=2)
    total_sold = models.IntegerField()

    class Meta:
        verbose_name_plural = 'Country Statistics'
        verbose_name = 'Country Statistics'

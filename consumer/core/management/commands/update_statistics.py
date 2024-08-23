from django.core.management.base import BaseCommand, CommandError
from core.models import CountryStats
from kafka import KafkaConsumer
import pdb
import json
import threading


def update_statistics(country: str) -> None:
    consumer = KafkaConsumer(country, group_id=None, bootstrap_servers='kafka:9092')
    print(f"[consumer] {country}\n")
    for msg in consumer:
        data = json.loads(msg.value)
        print(data)
        CountryStats.objects.create(
            country=country,
            most_sold_product=data['max'][0],
            most_sold_price=data['max'][1],
            least_sold_product=data['min'][0],
            least_sold_price=data['min'][1],
            total_sold=data['total_sold']
        )
        print("******************************************************")
        print("Created new statistics for: {}".format(country))


class Command(BaseCommand):
    help = "Creates Elasticsearch products index"

    def handle(self, *args, **options):
        try:
            united_kingdom = threading.Thread(target=update_statistics, args=('united_kingdom',))
            canada = threading.Thread(target=update_statistics, args=('canada',))
            germany = threading.Thread(target=update_statistics, args=('germany',))
            poland = threading.Thread(target=update_statistics, args=('poland',))
            brazil = threading.Thread(target=update_statistics, args=('brazil',))
            saudi_arabia = threading.Thread(target=update_statistics, args=('saudi_arabia',))
            spain = threading.Thread(target=update_statistics, args=('spain',))

            united_kingdom.start()
            canada.start()
            germany.start()
            poland.start()
            brazil.start()
            saudi_arabia.start()
            spain.start()

            united_kingdom.join()
            canada.join()
            germany.join()
            poland.join()
            brazil.join()
            saudi_arabia.join()
            spain.join()

        except Exception as e:
            raise CommandError(e)

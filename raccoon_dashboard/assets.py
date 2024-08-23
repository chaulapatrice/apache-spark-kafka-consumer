import sys

from dagster import asset, Failure
import os
from pathlib import Path
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import statistics

assets_path = Path(os.path.abspath(__file__))
# Go back 2 levels up to the base directory
base_path = assets_path.parent.parent


def create_pair(order: tuple[str, int, float]) -> tuple[str, float]:
    return (order[0], order[1] * order[2])


# 1. Get the most selling product
# 2. Get the least selling product
# 3. Get the number of products sold
def max_func(record: tuple[str, float]) -> tuple[str, float]:
    return max(record, key=lambda x: x[1])


def min_func(record: tuple[str, float]) -> tuple[str, float]:
    return min(record, key=lambda x: x[1])


def analyze_orders(country: str) -> str:
    try:
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"data/{country}.csv")
        rdd = df.select("Description", "Quantity", "UnitPrice") \
            .rdd \
            .map(lambda r: create_pair(r)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: x[1])

        values = rdd.collect()

        message = json.dumps({
            "min": values[0],
            "max": values[len(values) - 1],
            "total_sold": len(values),
        }).encode('utf-8')

        producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
        producer.send(country, message).get(timeout=10)
        producer.flush(timeout=10)
        producer.close()
    except Exception as e:
        raise Failure(str(e))


@asset
def analyze_united_kingdom_orders(context):
    analyze_orders('united_kingdom')

    return True


@asset
def analyze_canada_orders(analyze_united_kingdom_orders):
    analyze_orders('canada')
    return True


@asset
def analyze_brazil_orders(analyze_canada_orders):
    analyze_orders('brazil')
    return True


@asset
def analyze_saudi_arabia_orders(analyze_brazil_orders):
    analyze_orders('saudi_arabia')
    return True


@asset
def analyze_spain_orders(analyze_saudi_arabia_orders):
    analyze_orders('spain')
    return True


@asset
def analyze_germany_orders(analyze_spain_orders):
    analyze_orders('germany')
    return True


@asset
def analyze_poland_orders(analyze_germany_orders):
    analyze_orders('poland')
    return True

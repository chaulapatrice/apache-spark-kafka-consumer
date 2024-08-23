import dlt
from dlt.sources.helpers import requests
import pdb
import os
from pathlib import Path
import sys
import traceback


def remove_duplicates_from_table(client, table_name):
    duplicates = client.execute_sql(
        f"""
            SELECT
                "id",
                COUNT("id") AS "total_duplicates"
            FROM
                "upcoming_launches"."{table_name}"
            GROUP BY
                "id"
            HAVING
                COUNT("id") > 1
        """
    )

    print("Duplicates: ", duplicates)

    for duplicate in duplicates:
        row_id, _ = duplicate
        # Find the first row to keep
        rows = client.execute_sql(
            f"""
                SELECT
                    _dlt_id
                FROM
                    "upcoming_launches"."{table_name}"
                WHERE
                    "id" = { row_id }
                LIMIT
                    1
            """
        )

        # Delete the rest of the rows
        _dlt_id, = rows[0]

        client.execute_sql(
            f"""
                DELETE FROM
                    "upcoming_launches"."{table_name}"
                WHERE
                    id = { row_id }
                    AND _dlt_id != \'{_dlt_id}\'
            """
        )


def remove_duplicates(pipeline: dlt.Pipeline):
    with pipeline.sql_client() as client:
        client.execute_sql(
            f"SET search_path = '{pipeline.dataset_name}'"
        )

        tables = client.execute_sql(
            f"""
                SELECT
                    table_name
                FROM
                    information_schema.tables
                WHERE
                    table_schema = '{pipeline.dataset_name}'
                    AND table_name LIKE '%upcoming_launches_%'
                    AND table_name != 'upcoming_launches__mission__info_urls'
            """
        )

        for table in tables:
            table_name, = table
            print("Table name::: ", table_name)
            remove_duplicates_from_table(client, table_name)


def create_primary_key_for_table(pipeline: dlt.Pipeline, table_name: str, column: str):
    with pipeline.sql_client() as client:
      client.execute_sql(
          f"""
            DO $create_primary_key_if_not_exists$ 

            BEGIN IF NOT EXISTS (
                SELECT
                    *
                FROM
                    information_schema.table_constraints
                WHERE
                    constraint_type = 'PRIMARY KEY'
                    AND table_name = '{table_name}'
            ) THEN 

                ALTER TABLE
                    "upcoming_launches"."{table_name}"
                ADD
                    PRIMARY KEY ("{column}");

            END IF;

            END $create_primary_key_if_not_exists$
          """
      )

base_table_name = "upcoming_launches"

@dlt.resource(table_name=base_table_name, write_disposition="merge", primary_key="id", merge_key="id")
def get_upcoming_launches():
    next = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=100"

    while next != None:
        response = requests.get(next)
        response.raise_for_status()
        body = response.json()
        launches = body.get("results")
        next = body.get("next")
        yield launches


def load_upcoming_launches():
    pipeline = dlt.pipeline(
        pipeline_name="upcoming_launches_pipeline", destination="postgres", dataset_name="upcoming_launches"
    )
    load_info = pipeline.run(get_upcoming_launches)
    remove_duplicates(pipeline)
    create_primary_key_for_table(pipeline, base_table_name, "id")
    sys.stdout.write(str(load_info))
    print(load_info)


if __name__ == "__main__":
    try:
        load_upcoming_launches()
    except Exception as e:
        RED = "\033[91m"
        RESET = "\033[0m"
        sys.stderr.write(RED + str(e) + RESET + "\n")
        exit(1)

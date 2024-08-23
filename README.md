# Apache Spark & Kafka Demo

## Run dagster

```
docker compose up
```

## Start consumers

```
docker compose exec consumer /bin/bash
```

```
python3 manage.py update_statistics
```

## Open country stats page

http://localhost:8000/admin/core/countrystats/

User: demo

Password: 1234Demo

## Materialize dagster assets

Click materialize all assets in Dagster UI. Reload the page.

You will see the new stats from kafka have been saved in the database.





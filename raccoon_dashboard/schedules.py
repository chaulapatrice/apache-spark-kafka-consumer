from dagster import define_asset_job, ScheduleDefinition

asset_job = define_asset_job("asset_job", "*")

materialize_assets_schedule = ScheduleDefinition(job=asset_job, cron_schedule='0 * * * *')
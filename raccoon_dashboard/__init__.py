from dagster import Definitions, load_assets_from_modules

from . import assets
from .schedules import materialize_assets_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=[materialize_assets_schedule]
)

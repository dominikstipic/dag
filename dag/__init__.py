from dagster import (Definitions, 
                     load_assets_from_modules, 
                     define_asset_job, 
                     ScheduleDefinition,
                     AssetSelection)
from dag.etl import dfr_assets

DFR_GROUP = "dfr_group"
COUNTER_GROUP = "counter_group"

dfr_asset_group = load_assets_from_modules([dfr_assets], group_name=DFR_GROUP)

all_assets = [*dfr_asset_group]

# Jobs
dfr_job = define_asset_job("dfr_job", selection=AssetSelection.groups(DFR_GROUP))

# Schedules
dfr_job_schedule = ScheduleDefinition(
    job=dfr_job, cron_schedule="0 0 * * *" 
)
defs = Definitions(
    assets=all_assets,
    schedules=[dfr_job_schedule]
)
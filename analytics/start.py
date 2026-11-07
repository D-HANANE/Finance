import os
from pathlib import Path
import yaml
source=yaml.safe_load(Path('/app/dagster.yaml').read_text())
target=Path(os.environ.get('DAGSTER_HOME','/dagster'))/'dagster.yaml'
current=yaml.safe_load(target.read_text()) or {} if target.exists() else {}
for key,value in source.items():
    current.setdefault(key,value)
target.write_text(yaml.safe_dump(current))
# All workers of this local instance live in this container; none survives its restart.
from dagster import DagsterInstance,RunsFilter,DagsterRunStatus
with DagsterInstance.get() as instance:
    interrupted=instance.get_runs(filters=RunsFilter(statuses=[DagsterRunStatus.STARTED,DagsterRunStatus.STARTING,DagsterRunStatus.CANCELING]))
    for run in interrupted:
        instance.report_run_failed(run,message='Worker interrupted by instance restart; rerun is idempotent')
os.execvp('dagster',['dagster','dev','-h','0.0.0.0','-p','3000','-f','/app/definitions.py'])

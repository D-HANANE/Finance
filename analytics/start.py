"""Initialize optional instance settings also on an already existing volume."""
import os
import shutil
from pathlib import Path

target = Path(os.environ.get('DAGSTER_HOME','/dagster'))/'dagster.yaml'
if not target.exists():
    shutil.copyfile('/app/dagster.yaml',target)
os.execvp('dagster',['dagster','dev','-h','0.0.0.0','-p','3000','-f','/app/definitions.py'])

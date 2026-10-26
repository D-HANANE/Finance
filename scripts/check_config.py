import ast
import json
from pathlib import Path
import yaml

root = Path(__file__).resolve().parents[1]
for folder in ('app','spark','analytics','ui','tests','scripts','examples'):
    for path in (root/folder).glob('*.py'):
        ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
for path in root.rglob('*.yml'):
    if '.venv' not in path.parts:
        yaml.safe_load(path.read_text(encoding='utf-8'))
dashboard = json.loads((root/'grafana/dashboards/finance.json').read_text(encoding='utf-8'))
assert dashboard['refresh']=='5s' and len(dashboard['panels'])==3
print('Python, YAML et tableau de bord : valides')

#!/bin/bash
set -euo pipefail
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} ${SERVICE_OPTS:-}"
# The DB is persistent: init exactly when schema version is absent.
if /opt/hive/bin/schematool -dbType postgres -info; then
  echo 'HMS schema already initialized'
else
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi
exec /opt/hive/bin/hive --service metastore

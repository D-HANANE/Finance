# Licences et rôles

| Outil | Licence | Problème résolu |
|---|---|---|
| Python | PSF | Simulation et scripts |
| Kafka, Spark, Hive, Iceberg | Apache-2.0 | Transport, calcul, catalogue, transactions de table |
| MinIO | AGPL-3.0 | Stockage objet S3 local |
| PostgreSQL | PostgreSQL License | Service transactionnel et backend HMS |
| dbt Core, dbt-duckdb | Apache-2.0 | Modèles SQL, tests et lineage |
| DuckDB | MIT | Analytique embarquée |
| Soda Core 3.5.6 | Apache-2.0 | Contrôles opérationnels |
| Dagster | Apache-2.0 | Plans, relances, historique |
| Grafana OSS | AGPL-3.0 | Chandeliers en continu |
| Streamlit | Apache-2.0 | Application analytique |
| Plotly.py | MIT | Graphiques interactifs |
| boto3, PyArrow | Apache-2.0 | S3 et Parquet |
| confluent-kafka-python | Apache-2.0 | Clients Kafka |
| psycopg2 | LGPL avec exception OpenSSL | Écriture PostgreSQL |
| Docker Engine/Compose | Apache-2.0 | Exécution locale |

Docker Desktop est le prérequis Windows demandé ; sa distribution a des conditions
distinctes de Docker Engine/Compose. Aucun service cloud payant n'est requis.
Les images conservent les licences de leurs dépendances transitives.

Références techniques :

- https://spark.apache.org/docs/3.5.5/structured-streaming-programming-guide.html
- https://spark.apache.org/docs/3.5.5/structured-streaming-kafka-integration.html
- https://iceberg.apache.org/docs/1.7.2/aws/
- https://hive.apache.org/docs/latest/admin/setting-up-hive-with-docker/
- https://grafana.com/docs/grafana/latest/visualizations/panels-visualizations/visualizations/candlestick/
- https://github.com/sodadata/soda-core/tree/v3.5.6

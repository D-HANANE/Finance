# Diagnostic de migration vers les deux chaînes

Le projet est retrouvé sous Tech_Data. Diagnostic établi avant modification fonctionnelle.
Le cahier Word complet a été relu. Les nouvelles consignes utilisateur prévalent sur son ancienne architecture et sur le schéma générique : Gold utilise Spark SQL et Iceberg ; les alertes restent exclusivement PostgreSQL ; aucun service Hive.

| Composant | État actuel | Modification nécessaire |
|---|---|---|
| Producer | 20 symboles, 220/s, quantity, chocs | Conserver |
| Kafka | trades, 6 partitions, clé symbole | Conserver |
| Spark Streaming | Validation, déduplication, OHLCV 1 minute, watermark 2 minutes, PostgreSQL et Silver | Conserver PostgreSQL, séparer le traitement analytique |
| PostgreSQL | Upserts OHLCV et alertes sans cycle de vie | Migration additive des alertes, historique, verrou et curseur |
| Grafana | 3 panneaux, source PostgreSQL, refresh 5s | Conserver |
| MinIO Bronze | JSONL + Parquet + manifestes, offsets après archivage | Préserver les données, chemins et formats |
| Iceberg Silver | Depuis Kafka via Hive | Spark batch depuis les manifestes Bronze, catalogue JDBC |
| Iceberg Gold | Absent ; dbt/DuckDB utilisé | Spark SQL et tables Iceberg des métriques |
| Dagster | Gold et horaires | Assets et jobs séparés, évaluation des alertes à 5 minutes |
| Alertes | Calculées par le writer streaming | Source Gold, persistance uniquement PostgreSQL, statuts et historique |
| Streamlit | Source PostgreSQL | Métriques Gold via API de lecture Spark, alertes PostgreSQL |

Catalogue retenu : Iceberg JdbcCatalog, métadonnées dans PostgreSQL existant, fichiers Silver et Gold dans MinIO. Évite les hypothèses de renommage atomique de HadoopCatalog sur stockage objet. Aucun catalogue Hive.

Dépendances : Bronze -> transactions Silver canoniques -> chandeliers Silver -> métriques Gold -> évaluation -> alertes PostgreSQL. Le streaming Kafka -> PostgreSQL reste indépendant de ces calculs.

Règles conservées : chandeliers 1 minute ; trois actions Grafana ; refresh 5 secondes ; abs(variation sur 5 minutes) > 0.05. Fréquence du job alertes : 5 minutes, distincte du grain minute.

Hypothèse de cycle de vie : active tant que les observations valides dépassent le seuil ; résolue à une observation valide revenue dans le seuil ; clôture manuelle. Une référence absente ne résout pas arbitrairement une alerte.

Les volumes et l'ancienne base metastore ne seront pas supprimés. Les anciennes données Silver restent préservées ; le nouveau catalogue sera reconstruit depuis Bronze. Les résultats de validation antérieurs ne valent pas validation de cette migration.

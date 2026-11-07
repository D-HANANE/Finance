# Couverture de la nouvelle architecture

Les précisions utilisateur du 4 novembre remplacent l'ancien Gold dbt/DuckDB actif et
le catalogue précédent. Les intervalles et seuils du cahier Word restent inchangés.

| Exigence | Implémentation | Preuve exécutée / statut |
|---|---|---|
| Producer et Kafka conservés | app/market.py, producer.py, topic trades | Débit confirmé et vingt symboles |
| Streaming OHLCV une minute | spark/streaming.py | Test Spark réel et PostgreSQL |
| PostgreSQL sans doublons | app/serving.py, contraintes | test_serving.py |
| Grafana trois actions, 5s | provisioning et dashboard existants | Contrôle navigateur |
| Bronze préservé | storage.py et consumer_bronze.py inchangés | Archives et test_pipeline.py |
| Silver depuis Bronze | spark/lakehouse.py, manifestes et canonique | Job réel, registres, tables |
| Gold Spark SQL Iceberg | MINUTE_SQL, daily, corrélations | test_gold_spark.py et job réel |
| Catalogue sans Hive | spark/catalog.py, JDBC PostgreSQL | ICEBERG_V2_OK : nouveau processus, snapshots et tables relus |
| Alertes PostgreSQL uniquement | app/alerting.py, postgres/002-lakehouse.sql | test_alerts_v2.py, aucune table Iceberg d'alertes |
| Évaluation toutes les 5 min | Schedule lakehouse_and_alerts_every_five_minutes | Définitions et run Dagster |
| Statuts et historique | active/resolved/closed, UUID, empreintes | Tests de cycle de vie et Streamlit |
| Corrections tardives | Dirty minutes et metric_updated_at | Tests SQL isolés, rejeu réel sans doublons |
| Exécutions indépendantes | Jobs streaming_supervision et lakehouse_refresh | Deux arrêts séparés vérifiés ; run b1eff61b réussi sans streaming |
| Contrôles de qualité | Contrat, contraintes et contrôles SQL Spark | Tests nominal et invalide |
| CI | .github/workflows/ci.yml | Configurée, run GitHub non revendiqué |

Les résultats effectivement exécutés, avec nombres et limites, sont dans VALIDATION.md.
Les bonus FIN-09/10 restent non réalisés.

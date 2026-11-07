# Démonstration version 2

Préparer les services et une exécution lakehouse_refresh réussie avant la soutenance.

1. Montrer le diagnostic et le schéma à deux chaînes dans README.
2. Logs producer : confirmations Kafka, vingt symboles, environ 220/s.
3. Grafana : AAPL/MSFT/NVDA, chandeliers une minute, refresh cinq secondes.
4. MinIO Bronze : anciens et nouveaux JSONL/Parquet et manifestes conservés.
5. Dagster : groupe streaming indépendant ; groupe lakehouse bronze -> silver -> gold -> alerts.
6. Catalogue : schéma JDBC PostgreSQL et tables Iceberg silver/gold ; aucun service Hive.
7. Streamlit : cinq indicateurs PostgreSQL publiés depuis Gold, couverture des vingt actions, puis alertes actives et historiques.
8. Montrer un rejeu : pas de nouvel épisode ni d'historique dupliqué sans nouvelle observation.
9. Montrer resolved puis clôture manuelle, en expliquant cette hypothèse de cycle de vie.
10. Résultats réellement exécutés : VALIDATION.md. Ne pas présenter les preuves v1 comme des tests v2.

Les analyses peuvent fonctionner sur Bronze même si le service de streaming est
arrêté. Pour démontrer l'indépendance, arrêter uniquement spark et lancer
lakehouse_refresh ; le batch n'utilise pas ce service. Relancer spark ensuite.
Conserver tous les volumes et checkpoints. Ne pas interrompre Kafka pendant un test de reprise.

La simulation applique les chocs à toutes les actions, chacune à son tour ; montrer
alert_symbol_coverage pour distinguer une absence de dépassement d’une action non évaluée.
Les derniers tests sont dans docs/VALIDATION_MULTI_ACTIONS.md.

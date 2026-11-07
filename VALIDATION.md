> Complément actuel : [tests multi-actions et publication Gold PostgreSQL](docs/VALIDATION_MULTI_ACTIONS.md).

# Validation de la version 2 — 4 novembre 2026

Les résultats ci-dessous proviennent des commandes réellement exécutées sur le dépôt
déplacé dans C:\M2\P1\Tech_Data\Projet\Marche_Financier_Kafka_Bronze\finance-kafka-bronze.
L'ancien chemin Outils_Big_Data n'existe plus. Les résultats v1 du 26 octobre sont
archivés dans docs/archive/v1/VALIDATION.md et ne constituent pas des preuves V2.

## Résultats exécutés

| Contrôle | Résultat observé |
|---|---|
| Diagnostic avant modifications | Cahier Word intégral, Compose, Producer, Bronze, Spark, tables et interfaces analysés ; tableau dans docs/DIAGNOSTIC_V2.md |
| Construction Docker | Images Python, Spark, Dagster et Streamlit construites ; dépendances Spark résolues |
| Configuration | scripts/check_config.py réussi dans un conteneur Dagster avec le dépôt monté ; Python, YAML, dashboard, JDBC et absence de configuration Hive contrôlés |
| Tests unitaires | 9/9 réussis dans le conteneur Producer ; aucun test ignoré |
| Spark streaming réel | SPARK_TEST_OK : OHLCV=100/103/98/103, volume=10, notional=1007 ; doublons, retard admis, retard hors watermark et reprise des checkpoints vérifiés |
| Gold Spark SQL réel | GOLD_SPARK_OK : MA20=149,5 ; MA50=134,5 ; VWAP=129,5 ; référence exacte à cinq minutes, historique manquant et seuil de 5 % contrôlés |
| Service PostgreSQL isolé | POSTGRES_STREAM_OK : rejeu absolu sans double volume et protection des bougies finalisées |
| Alertes PostgreSQL isolées | ALERTS_V2_OK : épisodes actifs/résolus, seuil strict, rejeu sans historique dupliqué, correction tardive et clôture manuelle idempotente ; réponse Gold simulée dans ce test isolé |
| Kafka réel | 5 513 événements échantillonnés, 220,25/s ; 20 actifs et les 6 partitions observés ; clés conformes |
| Bronze réel | Parquet relu de 6 660 lignes, cohérent avec son manifeste ; ingestion existante conservée |
| Premier job analytique complet | Run d5a12d14-9789-4b01-be0d-fcab377bc733 réussi : 200 fichiers Bronze, 3 199 bougies/métriques et évaluation de 3 199 observations Gold |
| Rattrapage historique | Les jobs suivants ont traité les fichiers restants, puis seulement les nouveaux lots ; 540 manifestes traités au relevé intermédiaire |
| Catalogue Iceberg persistant | Après arrêt du moteur analytique, nouveau processus Spark : ICEBERG_V2_OK ; tables retrouvées via JDBC, snapshots relus et clés Gold uniques |
| Quantités Iceberg au contrôle indépendant | 3 576 648 transactions ; 5 860 bougies Silver ; 5 820 métriques minute Gold ; 120 lignes quotidiennes ; 190 corrélations |
| Vérification Silver indépendante | Une bougie réelle recalculée depuis les transactions canoniques ; OHLC, volume, notional et nombre d'échanges identiques |
| Gold pour Streamlit | LAKEHOUSE_V2_OK : 160 minutes AAPL et 4 lignes quotidiennes au premier relevé ; source Iceberg Gold / Spark SQL, snapshot 6086032037988744182 |
| Lecture après recréation finale | FINAL_GOLD_READ_OK : 299 minutes AAPL et 6 lignes quotidiennes ; snapshots Gold accessibles depuis le worker recréé |
| Alertes réelles | Épisodes issus de Gold présents dans PostgreSQL ; 35 détections historiques préservées comme legacy/closed ; aucune table Iceberg d'alertes |
| Rejeu réel | RUNTIME_REPLAY_OK : deux lectures de 80 observations du même snapshot, aucun actif modifié, 33 lignes d'historique avant/après ; une seule demande bronze validée pour le même request_id |
| Concurrence alertes réelle | ALERT_CONCURRENCY_OK : verrou détenu par une autre connexion PostgreSQL, évaluateur concurrent rejeté avant écriture |
| Schedules Dagster | Liste réelle : lakehouse_and_alerts_every_five_minutes RUNNING, cron */5 * * * * ; independent_streaming_supervision même cron ; end_of_day à 00:07 UTC |
| Streaming indépendant | Spark Streaming sain et événements servis jusqu'à 06:54:30 UTC pendant l'arrêt du moteur analytique |
| Analytique indépendante | Run b1eff61b-c4b9-4a55-8889-b27b4f8f0e2a réussi à 06:56:55 UTC, service spark arrêté : 7 fichiers supplémentaires, Silver, Gold et alertes terminés |
| Reprise réelle streaming | Service spark recréé avec le volume de checkpoints ; empreinte des 20 bougies finalisées comparées identique avant/après : e5392572c8abc0eaa20138e78dec9dfd ; données reçues jusqu'à 06:59:20 UTC au relevé |
| Interfaces réelles | UI_TEST_OK : navigateur Edge/Playwright, graphique Gold et historique Streamlit visibles, 3 panneaux Grafana alimentés et refresh=5s ; captures artifacts/ui |
| Rapport | PDF de 12 pages généré ; toutes les pages rendues avec Poppler, marges vérifiées et trois planches visuelles inspectées |
| État final des services contrôlés | PostgreSQL, Kafka, Spark Streaming, moteur Lakehouse, Dagster, Grafana et Streamlit sains ; Producer et consommateur Bronze en exécution |

Les nombres correspondent à des relevés successifs, pendant une ingestion continue.
Ils ne doivent pas être interprétés comme des comptes figés ou nécessairement identiques
entre les chaînes : le streaming applique une watermark et Gold attend les minutes closes.

## Commandes reproductibles

Utiliser les commandes de tests du README avec les dépendances des conteneurs.
Les tests d'intégration PostgreSQL créent et suppriment uniquement leur propre schéma temporaire.
Pour vérifier Iceberg dans un processus indépendant :

~~~powershell
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/verify_iceberg.py
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint python dagster /checks/scripts/check_config.py
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" dagster python /checks/test_runtime_replay.py
~~~

Le contrôle navigateur utilise Playwright et le chemin de l'exécutable Edge dans
BROWSER_EXECUTABLE. Aucun service externe, ordre financier ou publication distante
n'a été utilisé.

## Erreurs rencontrées et corrigées

- Une première écriture Silver a échoué : le plan MERGE Spark/Iceberg conservait une
  expression non déterministe. La matérialisation locale explicite de la source MERGE
  a corrigé le problème. Les jobs suivants, la relecture indépendante et les snapshots passent.
- Le premier lancement après redémarrage a reçu une connexion refusée pendant
  l'initialisation Spark. La reprise Dagster a réussi. Des retries sont configurés.
- Des runs Dagster interrompus provenant de l'ancienne instance occupaient la file.
  Ils sont conservés avec un statut d'échec ; le démarrage traite les workers interrompus.
- Le premier chargement navigateur de Grafana a dépassé 30 secondes. Le contrôle
  complet a réussi après adaptation du délai du test.
- Le Python Windows local ne contient ni PyYAML ni psycopg2. Les tentatives locales
  ont échoué ; les mêmes contrôles ont réussi dans les images appropriées.
- La première version du vérificateur de rejeu utilisait le nom type au lieu
  d'alert_type. Ce vérificateur a été corrigé puis son exécution complète a réussi.

## Limites et choix explicites

Le cahier ne définit pas la clôture d'un épisode : résolution automatique au retour dans
le seuil, puis clôture manuelle après résolution. Une référence absente ne résout rien.
La règle métier reste strictement abs(change_5m)>0.05.

Les transactions Bronze ont un identifiant immuable ; une seconde valeur sous le même
trade_id ne remplace pas arbitrairement la première version canonique.
Gold reprend les journées touchées et le contexte des fenêtres ; les statistiques de
corrélation quotidiennes sont consolidées sans relire tout l'historique brut à chaque run.

Pas de transaction distribuée PostgreSQL/Iceberg : la publication d'identifiants de
snapshots et les MERGE idempotents encadrent les reprises. Aucun volume n'a été supprimé.
Les anciennes données du catalogue restent préservées et inutilisées. Hive est absent
du Compose et des configurations Spark actives.

La CI GitHub est configurée mais n'a pas été exécutée à distance. Les bonus de mesure
de latence de bout en bout et ClickHouse ne sont pas revendiqués. Aucun test de haute
disponibilité ou de charge prolongée n'est revendiqué.

Après le déplacement du dossier, les commandes git status de l'environnement renvoient
« must be run in a work tree ». Les métadonnées Git n'ont pas été réinitialisées et aucun
commit ni push n'a été effectué ; cette anomalie ne bloque pas Docker ni les traitements.

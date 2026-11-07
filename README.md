# Marché financier simulé — deux chaînes indépendantes

Version 3 : deux chaînes, alertes multi-actions et publication des indicateurs Gold dans PostgreSQL.
Membres du groupe : à renseigner. Les symboles sont réels, mais tous les prix et échanges sont simulés.

## Démarrage Windows

Docker Desktop Linux, 16 Go physiques, environ 8 Go attribués à Docker. Depuis ce dossier :

~~~powershell
Copy-Item .env.example .env  # uniquement si .env est absent
docker compose up -d --build
~~~

Aux lancements suivants : `docker compose up -d`. Ne pas écraser un .env existant.
Les mots de passe du fichier exemple sont des valeurs de démonstration.
L'initialisation applique une migration additive sur une base PostgreSQL existante.
Aucun volume ni archive Bronze n'est supprimé.

| Interface | Adresse | Accès |
|---|---|---|
| Grafana | http://localhost:3001/d/finance-live | admin / GRAFANA_PASSWORD |
| Streamlit | http://localhost:8501 | Gold et alertes |
| Dagster | http://localhost:3000 | Assets, jobs, schedules et historique |
| MinIO | http://localhost:9001 | S3_ROOT_USER / S3_ROOT_PASSWORD |

## Architecture active

~~~mermaid
flowchart LR
 P[Producer Python] --> K[Kafka trades 6 partitions]
 K --> SS[Spark Structured Streaming]
 SS --> PG[PostgreSQL chandeliers]
 PG --> GF[Grafana trois actions 5 s]
 K --> B[Consommateur Bronze existant]
 B --> M[MinIO Bronze JSONL et Parquet]
 M --> BS[Spark batch]
 BS --> SI[Iceberg Silver dans MinIO]
 SI --> SQL[Spark SQL]
 SQL --> GO[Iceberg Gold dans MinIO]
 GO --> GS[PostgreSQL métriques Gold]
 GS --> UI[Streamlit analyses]
 GO --> A[Dagster évaluation toutes les 5 min]
 A --> PA[PostgreSQL alertes et historique]
 PA --> UI
 CAT[Catalogue Iceberg JDBC dans PostgreSQL] --- SI
 CAT --- GO
~~~

Le mécanisme Bronze existant reste Producer → Kafka → consommateur Bronze → MinIO.
La chaîne analytique lit uniquement les archives MinIO et peut traiter leur historique
sans le service Spark Streaming. La chaîne streaming n'utilise ni catalogue ni tables Iceberg.

Le catalogue JDBC conserve seulement les pointeurs Iceberg dans le schéma
`iceberg_catalog` de la base PostgreSQL finance existante. Les données et fichiers
de métadonnées Silver/Gold restent dans MinIO. Aucun service ou catalogue Hive n'est utilisé.
Le catalogue Hadoop n'est pas retenu : les commits sur stockage objet demandent
des précautions de concurrence supplémentaires. [Documentation Apache Iceberg](https://iceberg.apache.org/docs/1.7.2/jdbc/).

## Traitements et grains

- Streaming : chandeliers d'une minute ; tri Open/Close par (event_time, trade_id),
  watermark deux minutes, déduplication et checkpoints distincts sous /state/v2.
  Upserts absolus PostgreSQL par (symbol,minute_start), sans calcul d'alertes.
- Bronze : chemins, JSONL, Parquet, manifestes et commit d'offsets après archivage conservés.
- Silver : transactions canoniques, rejets et chandeliers minute. Lecture des seuls
  Parquet dont le manifeste est complet. Journal d'ingestion par clé et ETag.
  Les données tardives peuvent corriger les minutes historiques.
- Gold minute : close, MA20/MA50, log-rendement, VWAP journalier cumulatif et
  variation par rapport à la clôture de la minute exactement cinq minutes auparavant.
- Gold quotidien : rendement entre jours UTC consécutifs, volatilité minute regroupée
  par jour, VWAP pondéré, couverture. Gold corrélations : statistiques par jour
  puis Pearson sur l'historique disponible.
- Une minute entre dans Gold après sa clôture et le délai de deux minutes.
  Les corrections ultérieures restent prises en compte. Les valeurs manquantes
  ou un historique insuffisant produisent NULL.

Voir [dictionnaire](docs/DATA_DICTIONARY.md), [architecture](docs/ARCHITECTURE.md),
[diagnostic préalable](docs/DIAGNOSTIC_V2.md), [couverture](docs/COVERAGE.md)
et [résultats réellement exécutés](VALIDATION.md).

## Dagster

| Job | Rôle | Déclenchement |
|---|---|---|
| streaming_supervision | Observe Producer, Kafka, requêtes Spark et PostgreSQL | Toutes les 5 min ou manuel |
| lakehouse_refresh | Bronze → Silver → Gold → alertes PostgreSQL | Toutes les 5 min |
| alerts_refresh | Réévalue les nouvelles observations Gold publiées | Manuel, sans recalcul des métriques |
| end_of_day | Même chaîne analytique en fin de journée | Schedule à 00:07 UTC |

Les processus continus sont démarrés par Compose. Les assets de supervision se
terminent : ils n'hébergent pas une boucle infinie dans un run Dagster.
Un worker Spark analytique distinct exécute les tâches bornées via une API interne
non exposée sur l'hôte. Aucune socket Docker n'est montée dans Dagster.
La file limite la concurrence à un run ; les writers possèdent aussi un verrou PostgreSQL.
Les anciens runs interrompus restent dans l'historique avec un statut d'échec.

Pour lancer immédiatement une chaîne :

~~~powershell
docker compose exec dagster dagster job execute -f definitions.py -j streaming_supervision
docker compose exec dagster dagster job execute -f definitions.py -j lakehouse_refresh
docker compose exec dagster dagster job execute -f definitions.py -j alerts_refresh
~~~

Le premier passage peut avoir beaucoup de fichiers Bronze à reprendre.
BRONZE_FILES_PER_RUN limite le lot (200 par défaut). Les passages suivants ne
reprennent que les fichiers nouveaux ou modifiés. Les tâches sont rejouables par clé.

## Alertes PostgreSQL

Règle conservée : `abs(change_5m) > 0.05`. L'évaluation est planifiée toutes les
cinq minutes ; les observations sont des chandeliers d'une minute.
Chaque observation Gold nouvelle ou corrigée est parcourue, pas seulement la dernière.
Le traitement lit des métriques existantes ; il ne recalcule pas le rendement financier.

Un épisode continu au-dessus du seuil conserve le même identifiant. Il est actif,
puis résolu à la prochaine observation valide revenue dans le seuil.
Une référence absente ne résout pas arbitrairement l'alerte.
Le cahier ne précisant pas la clôture, elle est manuelle depuis Streamlit, après résolution.
Les tables alerts, alert_observations, alert_history et alert_state se trouvent
dans la base PostgreSQL finance. Aucune table Iceberg d'alertes n'est créée.

Un curseur sur la date de modification Gold prend aussi en compte les corrections
de minutes anciennes. Les épisodes ont des UUID déterministes. Le traitement
transactionnel et son verrou empêchent les doublons et les évaluations concurrentes.

## Contrôles et tests

~~~powershell
docker compose ps -a
docker compose logs --tail 10 producer consumer-bronze spark lakehouse
docker compose run --rm --no-deps -v "${PWD}/tests:/tests:ro" producer python -m unittest discover -s /tests -v
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit -v "${PWD}/scripts:/checks:ro" spark --master local[2] /checks/test_spark.py
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit -v "${PWD}/scripts:/checks:ro" spark --master local[2] /checks/test_gold_spark.py
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/postgres:/schema:ro" producer python /checks/test_serving.py
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/postgres:/schema:ro" producer python /checks/test_alerts_v2.py
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" dagster python /checks/verify_lakehouse_v2.py
~~~

L'image Python contient le code courant. Pour tester des modifications non
reconstruites, monter aussi ${PWD}/app:/app:ro dans les commandes de tests Python.
Les mesures de débit doivent être lues dans les callbacks Kafka du producteur,
pas déduites de la configuration 220/s.

## Migration et limites

Les 35 anciennes alertes détectées avant migration sont conservées dans
alerts_legacy_v1 et archivées dans alerts avec origin=legacy. Leur ancien journal
ne permet pas de déduire un état actif actuel ; elles ne sont donc pas présentées comme actives.
Les anciens fichiers Silver, la base historique de catalogue, les checkpoints v1,
les volumes exchange/gold_data et le projet dbt restent préservés mais ne pilotent
plus l'architecture active. Les documents et rapports v1 sont archivés sous docs/archive/v1 ; le rapport courant est dans output/pdf.

Le nouveau catalogue reconstruit Silver et Gold depuis Bronze, dans des préfixes
lakehouse-v2 séparés. Les nouveaux checkpoints streaming évitent de réutiliser
un journal de sink avec une définition de requête différente.

Il n'existe pas de transaction distribuée entre PostgreSQL et Iceberg.
Les écritures absolues et les journaux rendent les reprises idempotentes, avec
une possible visibilité différée entre étapes. Gold n'est exposé aux lecteurs
qu'après publication d'un ensemble cohérent d'identifiants de snapshots.
Un trade_id doit être immuable ; la première version canonique retenue est conservée.
Les conflits de contenu ne doivent pas être considérés comme des corrections métier.

Kafka mono-broker et stockage local : maquette, sans haute disponibilité.
Ne pas utiliser docker compose down -v pour préserver les données.
Les bonus de latence jusqu'au graphique et de comparaison ClickHouse ne sont pas revendiqués.

Rapport de la version 2 : [PDF de 12 pages](output/pdf/rapport_plateforme_financiere.pdf). Les changements de version 3 sont documentés dans [la validation multi-actions](docs/VALIDATION_MULTI_ACTIONS.md).

Guide pratique : [exécution et actualisation des conteneurs](docs/GUIDE_EXECUTION.md).

## Vérification des vingt actions et des cinq indicateurs

Les chocs de démonstration tournent désormais sur tous les actifs. Les alertes restent
conditionnelles au seuil commun, jamais forcées pour remplir une liste.
Gold Iceberg reste la source analytique ; ses résultats sont publiés dans les tables
PostgreSQL gold_minute, gold_daily et gold_correlations lues par Streamlit.
Le tableau de couverture distingue action évaluée, référence manquante et dépassement.
Voir [corrections et tests multi-actions](docs/VALIDATION_MULTI_ACTIONS.md).

## Comprendre, vérifier et présenter le projet

- [Guide pédagogique de A à Z](GUIDE_COMPREHENSION_VERIFICATION.md)
- [Plan des captures et légendes](PLAN_CAPTURES.md)
- [Scénario de soutenance : 15 minutes, dont 7 de démonstration](SCENARIO_PRESENTATION.md)

Ces supports distinguent les preuves historiques des contrôles à refaire. Le 5 novembre,
le moteur Docker était inaccessible pendant la rédaction ; les vérifications des services
et nouvelles captures restent donc à réaliser localement.

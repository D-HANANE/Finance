# Résultats de validation

Vérifications exécutées le 26 octobre 2026, relevé final vers 15:39 UTC.
Machine Windows, Docker Desktop Linux, 7,49 Gio de mémoire disponibles dans le
moteur Docker. Le dépôt et ses volumes contenaient déjà un historique ; aucun
volume existant n'a été supprimé et aucune mesure n'a été inventée.

## Résultats exécutés

| Contrôle | Résultat observé |
|---|---|
| Lecture du cahier et audit | Sujet financier FIN-01 à FIN-08 obligatoire ; FIN-09/10 optionnel ; rapport 10-15 pages ; démo 7 min |
| Tests unitaires Python | 5 tests réussis dans l'image du projet, aucun ignoré. Génération, choc AAPL, seuil strict, oracle OHLCV, Bronze JSONL/Parquet/offsets/manifeste et message malformé |
| Spark réel sans infrastructure externe | Décodage et alias volume vérifiés. OHLCV 100/103/98/103/10, notional 1007, 4 échanges uniques. Retard admis, doublon, événement trop tardif et reprise avec checkpoints vérifiés |
| PostgreSQL isolé | Upserts rejoués sans doubler les volumes ; seuils +5/-5 exclus ; +8/-8 détectés ; référence absente ignorée ; ligne finale protégée |
| dbt sur jeu temporaire | 6 modèles et 15 tests réussis, PASS=21, ERROR=0, SKIP=0 ; MA20=149,5 et MA50=134,5 au dernier close de 159 ; rendement/VWAP/volatilité comparés à des références indépendantes |
| Soda sur jeu temporaire | 14/14 contrôles réussis |
| Échec qualité injecté | Un low supérieur au high provoque un échec dbt candle_contract et un échec Soda OHLCV. Le script global réussit parce que ces échecs sont attendus |
| Démarrage complet | docker compose up -d --build exécuté avec succès ; init termine à 0 ; services avec healthcheck sains |
| Kafka | Topic trades, exactement 6 partitions ; 20 symboles observés ; toutes les clés correspondent au symbole ; les six partitions sont présentes dans l'échantillon |
| Débit | 5 503 messages reçus sur environ 25 s, 220,04/s. Fenêtres de confirmations du producteur : environ 219,9 à 220,3/s, zéro erreur et 20 symboles confirmés |
| Bronze réel | Manifeste complet relu ; Parquet de 6 681 lignes cohérent avec row_count ; prochains offsets conservés. Le consommateur continue après redémarrage |
| Iceberg/Hive/S3 | 380 lignes Silver comparées à PostgreSQL final : valeurs OHLCV/notional identiques et clés uniques. Lecture du snapshot 3745816776385664086 réussie |
| Reprise réelle | Spark et Bronze redémarrés avec leurs volumes. Comparaison de 20 chandeliers déjà finalisés : valeurs identiques avant/après et aucune clé dupliquée |
| Dagster réel | Run 47b3d9d5-41e6-4908-acaf-7dd5bc7216a7 réussi le 26/10 à 15:34:57 UTC. dbt build, docs generate, 14 contrôles Soda puis publication PostgreSQL |
| Planifications | gold_every_five_minutes : */5 * * * * UTC ; end_of_day : 00:05 UTC ; les définitions sont chargées. Un run planifié supplémentaire a publié Gold à 15:35:21 UTC |
| Grafana | Source PostgreSQL, 3 panneaux Candlestick AAPL/MSFT/NVDA, rafraîchissement 5 s ; rendu réellement observé dans un navigateur |
| Streamlit | Graphique, journal d'alertes et analyses Gold rendus dans le navigateur ; aucune exception applicative observée |
| Syntaxe et configuration | Python, YAML, JSON du dashboard et docker compose config vérifiés |
| Rapport | PDF de 12 pages généré et rendu en PNG pour contrôle visuel |

## Relevé des données

À 15:39 UTC : 2 400 chandeliers PostgreSQL, dont 2 340 finalisés ; 20 derniers
prix ; 15 alertes ; 60 lignes Gold quotidiennes. Gold contient 1 860 valeurs
MA20 et 1 260 valeurs MA50 non nulles. Ces compteurs évoluent pendant le flux.
Le volume de 60 lignes quotidiennes vient de l'historique préexistant, et non
d'un historique ajouté pour faire réussir les indicateurs.

Une alerte AAPL observée à 15:33:19 UTC compare son prix à la référence de
15:27:59 UTC : variation +8,762 %. Le producteur a réellement injecté son choc
après six minutes. Les baisses liées aux redémarrages du générateur sont aussi
visibles : elles proviennent de la réinitialisation de la marche aléatoire.
Les captures ne masquent pas ces transitions.

La consommation observée de tous les services est d'environ 4 Gio, hors tests
éphémères et cache Docker. Ce relevé ponctuel n'est pas un benchmark prolongé.

## Incidents rencontrés et corrigés

- L'image Apache Spark exige spark-submit pour charger PySpark. Les packages
  Kafka/Iceberg sont maintenant résolus explicitement lors du build, via Maven.
- Java constaté dans l'image Spark : 11.0.26 ; Scala 2.12 et Spark 3.5.5.
- Hive 4.0.1 supprime get_table, requis par le client Spark. Hive 4.0.0 est retenu.
  Son Hadoop 3.3.6 et ses dépendances S3 incluses sont activés pour les chemins S3.
- Les vues temporaires foreachBatch appartiennent à la session du micro-lot.
  Le MERGE utilise maintenant batch.sparkSession.
- Soda Core 3.5.6 impose DuckDB <1.1 : DuckDB 1.0.0 est utilisé dans les deux
  environnements. Le paramètre de connexion correct est path à la racine.
- L'image MinIO officielle demandée au registre est indisponible. L'image S3
  déjà utilisée par le dépôt est conservée, figée par son digest.
- Des extensions Grafana suggérées automatiquement étaient incompatibles avec
  Grafana 11.6. Elles sont désactivées ; les panneaux intégrés fonctionnent.
- Pendant le rattrapage des événements anciens, le contrôle de fraîcheur a
  bloqué Gold et Dagster a relancé le traitement. Aucun résultat périmé n'a été
  publié. Après rattrapage, les contrôles et la publication ont réussi.

## Limites des preuves

GitHub Actions est configuré mais aucun push ni run distant n'a été effectué.
Le déclenchement horaire de fin de journée n'a pas été attendu jusqu'à 00:05 ;
le même job a été exécuté manuellement et la planification est chargée.
Le redémarrage est vérifié, pas une campagne de pannes réseau ou de perte disque.
La rétention Kafka expirée et une reconstruction complète depuis Bronze n'ont
pas été testées. Aucune atomicité distribuée ou exactly-once globale n'est
revendiquée. FIN-09 et FIN-10 ne sont pas réalisés.

## Reproduire

Les commandes sont dans README.md. Vérificateurs : tests/, scripts/test_spark.py,
scripts/test_serving.py, scripts/test_gold.py, scripts/verify_runtime.py,
scripts/verify_iceberg.py, scripts/check_restart.py et scripts/verify_ui.cjs.
Les captures réelles et leur horodatage sont conservés dans docs/evidence.

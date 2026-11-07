# Couverture du cahier des charges

Implémenté décrit le code. VALIDATION.md atteste uniquement les contrôles exécutés.

| Réf. | Type | Implémentation | Vérification | Statut |
|---|---|---|---|---|
| FIN-01 | OBL | market.py, producer.py, cible 220/s | 20 symboles ; confirmations Kafka >=200/s | Vérifié : environ 220/s, zéro erreur |
| FIN-02 | OBL | clé symbole, init topic 6 partitions | describe et verify_runtime.py | Vérifié : six partitions et clés correctes |
| FIN-03 | OBL | spark/streaming.py, watermark et 4 checkpoints | test_spark.py, métriques, redémarrage | Vérifié : OHLCV connu, retards, doublons, checkpoints |
| FIN-04 | OBL | lake.finance.candles, Hive, S3 | verify_iceberg.py et check_restart.py | Vérifié : 380 lignes concordent, snapshot lisible, reprise stable |
| FIN-05 | OBL | modèles dbt minute et quotidien | test_gold.py et run Dagster réel | Vérifié : formules et publication Gold |
| FIN-06 | OBL | contraintes SQL, dbt, Soda | 15 tests dbt, 14 contrôles Soda, échec injecté | Vérifié : succès nominal et échec attendu |
| FIN-07 | OBL | 3 panneaux Grafana, refresh 5s | docs/evidence/grafana.png, verify_ui.cjs | Vérifié dans un navigateur |
| FIN-08 | OBL | serving.py, UI, choc AAPL | docs/evidence/streamlit-alerts.png ; +8,762 % détecté | Vérifié : ligne réelle et affichage |
| FIN-09 | OPT | Pas de mesure jusqu'au rendu client | Aucune latence graphique revendiquée | Non réalisé |
| FIN-10 | OPT | PostgreSQL retenu | Pas de benchmark ClickHouse revendiqué | Non réalisé |
| Commun | OBL | Compose, volumes, init, healthchecks | commande README exécutée | Vérifié sur la machine cible |
| Commun | OBL | Dagster, 2 schedules, retries | chargement et run réel, run planifié | Vérifié ; horaire 00:05 non attendu |
| Commun | OBL | GitHub Actions, tests | workflow versionné | Configuré ; aucun run GitHub revendiqué |
| Commun | OBL | README, architecture, dictionnaire, rapport, démo | fichiers du dépôt | Préparés |

Les noms d'actions réelles sont une adaptation demandée : le sujet dit actions
fictives. Le caractère simulé est indiqué partout. La bibliothèque standard
réalise la marche aléatoire, sans dépendance numpy superflue.

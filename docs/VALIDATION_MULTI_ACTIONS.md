# Vérification multi-actions et indicateurs Gold — version 3

## Diagnostic constaté

Les vingt symboles disposaient chacun de 310 observations d'alerte. AAPL était la seule
action avec des dépassements, car le Producer appliquait son choc de démonstration
uniquement à cette action. Le moteur d'alertes traitait déjà chaque symbole indépendamment.
Les autres actions restaient en dessous du seuil : leur absence d'alerte n'était pas une exclusion.

Les cinq indicateurs existaient dans les calculs Spark, mais PostgreSQL conservait
des projections anciennes, sans publication active depuis Iceberg. Streamlit lisait
directement l'API Spark. Le test avec des volumes variables a également révélé un
arrondi prématuré du VWAP lors de la division décimale Spark.

## Corrections

- Market.shock exige un symbole explicite ; shock_next parcourt les vingt symboles
  équitablement. Le Producer espace les chocs sur un cycle SHOCK_EVERY_SECONDS :
  par défaut, un choc toutes les 15 secondes sur une action différente, chaque action
  étant revisitée toutes les cinq minutes. Le premier commence après 360 secondes.
  Le choc de démonstration reste +8 % ; le seuil d'alerte reste strictement >5 %.
- Aucune branche du moteur d'alertes n'est réservée à une action. Les observations,
  épisodes et historiques possèdent leurs clés par symbole.
- Spark convertit les sommes pondérées en double avant la division VWAP minute et
  quotidienne. La somme des notionals reste calculée en décimal.
- La version gold-v3-vwap-precision provoque une seule réparation historique.
  Les exécutions suivantes reprennent les jours touchés et les métriques modifiées.
- Les tables PostgreSQL gold_minute, gold_daily et gold_correlations sont réutilisées.
  La migration ajoute des colonnes sans supprimer les données existantes.
- Les résultats proviennent des snapshots Iceberg, sans recalcul financier dans PostgreSQL.
  Les upserts et gold_serving_publication sont publiés avec lakehouse_publication dans
  la même transaction PostgreSQL. En cas d'échec, le dernier état validé reste visible.
- Streamlit lit PostgreSQL dans une transaction cohérente : cinq cartes, courbes MA/VWAP,
  historique quotidien, tableau alert_symbol_coverage et filtres d'alertes par action.

## Définitions conservées

Daily Return = close du jour / close du jour UTC précédent - 1. Sans clôture de la veille :
NULL, explicitement indiqué dans Streamlit. Un historique avec des jours absents ne
permet pas d'inventer un rendement quotidien ni de le remplacer par zéro.

Daily Volatility = écart type échantillonnal des log-rendements entre minutes consécutives
du jour ; au moins deux valeurs ; aucune annualisation. La journée courante est provisoire.

VWAP = somme(price*quantity) / somme(quantity). Le VWAP minute est cumulatif depuis
minuit UTC ; le VWAP quotidien couvre les bougies disponibles du jour.
MA20/MA50 utilisent les vingt/cinquante clôtures minute observées, partitionnées par symbole.
Avant d'avoir suffisamment de données, elles restent NULL.

## Tests réellement exécutés

| Contrôle | Résultat |
|---|---|
| Tests unitaires du projet | 9/9 réussis, dont deux cycles de chocs couvrant chaque actif exactement une fois par cycle |
| Fixture intégrée Spark/Iceberg/PostgreSQL | ALL_SYMBOLS_GOLD_OK : 20 actifs, 2 400 minutes, 40 journées ; les cinq indicateurs concordent avec des références Python indépendantes |
| Alertes issues de cette fixture Gold | 20 épisodes distincts, résolus ; rejeu stable sans historique dupliqué ; schéma PostgreSQL et catalogue Iceberg locaux temporaires supprimés en fin de test |
| Cycle de vie PostgreSQL | ALERTS_V2_OK : maintien, résolution, clôture, correction tardive et rejeu |
| Réparation historique réelle | Run Dagster e3760272-6e73-4b3f-b4d7-3fb68ab9b96b réussi : 6 660 lignes Gold minute, 120 quotidiennes et 190 corrélations publiées dans PostgreSQL |
| Comparaison exhaustive réelle | ALL_SYMBOLS_RUNTIME_OK : chaque valeur de 6 660 lignes minute et 120 quotidiennes égale à son snapshot Iceberg ; les vingt actions ont une couverture d'alertes et des journées avec les cinq indicateurs |
| Streamlit réel | ALL_SYMBOLS_UI_OK : vingt sélections d'actif, cinq cartes d'indicateurs, source PostgreSQL et filtre d'alertes MSFT |
| Alertes de production | SELECT count(DISTINCT symbol) FROM alerts WHERE origin=gold : 20 ; au relevé, 17 épisodes actifs et 18 résolus, hors anciennes détections legacy |
| Publication incrémentale suivante | 180 lignes minute modifiées publiées, avec 120 agrégats quotidiens et 190 corrélations ; la réparation complète ne se répète pas |
| Générateur réel | Logs CHOC observés sur les vingt actifs du cycle ; confirmations Kafka à environ 220/s, vingt symboles, zéro erreur |

Le premier test intégré a échoué sur le VWAP ; ce résultat a conduit à la correction.
Le test complet a ensuite réussi. Le premier script navigateur ne trouvait pas MSFT
dans la liste virtualisée ; la recherche dans le composant a corrigé le test, puis les
vingt sélections ont réussi. Aucun échec n'est présenté comme une réussite.

Les nombres sont des relevés ponctuels pendant une ingestion continue.
Un épisode de production n'est créé que lorsqu'une observation Gold éligible franchit
le seuil. Le tableau de couverture distingue zéro dépassement et référence manquante.
Les tests temporaires ne sont pas injectés dans les données financières de production.

## Reproduire

~~~powershell
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" -v "${PWD}/postgres:/schema:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/test_all_symbols_gold.py

docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/verify_all_symbols_runtime.py

docker compose exec postgres psql -U finance -d finance -c "SELECT * FROM alert_symbol_coverage ORDER BY symbol;"
~~~

Le test navigateur scripts/verify_all_symbols_ui.cjs nécessite Playwright et
BROWSER_EXECUTABLE configuré vers Edge/Chromium.

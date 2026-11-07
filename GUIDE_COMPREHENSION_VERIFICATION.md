# Guide de compréhension, vérification et démonstration du projet

Édition du 5 novembre 2026, horaires exprimés en UTC. Version étudiée : architecture V3 et publication Gold PostgreSQL.

## Lire ce guide et distinguer les preuves

Ce document décrit le **code réellement présent**, pas une architecture idéale. Les résultats du 4 novembre figurent dans VALIDATION.md et docs/VALIDATION_MULTI_ACTIONS.md. Ils sont historiques. Le 5 novembre, Docker Desktop n'était pas accessible : aucun nouveau résultat Kafka/Spark/MinIO/PostgreSQL ni aucune nouvelle capture d'interface n'est revendiqué. Le bilan exact figure en fin de guide.

Chemin réel retrouvé :

~~~text
C:\M2\P1\Tech_Data\Projet\Marche_Financier_Kafka_Bronze\finance-kafka-bronze
~~~

L'ancien chemin Outils_Big_Data ne correspond plus au dossier trouvé. Les noms de fichiers mentionnés ensuite sont relatifs à ce dossier. Les trois supports nouveaux sont placés à sa racine. PLAN_CAPTURES.md fournit les légendes ; SCENARIO_PRESENTATION.md organise 15 minutes de soutenance, **dont 7 minutes de démonstration**, conformément au Word.

Une **configuration** indique ce qui devrait se passer. Un **service healthy** prouve une disponibilité limitée. Une **donnée retrouvée en sortie et comparée à une référence** prouve une transformation précise. Toujours annoncer le niveau de preuve obtenu.

## 1. Comprendre le projet comme un système métier

### Besoin, utilisateurs, entrées et sorties

Une salle de marché pédagogique suit vingt actions. Une analyste veut voir les cours bouger, comparer leurs tendances et repérer un mouvement supérieur à 5 % sur cinq minutes. Un responsable de la plateforme veut conserver les transactions, pouvoir recalculer les résultats et reprendre après une interruption.

Les noms d'entreprises sont réels, mais **tous les prix, quantités et échanges sont simulés**. Il ne s'agit pas d'un service de cotations réelles, de prévision ni de conseil en investissement.

Une transaction est un événement portant un identifiant unique, une action, un prix, une quantité et une date. Exemple pédagogique de forme, pas événement prétendument observé :

~~~json
{"trade_id":"exemple_msft_001","symbol":"MSFT","price":100.0,"quantity":2,"event_time":"2026-11-05T12:00:10+00:00"}
~~~

Les vingt symboles sont AAPL, MSFT, AMZN, GOOGL, META, NVDA, TSLA, NFLX, ADBE, CRM, ORCL, IBM, INTC, AMD, JPM, BAC, V, MA, KO et WMT.

Un **chandelier** résume les échanges d'une action pendant une minute. Son ouverture est le premier prix, sa clôture le dernier, son plus haut et son plus bas les extrêmes. Le volume est la somme des quantités. Une transaction n'est donc pas une bougie ; une bougie peut résumer des centaines d'événements.

### Schéma fidèle : données et déclenchements

Les flèches pleines représentent les données. Les flèches pointillées représentent le déclenchement ou la supervision, pas un transport de transactions.

~~~mermaid
flowchart TD
    P["Producer Python : 20 actions simulées"] --> K["Kafka : trades, 6 partitions"]
    K --> SS["spark : Structured Streaming"]
    SS --> CP["PostgreSQL : candles, latest_prices"]
    CP --> G["Grafana : 3 chandeliers"]

    K --> B["consumer-bronze : archivage existant"]
    B --> M["MinIO Bronze : JSONL + Parquet + manifestes"]
    M --> SB["lakehouse : Spark batch"]
    SB --> SI["Iceberg Silver dans MinIO"]
    SI --> SQ["Spark SQL : calculs Gold"]
    SQ --> GI["Iceberg Gold dans MinIO"]
    GI --> PUB["Publication des snapshots et des résultats"]
    PUB --> GP["PostgreSQL : gold_minute, gold_daily, gold_correlations"]
    GP --> UI["Streamlit : analyse"]

    GI --> EV["Évaluation des observations Gold"]
    EV --> AP["PostgreSQL : alertes et historique"]
    AP --> UI

    D["Dagster : schedules et jobs finis"] -.-> SB
    D -.-> EV
    D -.-> SS
    DC["Docker Compose : démarrage des services continus"] -.-> P
    DC -.-> K
    DC -.-> B
    DC -.-> SS
    CAT["Catalogue JDBC : schéma iceberg_catalog dans PostgreSQL"] --- SI
    CAT --- GI
~~~

La seconde chaîne ne lit pas les bougies du streaming pour construire Silver : elle lit les archives Bronze. Les deux chaînes partagent les événements d'origine, mais ont des traitements et des temporalités indépendants.

### Chaque composant, de l'entrée à la sortie

| Composant réel | Reçoit | Fait et transmet | Pourquoi il existe |
|---|---|---|---|
| app/market.py et app/producer.py | Paramètres de simulation | Génère les événements ; envoie dans Kafka avec clé symbol ; mesure les confirmations | Fournir une source maîtrisée et mesurer le débit livré |
| Kafka 3.8.0, service kafka | Messages du Producer | Conserve un journal ordonné par partition ; deux consommateurs indépendants | Découpler transport, archivage et streaming |
| app/consumer_bronze.py | Topic trades | Constitue un lot de 10 000 messages ou attend 30 s ; archive avant de valider les offsets | Conserver la donnée brute et permettre la reprise |
| MinIO, service minio | JSONL, Parquet et manifestes | Stocke les objets Bronze et les fichiers Iceberg Silver/Gold | Archive durable dans des volumes locaux |
| spark/streaming.py, service spark | Kafka puis Parquet validés sous /state/v2 | Valide, déduplique, calcule OHLCV ; écrit PostgreSQL | Suivi continu indépendant du batch |
| PostgreSQL 16.8 | Bougies, projections Gold, alertes, pointeurs de catalogue | Contraintes, transactions, upserts et lectures des interfaces | Base de service et catalogue transactionnel |
| spark/lakehouse.py, service lakehouse | Manifestes Bronze complets | Transactions Silver canoniques, bougies, métriques Gold | Recalcul analytique et gestion des corrections historiques |
| Apache Iceberg 1.7.2 | Fichiers et commits Spark | Organise des tables avec snapshots et clés de fusion | État tabulaire durable et versions relisibles |
| spark/catalog.py | Configuration MinIO/PostgreSQL | JdbcCatalog dans iceberg_catalog ; fichiers dans MinIO | Retrouver les tables sans Hive |
| spark/gold_serving.py | Snapshots Gold validés | Copie les valeurs calculées vers PostgreSQL ; publie les références de snapshots | Servir Streamlit sans recalculer les indicateurs dans PostgreSQL |
| app/alerting.py | Observations de Gold publiées | Applique le seuil pour chaque symbole ; conserve épisodes et transitions | Détection persistante, indépendante de l'affichage |
| analytics/definitions.py, Dagster 1.9.13 | Déclenchement manuel ou schedule | Enchaîne des étapes finies ; retries et historique | Pilotage, observabilité et reprise |
| Grafana OSS 11.6.0 | PostgreSQL candles | Affiche AAPL, MSFT, NVDA ; rafraîchit toutes les 5 s | Suivi des chandeliers |
| Streamlit 1.43.2, ui/app.py | Projections Gold et alertes PostgreSQL | Sélecteurs, cinq indicateurs, courbes, tables et clôture d'une alerte résolue | Analyse et consultation de l'historique |

Un **offset Kafka** est une position dans une partition, pas un identifiant métier. Deux messages à des offsets différents peuvent porter le même trade_id. Un **upsert** insère une clé absente ou remplace ses valeurs ; ici il ne rajoute pas le volume à chaque rejeu. Un **snapshot Iceberg** identifie une version validée d'une table. Le catalogue JDBC stocke les pointeurs ; les fichiers de données et métadonnées Iceberg restent dans MinIO.

### Données et fichiers à reconnaître

| Emplacement | Grain et champs utiles |
|---|---|
| Kafka trades | Une transaction JSON ; clé = symbol |
| bronze/json/trades/batch_date=AAAA-MM-JJ/<batch_id>.jsonl | Payloads bruts, une ligne JSON par message, doublons éventuellement conservés |
| bronze/parquet/trades/batch_date=AAAA-MM-JJ/<batch_id>.parquet | Données typées, coordonnées _topic/_partition/_offset, _ingested_at, _batch_id et _parse_error |
| bronze/manifests/trades/batch_date=AAAA-MM-JJ/<batch_id>.json | batch_id, row_count, json_key, parquet_key et next_offsets ; écrit en dernier |
| lake.silver.trades | Une transaction canonique par trade_id ; coordonnées Kafka source |
| lake.silver.candles | Une action et une minute ; OHLCV, notional, trade_count, dernier événement |
| lake.silver.rejected | Rejets analytiques avec provenance et motif |
| lake.gold.minute_metrics | Une action/minute : close, MA20/50, VWAP, log_return, référence et change_5m |
| lake.gold.daily_metrics | Une action/jour : rendement, volatilité, VWAP, couverture |
| lake.gold.correlation_daily, lake.gold.correlations | Statistiques par paire/jour puis corrélations consolidées |
| PostgreSQL candles et latest_prices | Service du streaming, pas source des calculs Gold |
| PostgreSQL gold_minute, gold_daily, gold_correlations | Projections du Gold Iceberg, avec source_snapshot et published_at |
| PostgreSQL alerts, alert_observations, alert_history, alert_state | Épisodes, observations, changements d'état et curseur d'évaluation |
| PostgreSQL lakehouse_files, lakehouse_dirty, lakehouse_runs | Manifestes traités, minutes à reprendre et journal des tâches |
| PostgreSQL lakehouse_publication, gold_serving_publication | Snapshots publiés, version de formule et curseurs |
| /state/v2 dans spark_state | Checkpoints, Parquet validés/rejetés, métriques des requêtes |

Le partitionnement Bronze utilise la date du **plus ancien événement valide du lot**, pas nécessairement le jour où l'on ouvre MinIO. Un batch qui traverse minuit peut donc être rangé sous la veille. Un ancien champ volume est accepté comme alias de quantity. Un trade_id est immuable : la première version canonique retenue gagne ; changer son prix avec le même identifiant n'est pas une correction métier prise en charge.

### Formules réellement implémentées

- OHLC : tri de (event_time, trade_id) pour départager des dates identiques ; High/Low par maximum/minimum.
- Volume = somme(quantity). Notional = somme(price × quantity).
- MA20 et MA50 = moyenne des 20/50 clôtures minute **observées** de la même action. Les 19/49 premières restent NULL. Une lacune peut allonger la durée réelle de la fenêtre.
- VWAP du jour = somme(notional) / somme(volume). La valeur minute est cumulative depuis minuit UTC. Les sommes sont calculées avant division ; la correction V3 évite un arrondi décimal prématuré.
- Daily Return = close du jour / close du jour UTC précédent − 1, seulement si cette veille existe. On ne substitue pas arbitrairement le dernier jour disponible à la veille.
- log_return = ln(close courant / close précédent), uniquement entre minutes consécutives. Daily Volatility = écart type échantillonnal de ces log-rendements du jour, avec au moins deux valeurs, sans annualisation.
- Corrélation = Pearson sur les log-rendements communs ; les statistiques par jour évitent de relire tout l'historique brut pour la consolidation.
- change_5m = close courant / close de la minute exactement cinq minutes avant − 1. Sans référence exacte : NULL. Une alerte apparaît si abs(change_5m) > 0.05 ; exactement +5 % ou −5 % ne suffit pas.

Exemple exact du test scripts/test_spark.py : a=(100,4) et b=(101,3) à la même seconde, c=(98,1) à +20 s, z=(103,2) à +40 s. Le tri par identifiant départage a avant b. Open=100, High=103, Low=98, Close=103, Volume=4+3+1+2=10, Notional=400+303+98+206=1007, VWAP=100.7. Un second message z ne doit pas rajouter 2 au volume. Les vraies valeurs d'une minute de production se recalculent sur toutes ses transactions, pas sur ces seuls quatre événements de test.

### Ce que le dépôt ne réalise pas, et les écarts à expliquer

| Consigne ou point | Constat réel / conséquence |
|---|---|
| Ancien Word : Gold dbt/DuckDB et Silver issu du streaming | Remplacés par les consignes ultérieures : Spark SQL/Iceberg depuis Bronze. dbt/ et soda/ restent historiques, inactifs |
| Ancienne architecture avec Hive | Aucun service Hive actif ; catalogue JDBC dans la base finance existante |
| Alertes stockées dans Gold | Non : PostgreSQL obligatoire. Aucune table Iceberg d'alertes |
| Transactions fictives du Word | Noms d'entreprises réelles mais événements et prix entièrement simulés |
| Action hors des vingt symboles déclarés | Le streaming et Silver filtrent le registre SYMBOLS ; l'ajout d'un 21e ticker exige une évolution du contrat, pas seulement une option Streamlit |
| Contrôle de santé du Producer | L'asset observe latest_prices ; il ne prouve pas à lui seul que le Producer produit actuellement. Vérifier les confirmations et les dates |
| FIN-09 et FIN-10 | Latence de bout en bout et comparaison ClickHouse non implémentées ; aucun chiffre de latence n'est revendiqué |
| Calendrier boursier | Jours UTC et minutes disponibles, pas de calendrier officiel des séances, jours fériés ni ajustements de dividendes |
| Historique incomplet | NULL légitime pour certaines moyennes, références ou rendements ; is_complete signifie jour passé, pas 1 440 minutes complètes |
| Haute disponibilité | Un broker Kafka, un PostgreSQL et un stockage local ; pas de test HA ni de transaction distribuée PostgreSQL/Iceberg |
| Rétention Kafka | Au plus 24 h configurées, et limite de taille de 1 Gio pouvant réduire la durée effective ; un arrêt prolongé peut dépasser la rétention |
| Trade corrigé/supprimé | Pas de suppression métier ni modification d'un événement immuable ; seul le cycle résolue → clôturée est disponible dans Streamlit |
| Rapport et captures V1/V2 | Archives et PDF antérieurs ne prouvent pas l'état courant. Utiliser les documents V3 et refaire les captures datées |
| Nouveau démarrage après arrêt | La finalisation streaming attend une progression du temps d'événement ; arrêter le Producer peut laisser les dernières minutes provisoires |

## 2. Préparation reproductible : applications, ressources et terminaux

Utiliser Windows, PowerShell et Docker Desktop en mode conteneurs Linux. Prévoir environ 8 Go pour Docker sur une machine de 16 Go ; c'est une cible de la maquette, pas un benchmark garanti. Deux moteurs Spark et les services de base tournent ensemble. Les images locales doivent être construites ; un premier build peut demander réseau, espace disque et plusieurs minutes.

Ports Windows utilisés ici : Kafka 9092, API S3 9000, console MinIO 9001, Dagster 3000, Grafana 3001, Streamlit 8501. **PostgreSQL n'est pas exposé sur Windows**. Entre conteneurs : kafka:29092, postgres:5432, minio:9000, lakehouse:8090 et spark:8091. localhost dans un conteneur désigne ce conteneur, pas Windows.

Préparer :
1. Terminal A PowerShell dans le dossier du projet : Compose, scripts et sauvegarde des preuves.
2. Terminal B PowerShell, puis console PostgreSQL : requêtes SQL.
3. Terminal C facultatif : suivi des journaux ; Ctrl+C ferme les logs sans arrêter le projet.
4. Navigateur avec Grafana, MinIO, Dagster et Streamlit déjà ouverts pour l'oral.
5. Dossier artifacts/demo pour la trace JSON et artifacts/captures pour **tes nouvelles** captures. Ne pas photographier .env, PG_DSN, mots de passe ni clés S3.

Dans A :

~~~powershell
Set-Location "C:\M2\P1\Tech_Data\Projet\Marche_Financier_Kafka_Bronze\finance-kafka-bronze"
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
New-Item -ItemType Directory -Force artifacts/demo, artifacts/captures | Out-Null
~~~

Ne pas écraser .env. Les identifiants non secrets : Grafana admin, PostgreSQL utilisateur/base finance. Les mots de passe sont dans GRAFANA_PASSWORD, POSTGRES_PASSWORD ; MinIO utilise S3_ROOT_USER et S3_ROOT_PASSWORD. Un mot de passe changé dans .env n'actualise pas automatiquement celui d'une base déjà initialisée.

## 3. Parcours de vérification et captures

Les intitulés Objectif/Où agir/Actions/Résultat attendu/Interprétation/Capture/Légende/Explication orale/En cas d'échec sont répétés pour chaque vérification. Une légende n'est utilisable **que si le résultat est observé**. C01 à C21 sont des identifiants de preuves, pas des résultats garantis.

### C01 — Démarrer depuis l'arrêt avec configuration et volumes conservés

**Objectif :** Vérifier le moteur Docker, les ports, la configuration et le démarrage de ce projet.

**Où agir :** Terminal A PowerShell ; Docker Desktop ouvert en mode Linux. Ne pas arrêter les autres projets.

**Actions :** ~~~powershell
docker --version
docker compose version
docker info --format 'OS={{.OSType}} RAM={{.MemTotal}}'
Get-NetTCPConnection -State Listen -LocalPort 9092,9000,9001,3000,3001,8501 -ErrorAction SilentlyContinue | Select-Object LocalAddress,LocalPort,OwningProcess
docker compose config --quiet
docker compose up -d
docker compose ps -a
~~~
Si les images n'existent pas ou si le code a changé, utiliser une seule fois `docker compose up -d --build` à la place de la commande up précédente. Ne pas faire de pull arbitraire de versions nouvelles. Compose respecte les dépendances de démarrage. Attendre PostgreSQL/Kafka puis init et les sessions Spark.
Pour une actualisation explicite des seuls services concernés : `docker compose up -d --build producer spark lakehouse dagster streamlit`. Un restart seul ne reconstruit pas le code embarqué.

**Résultat attendu :** Services avec healthcheck healthy ; producer et consumer-bronze Up ; init Exited (0). minio n'a pas de healthcheck dans ce Compose. Les six volumes nommés sont conservés.

**Interprétation :** La plateforme est lancée ; ceci ne démontre pas encore que des transactions passent ni que les calculs sont justes. Une interface peut montrer de vieilles données.

**Capture :** Essentielle : ps -a, nom du dossier et date du relevé ; ne pas capturer la sortie complète de compose config contenant des secrets. Fichier C01_services.png.

**Légende :** Les services du projet finance-market sont démarrés avec les volumes conservés ; l'initialisation s'est terminée sans erreur.

**Explication orale :** Docker Compose lance les composants et leurs dépendances. Les données sont dans des volumes, ce qui permet de conserver l'historique après l'arrêt des conteneurs.

**En cas d’échec :** Erreur dockerDesktopLinuxEngine : démarrer Docker Desktop et attendre que le moteur Linux soit prêt. Port occupé : identifier OwningProcess et le projet concerné, sans le tuer. Changer un port seulement si nécessaire et adapter les adresses. Exited (137) indique SIGKILL, sans prouver à lui seul un manque mémoire ; consulter logs et docker stats. Ne jamais supprimer les volumes pour résoudre une erreur de démarrage.

### C02 — Prouver la génération et le débit confirmé

**Objectif :** Observer des confirmations Kafka et vingt symboles, pas seulement une cible configurée.

**Où agir :** Terminal A ; code app/producer.py et app/market.py ; Terminal C si suivi continu.

**Actions :** ~~~powershell
docker compose logs --tail 20 producer
docker compose logs -f --tail 5 producer
~~~
Lire les lignes DEBIT : confirme, fenetre, total_confirme, symboles_confirmes, erreurs. Fermer le suivi avec Ctrl+C. Les paramètres par défaut sont 220 événements/s, premier choc après 360 s, cycle de 300 s avec un symbole différent toutes les 15 s.

**Résultat attendu :** Plusieurs fenêtres stabilisées à au moins 200 confirmations/s, symboles_confirmes=20 et erreurs=0. Le démarrage et les ressources peuvent dégrader une fenêtre : relever la valeur observée.

**Interprétation :** Les callbacks prouvent la livraison au broker. Ils ne prouvent ni le débit de Spark ni la latence du graphique. Les chocs +8 % sont une simulation volontaire et tournent sur tous les symboles.

**Capture :** Essentielle : plusieurs lignes DEBIT et, si disponible, deux CHOC portant des symboles différents ; C02_producer.png.

**Légende :** Le producteur confirme la livraison Kafka de vingt symboles ; le débit mesuré figure dans les journaux.

**Explication orale :** Le générateur émet des prix simulés. Je vérifie le débit sur les confirmations Kafka, plutôt que sur le nombre d'appels d'envoi.

**En cas d’échec :** Aucune ligne : vérifier l'état du Producer et init. Erreurs de livraison : consulter Kafka et l'adresse interne kafka:29092. Débit sous 200 : laisser stabiliser, vérifier docker stats --no-stream et éviter plusieurs tests Spark simultanés.

### C03 — Vérifier le topic, les partitions et une transaction identifiable

**Objectif :** Prouver l'organisation Kafka et choisir la donnée suivie dans tout le parcours.

**Où agir :** Terminal A. Le script s'exécute dans un conteneur Producer temporaire avec les dépendances du projet.

**Actions :** ~~~powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic trades
$trace = docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" producer python /checks/demo_trace.py capture --symbol MSFT | ConvertFrom-Json
$trace | ConvertTo-Json -Depth 10 | Set-Content -Encoding utf8 artifacts/demo/trace.json
$trace | Select-Object topic,partition,offset,key,captured_at
$trace.event | Format-List
~~~
Le script lit seulement les nouveaux messages, ne valide aucun offset et n'interfère pas avec les consommateurs applicatifs.

**Résultat attendu :** Topic trades avec 6 partitions ; une transaction MSFT, une clé MSFT, un trade_id, un prix, une quantité et event_time UTC. Le fichier trace.json contient les octets d'origine et leur SHA-256.

**Interprétation :** La partition conserve un ordre de journal, pas une garantie générale de tri en event_time. La preuve porte sur la transaction capturée ; l'offset n'est unique qu'avec topic et partition.

**Capture :** Essentielle : description du topic puis coordonnées Kafka et JSON du même événement ; C03_kafka_trace.png. Conserver trace.json comme pièce technique.

**Légende :** La transaction identifiée par son trade_id a été observée dans le topic trades avec une clé conforme à son symbole.

**Explication orale :** Kafka répartit le journal en six partitions. Je conserve l'identifiant métier et les coordonnées du message pour retrouver la même donnée dans les couches suivantes.

**En cas d’échec :** Délai de capture dépassé : vérifier que le Producer génère des événements MSFT. JSON vide après ConvertFrom-Json : relancer la commande sans conversion pour lire l'erreur. Ne pas remplir un fichier d'exemple en le présentant comme une capture réelle.

### C04 — Vérifier les archives Bronze et le manifeste

**Objectif :** Retrouver la transaction Kafka dans MinIO et comprendre le commit du lot.

**Où agir :** Terminal A, puis navigateur http://localhost:9001 ; connexion avec les valeurs S3_ROOT_USER/S3_ROOT_PASSWORD de .env, hors capture.

**Actions :** ~~~powershell
docker compose logs --tail 12 consumer-bronze
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/artifacts/demo:/demo:ro" producer python /checks/demo_trace.py inspect /demo/trace.json --max-files 30 | Tee-Object -FilePath artifacts/demo/trace_inspection.txt
~~~
Attendre au moins un flush du consommateur, habituellement la borne de 30 s ou 10 000 messages, puis répéter si nécessaire. Dans MinIO, ouvrir bronze puis les chemins json/trades/, parquet/trades/ et manifests/trades/. Ouvrir le manifeste dont manifest_key est fourni par le script ; retrouver ses json_key/parquet_key et row_count.

**Résultat attendu :** Au moins une entrée bronze_matches contient le trade_id et les coordonnées Kafka d'origine. Le manifeste est un JSON avec row_count et next_offsets. Les journaux BRONZE indiquent batch, nombre de lignes et chemin Parquet.

**Interprétation :** Le manifeste écrit en dernier signale un lot complet pour Silver. next_offsets donne la prochaine position à lire, pas le dernier offset traité. Un objet brut sans manifeste ne suffit pas à prouver que le lot est engagé. La recherche du script est limitée aux 30 manifestes les plus récents.

**Capture :** Essentielle : manifeste ouvert et correspondance avec le trade_id affiché par inspect ; C04_bronze_manifeste.png. Masquer l'identité de connexion, pas les identifiants techniques de transaction.

**Légende :** Le message suivi est archivé dans un lot Bronze complet, identifié par un manifeste JSON.

**Explication orale :** Bronze conserve la preuve brute, y compris les éventuels messages malformés. Le consommateur ne confirme ses offsets qu'après l'écriture du JSONL, du Parquet et du manifeste.

**En cas d’échec :** bronze_matches vide : attendre le flush, examiner les logs puis augmenter --max-files pour une trace plus ancienne. Vérifier le bucket et la date du plus ancien événement du lot. Ne pas conclure à une perte sur une recherche bornée. Une erreur d'authentification peut venir de mots de passe .env modifiés après initialisation.

### C05 — Lire la structure du Parquet Bronze

**Objectif :** Distinguer le payload brut des colonnes typées et de provenance.

**Où agir :** Terminal A ; sortie inspect de C04, chemins dans MinIO.

**Actions :** Dans trace_inspection.txt, relever row.price, quantity, event_time, _topic, _partition et _offset. Vérifier qu'ils correspondent à trace.json. Le code app/storage.py définit en plus _ingested_at, _batch_id et _parse_error. Pour un contrôle général indépendant du seul trade suivi :

~~~powershell
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" producer python /checks/verify_runtime.py
~~~
Ce script échantillonne Kafka pendant environ 25 s, relit un Parquet et vérifie PostgreSQL ; il ne mesure pas la latence de bout en bout.

**Résultat attendu :** Prix décimal à quatre décimales, quantité entière, date UTC et provenance Kafka. row_count du manifeste concordant avec le Parquet dans verify_runtime.

**Interprétation :** Le Parquet est une représentation typée utile à Spark ; le JSONL préserve les octets métier. Un message invalide peut rester dans Bronze avec _parse_error, sans devoir contaminer Silver.

**Capture :** Facultative : colonnes du même événement dans la sortie inspect et row_count ; C05_bronze_parquet.png.

**Légende :** L'archive Parquet conserve les types métier et les coordonnées Kafka nécessaires à la traçabilité.

**Explication orale :** Les formats se complètent : JSONL conserve le contenu et Parquet facilite les traitements. Les coordonnées de provenance relient le fichier au journal Kafka.

**En cas d’échec :** Ne pas essayer d'afficher le Parquet comme du texte. Dépendances manquantes sous Windows : exécuter dans l'image Producer. Une archive ancienne peut employer volume ; le pipeline accepte cet alias.

### C06 — Vérifier les requêtes Spark Streaming et leur progression

**Objectif :** Prouver que Spark consomme et fait progresser ses micro-lots et son temps d'événement.

**Où agir :** Terminal A ; service spark, API interne 8091 et volume spark_state.

**Actions :** ~~~powershell
docker compose exec -T dagster python -c "import json,urllib.request;print(json.load(urllib.request.urlopen('http://spark:8091/health')))"
docker compose exec -T spark python3 -c "import pathlib,json;[(print(p.name,json.loads(p.read_text()).get('batchId'),json.loads(p.read_text()).get('numInputRows'),json.loads(p.read_text()).get('eventTime'))) for p in pathlib.Path('/state/v2/metrics').glob('*.json')]"
docker compose logs --tail 25 spark
~~~
Reprendre la commande des métriques après une nouvelle période d'ingestion. Les quatre noms de requêtes sont rejected, validated, live et final-postgres.

**Résultat attendu :** active=true ; batchId progresse et des micro-lots validated/live/final-postgres présentent des entrées. Un lot vide est normal. eventTime contient notamment la watermark si la requête l'expose.

**Interprétation :** L'API prouve l'activité des requêtes ; la progression des métriques prouve du travail. La watermark est fondée sur les dates des événements observés, pas sur une simple minuterie de deux minutes. Les rejets du streaming sont des Parquet sous /state/v2/rejected, distincts de Silver.rejected.

**Capture :** Essentielle : santé et deux relevés des métriques avec dates ; C06_spark_progression.png.

**Légende :** Les requêtes Structured Streaming sont actives et leurs micro-lots progressent avec le temps d'événement.

**Explication orale :** Spark valide les événements et déduplique les identifiants dans la fenêtre de rétention. Il calcule ensuite des bougies d'une minute et accepte un retard borné par la watermark de deux minutes.

**En cas d’échec :** API non publiée sur Windows : utiliser l'adresse interne depuis Dagster. Pas de métriques : attendre le premier micro-lot. Rechercher QUERY_TERMINATED et invalid_contract. Le contrat rejette notamment une clé différente du symbole, un prix/quantité invalide, une date sans fuseau ou plus de 30 s dans le futur par rapport à la date Kafka.

### C07 — Vérifier les bougies PostgreSQL et leur cohérence

**Objectif :** Contrôler la sortie effective du streaming et les clés métier.

**Où agir :** Terminal B : ouvrir psql, puis exécuter les blocs SQL. Conserver cette console pour la suite.

**Actions :** ~~~powershell
docker compose exec postgres psql -U finance -d finance
~~~

~~~sql
SET TIME ZONE 'UTC';
\pset null '<NULL>'
SELECT symbol,count(*) AS minutes,max(minute_start) AS derniere_minute
FROM candles GROUP BY symbol ORDER BY symbol;
SELECT symbol,minute_start,open,high,low,close,volume,notional,trade_count,is_final
FROM candles WHERE symbol='MSFT' ORDER BY minute_start DESC LIMIT 5;
SELECT count(*) AS incoherences
FROM candles WHERE low>open OR open>high OR low>close OR close>high OR volume<0;
SELECT symbol,minute_start,count(*) FROM candles GROUP BY symbol,minute_start HAVING count(*)>1;
SELECT symbol,event_time,price FROM latest_prices ORDER BY symbol;
~~~

**Résultat attendu :** Des bougies pour les actifs réellement ingérés ; zéro incohérence et aucune ligne de clé dupliquée. Les bougies récentes peuvent avoir is_final=false et les plus anciennes true.

**Interprétation :** Ces requêtes prouvent la persistance et les invariants vérifiés. La clé primaire empêche les doublons de bougies ; cela ne prouve pas à elle seule l'absence d'un double comptage interne des volumes, vérifié par les tests et le traçage Silver.

**Capture :** Essentielle : bougies MSFT, is_final et compte des incohérences ; C07_postgresql_bougies.png.

**Légende :** Les chandeliers minute sont persistés dans PostgreSQL, avec des clés uniques et des bornes OHLC cohérentes.

**Explication orale :** PostgreSQL sert les bougies au tableau de bord. Les upserts remplacent les agrégats calculés ; ils n'ajoutent pas une deuxième fois le volume en cas de rejeu.

**En cas d’échec :** Table inexistante : contrôler init et les migrations, sans réinitialiser la base. Pas de ligne récente : vérifier latest_prices puis Spark/Kafka. Ne pas comparer une bougie provisoire changeante à une référence figée sans fixer sa minute et son état.

### C08 — Relier Grafana aux données PostgreSQL

**Objectif :** Montrer trois chandeliers réellement alimentés par la base du streaming.

**Où agir :** Navigateur http://localhost:3001/d/finance-live ; Terminal B pour comparaison.

**Actions :** Se connecter avec admin et GRAFANA_PASSWORD. Ouvrir le dashboard Marché financier simulé. Montrer AAPL, MSFT, NVDA, la plage de dates et le refresh 5s. Choisir une minute MSFT visible et retrouver la même ligne dans candles avec la requête de C07. La source provisionnée s'appelle Finance PostgreSQL, UID finance-pg, serveur postgres:5432, base finance. Le fichier grafana/dashboards/finance.json utilise minute_start, open, high, low, close et volume.

**Résultat attendu :** Trois panneaux candlestick, sans Query error, avec des minutes qui existent en SQL. Après un long arrêt, élargir explicitement la plage au jour conservé si les nouvelles données ne sont pas encore arrivées.

**Interprétation :** La figure prouve une lecture et une représentation des bougies PostgreSQL. Un rafraîchissement de cinq secondes ne signifie pas une bougie de cinq secondes ni une latence garantie de cinq secondes.

**Capture :** Essentielle : les trois panneaux, la plage de temps et le réglage de rafraîchissement ; C08_grafana.png.

**Légende :** Grafana représente les chandeliers PostgreSQL de trois actions, avec un rafraîchissement configuré à cinq secondes.

**Explication orale :** Chaque chandelle résume une minute d'échanges. Le tableau relit la base toutes les cinq secondes pour actualiser aussi les bougies encore provisoires.

**En cas d’échec :** No data : vérifier la plage now-30m, l'heure UTC et les dates SQL. Query error : vérifier la source provisionnée et les identifiants. Une capture ancienne du 26 octobre ne constitue pas une preuve du fonctionnement actuel.

### C09 — Contrôler l'orchestration Dagster, les dépendances et les schedules

**Objectif :** Vérifier les assets finis, l'ordre du batch et son évaluation des alertes.

**Où agir :** Navigateur http://localhost:3000 ; Terminal A pour les commandes certaines de la CLI.

**Actions :** ~~~powershell
docker compose exec -T dagster dagster schedule list -f definitions.py
docker compose exec -T dagster dagster job execute -f definitions.py -j lakehouse_refresh
~~~
Avant la seconde commande, vérifier dans Runs qu'un traitement lakehouse n'est pas déjà en cours. Une exécution CLI directe n'est pas une soumission à la file ; les verrous protègent les writers, mais il faut éviter de multiplier les lancements. Dans Dagster, montrer les assets bronze → silver → gold → alerts, puis le run et ses logs. Le groupe streaming contient producer → kafka → streaming_candles → postgres_candles.

**Résultat attendu :** lakehouse_and_alerts_every_five_minutes et independent_streaming_supervision RUNNING, cron */5 * * * * ; end_of_day à 7 0 * * * UTC. Un run complet montre chaque étape réussie, avec volumes traités et résultat de publication.

**Interprétation :** Une planification présente mais STOPPED ne déclenche rien. Le worker analytique est distinct du streaming. Les services continus sont gérés par Compose ; les assets de supervision observent et se terminent.

**Capture :** Essentielle : graphe et run réussi, plus schedules ; C09_dagster.png. Éviter trois images répétant le même écran : une capture composée ou deux fichiers C09a/C09b suffisent.

**Légende :** Dagster enchaîne Bronze, Silver, Gold et alertes dans un run traçable ; les schedules de cinq minutes sont actifs.

**Explication orale :** Dagster ne reste pas bloqué dans la boucle continue de Spark Streaming. Il lance des tâches analytiques bornées et garde la trace des exécutions et des erreurs.

**En cas d’échec :** Connection refused au démarrage : attendre l'API lakehouse prête ; les assets ont deux retries espacés de 30 s. Vérifier les logs lakehouse et les états de lakehouse_runs. Un run queued peut attendre un autre traitement ; ne pas effacer l'historique pour vider la file.

### C10 — Vérifier Silver, les snapshots et le catalogue sans Hive

**Objectif :** Prouver que les archives sont transformées en tables Iceberg durables.

**Où agir :** Terminal B pour le catalogue JDBC ; Terminal A pour la lecture Spark indépendante ; MinIO pour les fichiers.

**Actions :** ~~~sql
SELECT table_namespace,table_name,metadata_location
FROM iceberg_catalog.iceberg_tables ORDER BY table_namespace,table_name;
SELECT count(*) AS manifestes_traites FROM lakehouse_files;
SELECT count(*) AS minutes_a_reprendre FROM lakehouse_dirty;
~~~

~~~powershell
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/verify_iceberg.py
~~~
Dans MinIO, les tables Silver sont sous silver/lakehouse-v2/trades, candles et rejected. Les tables Gold sont dans gold/lakehouse-v2. Conserver la sortie de la lecture indépendante.

**Résultat attendu :** Sept tables analytiques enregistrées (trois Silver, quatre Gold), aucune table d'alertes Iceberg. ICEBERG_V2_OK indique les comptes, l'unicité et la relecture de snapshots ; une bougie Silver est recalculée depuis les transactions canoniques.

**Interprétation :** Le catalogue JDBC conserve les pointeurs et MinIO les fichiers. Voir des Parquet dans un bucket ne démontre pas l'existence d'une table Iceberg lisible. Le test indépendant lit les tables avec une nouvelle session Spark ; il ne redémarre pas PostgreSQL/MinIO.

**Capture :** Essentielle : catalogue avec adresses MinIO et sortie ICEBERG_V2_OK ; C10_iceberg_silver.png.

**Légende :** Les tables Silver et Gold sont relues par un processus Spark indépendant grâce au catalogue JDBC, sans service Hive.

**Explication orale :** Iceberg apporte des versions de table et un mécanisme de commit. PostgreSQL conserve les pointeurs du catalogue, tandis que les données et métadonnées Iceberg restent dans MinIO.

**En cas d’échec :** Catalogue vide : exécuter lakehouse_refresh après ingestion Bronze. Accès S3 refusé : contrôler endpoint et identifiants sans les afficher en capture. Connexion JDBC refusée : vérifier PostgreSQL et le schéma. Éviter de lancer ce driver supplémentaire si la mémoire Docker est déjà saturée.

### C11 — Vérifier les cinq indicateurs Gold et leur publication PostgreSQL

**Objectif :** Distinguer calcul Spark, persistance Iceberg et copie transactionnelle vers la base de service.

**Où agir :** Terminal B psql, puis Terminal A pour la comparaison exhaustive.

**Actions :** ~~~sql
SELECT formula_version,published_at,row_counts,snapshots
FROM gold_serving_publication WHERE id=1;
SELECT symbol,count(*) AS minutes,count(ma20) AS ma20_disponibles,
count(ma50) AS ma50_disponibles,count(vwap_day) AS vwap_disponibles
FROM gold_minute WHERE source_snapshot IS NOT NULL GROUP BY symbol ORDER BY symbol;
SELECT day,close,daily_return,daily_volatility,vwap,minutes,is_complete,source_snapshot
FROM gold_daily WHERE symbol='MSFT' AND source_snapshot IS NOT NULL ORDER BY day DESC LIMIT 8;
SELECT minute_start,close,ma20,ma50,vwap_day,change_5m,source_snapshot
FROM gold_minute WHERE symbol='MSFT' AND source_snapshot IS NOT NULL ORDER BY minute_start DESC LIMIT 5;
~~~

~~~powershell
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/verify_all_symbols_runtime.py
~~~

**Résultat attendu :** Version gold-v3-vwap-precision ; données des vingt actifs après traitement ; identifiants source_snapshot. ALL_SYMBOLS_RUNTIME_OK compare toutes les valeurs minute/quotidiennes au snapshot Iceberg référencé dans une transaction de lecture cohérente.

**Interprétation :** Les valeurs financières ont été calculées par Spark, pas par les cartes Streamlit. row_counts rapporte les lignes publiées au dernier passage, pas toujours la taille totale. Le test strict suppose vingt actifs et des jours consécutifs disponibles pour le rendement ; un historique insuffisant peut le faire échouer sans rendre un NULL métier incorrect.

**Capture :** Essentielle : les cinq champs et la version de publication, plus la comparaison réussie si exécutée ; C11_gold_postgresql.png.

**Légende :** Les indicateurs calculés dans Gold Iceberg sont publiés dans PostgreSQL avec leur provenance de snapshot et contrôlés par comparaison.

**Explication orale :** Je sépare le calcul et sa consultation. Spark produit les métriques, puis une transaction publie leurs valeurs dans PostgreSQL ; Streamlit lit cet état validé.

**En cas d’échec :** Tables vides ou source_snapshot NULL : ancienne projection non actualisée ; attendre un run Gold réussi. Ne pas utiliser les seules lignes legacy comme preuve. MA manquante : compter les minutes. Daily Return absent : vérifier la veille UTC. Un écart précis de valeurs doit être investigué ; ne pas arrondir la requête pour le cacher.

### C12 — Expliquer chaque élément de Streamlit

**Objectif :** Relier les indicateurs, courbes et filtres à leur origine réelle.

**Où agir :** Navigateur http://localhost:8501 ; Terminal B pour la même action.

**Actions :** Choisir MSFT dans Action, puis une autre action. Montrer Daily Return et Daily Volatility affichés en pourcentage, VWAP, MA20 et MA50 en unités de prix. Montrer la date de la dernière minute et la couverture du jour, les courbes close/ma20/ma50/vwap_day, la table des dernières minutes, Indicateurs quotidiens, Corrélations des rendements minute et la date de publication. Comparer au SQL de C11, en tenant compte de l'arrondi visuel. Le lecteur limite à 1 440 minutes et 30 jours par action.

**Résultat attendu :** Le nom Action analysée correspond au sélecteur ; cinq cartes, courbes et tables sont cohérentes avec PostgreSQL. Une valeur insuffisante est Indisponible, pas zéro. Le pourcentage est la fraction SQL multipliée par cent.

**Interprétation :** Streamlit est un lecteur, pas le moteur financier. Ses métriques sont mises en cache 15 s, et le fragment se rafraîchit toutes les 5 s. On peut momentanément voir la publication précédente même si SQL vient de changer. Une corrélation ne prouve pas une causalité.

**Capture :** Essentielle : action choisie, cinq cartes, dates et source PostgreSQL ; C12_streamlit_indicateurs.png. Facultatif : seconde action ou table quotidienne plus détaillée.

**Légende :** Streamlit présente les cinq indicateurs de l'action sélectionnée à partir des projections PostgreSQL du Gold.

**Explication orale :** Le filtre change l'action analysée. Les valeurs ne sont pas inventées par le graphique : je peux retrouver les mêmes données et leur date de publication dans la base.

**En cas d’échec :** Message d'indisponibilité : consulter la publication, le run et les logs streamlit. Une table manquante avant migration peut empêcher l'affichage malgré un healthcheck HTTP réussi. Si l'interface paraît figée, attendre le cache, vérifier dates et rafraîchir la page ; ne pas confondre cache et panne de Spark.

### C13 — Prouver la couverture des alertes, leurs états et leur historique

**Objectif :** Montrer que toutes les actions sont évaluées et que les alertes respectent le seuil commun.

**Où agir :** Terminal B et partie basse de Streamlit.

**Actions :** ~~~sql
SELECT * FROM alert_symbol_coverage ORDER BY symbol;
SELECT symbol,status,count(*) FROM alerts WHERE origin='gold' GROUP BY symbol,status ORDER BY symbol,status;
SELECT alert_id,symbol,opened_at,last_observed_at,trigger_value,observed_value,threshold,status
FROM alerts WHERE symbol='MSFT' AND origin='gold' ORDER BY opened_at DESC LIMIT 5;
SELECT a.symbol,h.status,h.action,h.occurred_at,h.recorded_at
FROM alert_history h JOIN alerts a USING(alert_id)
WHERE a.symbol='MSFT' ORDER BY h.history_id DESC LIMIT 10;
SELECT source_cursor,source_snapshot,updated_at FROM alert_state WHERE name='gold_minute';
~~~
Dans Streamlit, montrer Couverture des alertes par action, Filtrer les alertes = Toutes les actions puis MSFT, Alertes actives et Historique des alertes. Une clôture réelle est facultative : choisir uniquement un épisode résolu et cliquer Clôturer l'alerte résolue. Cela modifie durablement son statut ; préférer le test isolé pour une répétition sans effet métier.

**Résultat attendu :** Des observations pour les vingt symboles après traitement. eligible_observations compte les références présentes, breaches les observations au-dessus du seuil, pas les épisodes. Plusieurs dépassements continus partagent un seul alert_id. Les origines legacy et gold sont distinguées.

**Interprétation :** Zéro alerte n'est pas forcément un défaut : il peut n'y avoir aucun dépassement. Une référence NULL n'est pas un zéro ni un retour dans le seuil. Les états réels sont active, resolved et closed ; les descriptions françaises n'en changent pas le stockage.

**Capture :** Essentielle : couverture multi-actions, une alerte hors AAPL et son historique ; C13_alertes.png. Si aucune alerte courante, montrer l'historique réellement présent et l'expliquer.

**Légende :** Les alertes sont évaluées par symbole, persistées dans PostgreSQL et suivies par un historique de transitions.

**Explication orale :** Une observation déclenche l'alerte si la variation dépasse strictement 5 %. L'épisode reste le même tant que la condition persiste, puis il est résolu au retour dans le seuil ; la clôture est une décision manuelle.

**En cas d’échec :** Aucune observation : vérifier Gold puis l'asset alerts. Observations mais références absentes : contrôler les minutes exactement cinq minutes avant. Attendre le premier choc de simulation (6 min) puis les délais Gold et Dagster, sans promettre une alerte à un instant précis. Ne pas changer le seuil pour obtenir une capture.

### C14 — Suivre la transaction capturée jusqu'aux sorties de service

**Objectif :** Relier les preuves des deux chaînes à un identifiant précis.

**Où agir :** Terminal A ; trace.json de C03. Maintenir le Producer actif pendant la maturation normale.

**Actions :** ~~~powershell
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/artifacts/demo:/demo:ro" producer python /checks/demo_trace.py inspect /demo/trace.json --max-files 30 | Tee-Object -FilePath artifacts/demo/trace_inspection.txt
~~~
Après clôture de la minute et délai Gold, lancer lakehouse_refresh si aucun run ne travaille déjà, puis répéter inspect. Relever trade_id, source Kafka, bronze_matches, streaming_candle et gold_minute. Dans Grafana, choisir la minute MSFT concernée ; dans Streamlit, retrouver l'action et sa dernière publication. Si la minute ne fait pas partie des dix dernières lignes affichées, utiliser le SQL plutôt qu'inventer un filtre de dates absent de Streamlit.

**Résultat attendu :** kafka.same_payload=true tant que l'offset est retenu ; au moins une copie Bronze ; une bougie PostgreSQL pour le symbole/minute ; une métrique Gold pour la même clé quand le traitement est terminé.

**Interprétation :** La bougie regroupe d'autres transactions : son volume n'est pas la seule quantité de l'événement suivi. L'identifiant individuel n'est pas stocké comme une ligne de transaction dans candles. La preuve de contribution canonique exacte est C15.

**Capture :** Facultative si les autres captures du même trade suffisent : même trade_id, minute et correspondances ; C14_trace_chaine.png.

**Légende :** La transaction suivie est reliée à son archive Kafka/Bronze et aux agrégats minute servis par les deux chaînes.

**Explication orale :** Je n'utilise pas une capture différente à chaque étape. Cet identifiant permet de relier les messages et les fichiers, puis la clé action/minute relie les agrégats à l'affichage.

**En cas d’échec :** Un champ null signifie sortie encore absente, pas réussite. Vérifier l'ordre et les dates des traitements. Si Kafka a expiré le message, l'archive peut rester correcte ; refaire une capture fraîche pour une preuve Kafka reproductible.

### C15 — Recalculer la bougie canonique du trade suivi

**Objectif :** Prouver la présence unique du trade dans Silver et le calcul de sa minute.

**Où agir :** Terminal A, nouvelle session Spark indépendante ; données de production en lecture seule.

**Actions :** ~~~powershell
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/artifacts/demo:/demo:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/demo_trace_iceberg.py /demo/trace.json 2> artifacts/demo/trace_spark.log | Tee-Object -FilePath artifacts/demo/trace_silver.txt
~~~
Le script cherche trade_id, exige une transaction canonique, relit toutes les transactions de sa minute et compare OHLC/volume/notional/trade_count. Il lit aussi la métrique du snapshot Gold publié si elle existe.

**Résultat attendu :** canonical_count=1, silver_verified=true, valeurs expected identiques à silver_candle. gold_minute peut être null si la publication n'a pas encore couvert la minute.

**Interprétation :** Cette comparaison est une preuve de calcul pour cette minute, pas de toutes les minutes. Des entrées trop tardives pour le streaming peuvent être reprises par le batch ; il ne faut pas forcer l'égalité entre toutes les bougies PostgreSQL streaming et Silver dans ces cas.

**Capture :** Facultative forte : identifiant, canonical_count, expected et silver_verified ; C15_trace_silver.png.

**Légende :** La transaction suivie est unique dans Silver et la bougie correspondante concorde avec son recalcul depuis les transactions canoniques.

**Explication orale :** Silver élimine les copies d'un même identifiant métier. Je recalcule la bougie à partir de ses transactions, ce qui est plus probant qu'une simple capture de graphique.

**En cas d’échec :** Aucune ligne canonique : vérifier le lot Bronze et le run Silver. Deux lignes indiquent un problème réel à investiguer. Échec de lecture : consulter trace_spark.log. Ne pas masquer un écart par une comparaison arrondie ou par une suppression des lignes.

### C16 — Opération contrôlée : rejouer exactement le même événement

**Objectif :** Démontrer un changement identifiable dans Kafka/Bronze et une identité canonique inchangée.

**Où agir :** Terminal A, après C14/C15 préparés ; commande volontaire d'écriture d'un doublon, jamais automatique au démarrage.

**Actions :** ~~~powershell
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" -v "${PWD}/artifacts/demo:/demo:ro" producer python /checks/demo_trace.py replay /demo/trace.json | Tee-Object -FilePath artifacts/demo/replay_delivery.txt
~~~
Le script vérifie le SHA-256 du payload enregistré et republie exactement ces octets, avec la même clé et le même trade_id. Il affiche le nouvel offset confirmé. Attendre le flush Bronze, exécuter un passage lakehouse si nécessaire, puis reprendre C14 et C15. Conserver les sorties avant/après dans des fichiers distincts.

**Résultat attendu :** Un nouvel offset Kafka pour le même SHA-256 ; au moins deux occurrences Bronze dans la zone recherchée si les deux copies y sont encore visibles ; toujours canonical_count=1 dans Silver et une seule clé de bougie. Sans autre donnée historique nouvelle, les agrégats de cette minute restent identiques.

**Interprétation :** Kafka ne déduplique pas deux appels intentionnels portant le même trade_id ; l'idempotence du client traite les retries de livraison. La règle métier Silver gère ici les copies. Pour un événement déjà hors watermark, le streaming peut le rejeter comme tardif : ce seul rejeu ne démontre pas sa déduplication dans la fenêtre, couverte par C17.

**Capture :** Essentielle pour l'opération contrôlée : ancien/nouvel offset, même trade_id/SHA-256 et canonical_count avant/après ; C16_rejeu.png.

**Légende :** Un doublon volontaire est conservé dans le journal brut tandis que Silver garde une seule transaction canonique.

**Explication orale :** Je provoque une opération contrôlée, sans changer le prix ni inventer une nouvelle transaction. Le journal brut enregistre le second message, mais l'agrégation canonique ne doit pas le compter deux fois.

**En cas d’échec :** Ne pas modifier trace.json à la main. Une erreur de checksum arrête l'envoi. Recherche Bronze bornée : augmenter max-files avant de conclure. Une minute de streaming encore provisoire peut changer à cause d'autres échanges ; comparer l'agrégat Silver canonique et une minute déjà mature. Ne jamais réinitialiser les offsets pour réussir cette preuve.

### C17 — Démontrer des calculs à valeurs connues

**Objectif :** Comparer les résultats du code actif à des références indépendantes et distinguer fixture et production.

**Où agir :** Terminal A ; tests isolés dans les images du projet, à exécuter un par un.

**Actions :** ~~~powershell
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" producer python -m unittest discover -s /checks/tests -v
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/test_spark.py
docker compose run --rm --no-deps -v "${PWD}:/checks:ro" -v "${PWD}/postgres:/schema:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/scripts/test_all_symbols_gold.py
~~~
Le test Streaming utilise a=(100,4), b=(101,3), c=(98,1), z=(103,2), rejoue z et teste un événement trop tardif puis la reprise. Le test multi-actions utilise vingt séries de 120 minutes sur deux jours, avec un saut simulé de +8 %, des volumes variables et de vraies tables Iceberg temporaires locales.

**Résultat attendu :** SPARK_TEST_OK : OHLC=100/103/98/103, volume=10, notional=1007, quatre échanges uniques. ALL_SYMBOLS_GOLD_OK : 20 actifs, 2 400 métriques minute, 40 quotidiennes, cinq métriques conformes aux références Python, 20 épisodes et rejeu stable. Les schémas PostgreSQL temporaires et le catalogue local de test sont nettoyés.

**Interprétation :** C'est une validation de calcul et de persistance, pas une mesure du débit de production. Le catalogue Hadoop local du test ne remplace pas le catalogue JDBC de production et ne stocke aucune donnée de production. Des tables synthétiques ne doivent pas être présentées comme des cotations réelles.

**Capture :** Essentielle pour le rapport : commandes, noms des tests et sorties de succès effectivement obtenues ; C17_tests_calculs.png. Ne pas couper une erreur située juste au-dessus d'un résultat partiel.

**Légende :** Les transformations sont vérifiées sur des jeux déterministes avec résultats attendus et références indépendantes.

**Explication orale :** Pour vérifier les calculs, je contrôle un petit jeu dont je connais le résultat. J'utilise ensuite vingt actions pour vérifier que les fenêtres et les épisodes restent indépendants par symbole.

**En cas d’échec :** Un FAILED ou un test ignoré n'est pas un succès complet. Vérifier le montage /schema pour le test multi-actions, la connexion PostgreSQL et les ressources. Ne pas lancer plusieurs Spark de test en même temps. Les dépendances ne sont pas nécessairement installées dans Python Windows.

### C18 — Vérifier la reprise sans supprimer les checkpoints

**Objectif :** Comparer des lignes finalisées avant et après un redémarrage contrôlé de Spark et Bronze.

**Où agir :** Terminal A ; au moins vingt bougies finalisées, Kafka actif et données conservées.

**Actions :** ~~~powershell
python scripts/check_restart.py
~~~
Le script lit vingt lignes finalisées, redémarre seulement consumer-bronze et spark, attend /state/v2/heartbeat et une API de santé confirmant les quatre requêtes actives, puis compare leurs valeurs et l'unicité des clés. Garder Producer et Kafka actifs. Vérifier ensuite C06 et les dernières dates de latest_prices. Conserver les logs du redémarrage.

**Résultat attendu :** RESTART_TEST_OK, vingt lignes identiques et aucune clé dupliquée. Les nouvelles données continuent d'arriver après reprise.

**Interprétation :** Le contrôle porte sur les vingt lignes comparées et la reprise des requêtes. Il ne prouve pas l'absence de perte sur tout l'historique. La rétention Kafka peut expirer des offsets pendant un long arrêt : failOnDataLoss=true doit alors révéler le problème au lieu de le masquer.

**Capture :** Facultative : sortie avant/après et contrôles de progression ; C18_reprise.png.

**Légende :** Après redémarrage contrôlé, les vingt bougies finalisées comparées conservent leurs valeurs et les requêtes reprennent.

**Explication orale :** Les checkpoints mémorisent la progression du streaming. Je redémarre les consommateurs avec leurs volumes pour vérifier la stabilité de données finalisées et la reprise de l'activité.

**En cas d’échec :** Le script a été aligné sur /state/v2/heartbeat ; une ancienne version lisait /state/heartbeat. Si le test échoue, lire les logs et laisser les volumes en place. Ne pas utiliser --force pour changer les offsets, supprimer le checkpoint ou passer failOnDataLoss à false pour faire disparaître l'erreur.

### C19 — Vérifier l'indépendance des deux chaînes

**Objectif :** Montrer que le batch lit Bronze et que le streaming n'a pas besoin du moteur analytique.

**Où agir :** Terminal A ; hors des sept minutes de démonstration, sans run analytique en cours.

**Actions :** ~~~powershell
docker compose stop spark
docker compose exec -T dagster dagster job execute -f definitions.py -j lakehouse_refresh
docker compose up -d spark
~~~
Puis, une fois le streaming revenu sain et aucun run batch en cours :
~~~powershell
docker compose stop lakehouse
docker compose logs --tail 5 producer
docker compose exec -T postgres psql -U finance -d finance -c "SELECT max(event_time) FROM latest_prices;"
docker compose up -d lakehouse
~~~
Relire latest_prices à deux instants pour démontrer une progression. Restaurer les deux services même si une commande de test échoue.

**Résultat attendu :** Le job analytique peut terminer sans spark. Sans lakehouse, Spark Streaming et les dates PostgreSQL continuent de progresser. Les services sont remis en route ensuite.

**Interprétation :** L'indépendance fonctionnelle n'est pas une indépendance totale d'infrastructure : PostgreSQL et MinIO restent partagés. Le schedule de supervision peut signaler l'arrêt volontaire de spark ; ce résultat est attendu pendant l'expérience et doit être annoncé.

**Capture :** Facultative : services arrêtés ciblés et preuves de progression de l'autre chaîne ; C19_independance.png.

**Légende :** Les deux traitements fonctionnent indépendamment l'un de l'autre, tout en partageant la source et certaines infrastructures.

**Explication orale :** J'arrête un seul moteur pour vérifier que l'autre chaîne continue. Ce test distingue l'indépendance des traitements d'une infrastructure à haute disponibilité, que ce projet ne revendique pas.

**En cas d’échec :** Un arrêt de PostgreSQL affecterait les deux chaînes et ne testerait pas cette propriété. Attendre les healthchecks après chaque relance. Ne pas interrompre des writers en plein commit pour simplifier la capture.

### C20 — Montrer un échec de qualité sans corrompre les données

**Objectif :** Prouver qu'un chandelier incohérent est rejeté par le contrôle actif.

**Où agir :** Terminal A ; DataFrame Spark local temporaire, aucun objet ni ligne de production modifié.

**Actions :** ~~~powershell
docker compose run --rm --no-deps -v "${PWD}/scripts:/checks:ro" --entrypoint /opt/spark/bin/spark-submit lakehouse --master local[1] --driver-memory 512m /checks/demo_quality_failure.py
~~~
Le script valide d'abord une bougie cohérente, puis remplace son low en mémoire par 105 alors que high=103 et open=100. Il attend explicitement un ValueError ; il échoue si le contrôle laisse passer la ligne.

**Résultat attendu :** EXPECTED_QUALITY_REJECTION, puis QUALITY_DEMO_OK. Le succès signifie que l'échec attendu a été détecté ; cela ne signifie pas que la ligne invalide a été acceptée.

**Interprétation :** Cette expérience vérifie assert_candles sur un exemple invalide, sans insérer un faux cours dans Kafka, Bronze, Silver ou PostgreSQL. Elle ne prétend pas exécuter les anciens tests Soda/dbt inactifs.

**Capture :** Facultative, utile pour la grille d'évaluation : cause du rejet et résultat final ; C20_rejet_qualite.png.

**Légende :** Le contrôle de cohérence OHLC rejette un plus bas incompatible avec le plus haut et l'ouverture, dans un test isolé.

**Explication orale :** Je provoque une erreur dans un jeu en mémoire pour vérifier le garde-fou. Les données de production restent intactes ; le résultat attendu est le rejet de la ligne.

**En cas d’échec :** Si aucune exception n'apparaît, c'est un échec du test. Une erreur de dépendance ou de démarrage Spark ne prouve pas le rejet métier ; ne pas la présenter comme EXPECTED_QUALITY_REJECTION.

### C21 — Conserver les preuves et arrêter proprement

**Objectif :** Terminer la démonstration sans effacer les données ni couper les autres projets.

**Où agir :** Terminal A et console psql du Terminal B.

**Actions :** Dans psql : `\q` pour quitter. Dans A :
~~~powershell
docker compose logs --tail 80 producer consumer-bronze spark lakehouse dagster streamlit > artifacts/demo/journaux_fin.txt
docker compose stop producer
docker compose stop
docker compose ps -a
~~~
Avant d'arrêter le reste, attendre la fin des runs Dagster actifs si possible ; le consommateur Bronze effectue aussi un flush à l'arrêt normal. Pour reprendre ultérieurement : `docker compose up -d`.

**Résultat attendu :** Conteneurs de ce projet arrêtés ; données et preuves conservées dans leurs volumes et fichiers. Une tâche interrompue devra être revue et rejouée si nécessaire au prochain démarrage.

**Interprétation :** stop ne supprime pas les volumes. Ne pas promettre que le dernier chandelier se finalise après arrêt du Producer : sa watermark ne progresse plus sans nouveaux événements. Un arrêt normal du consommateur vise à archiver son lot restant.

**Capture :** Facultative : état des services après arrêt, hors séquence d'oral ; C21_arret.png.

**Légende :** Le projet est arrêté proprement en conservant les archives, les tables, les checkpoints et les preuves collectées.

**Explication orale :** Je conserve les volumes pour que la prochaine exécution retrouve l'historique et les checkpoints. L'arrêt vise uniquement ce projet.

**En cas d’échec :** S'il reste un run actif, expliquer son état et le surveiller. Sont exclus du parcours : down -v, docker volume rm, prune --volumes, suppression de /state, DELETE/TRUNCATE des tables et reset d'offsets. Aucun nettoyage global de Docker.

## 4. Scénario complet et contrôlé : transaction MSFT et rejeu

### Préparation avant l'oral, sans valeur inventée

1. Démarrer et vérifier C01–C13. Attendre un run lakehouse_refresh complet.
2. Capturer une **nouvelle** transaction MSFT avec C03. Noter le vrai trade_id, la minute M, le prix p et la quantité q.
3. Suivre C04/C14 : même SHA-256 dans Kafka et mêmes coordonnées dans Bronze. Le volume attendu d'une copie brute supplémentaire lors du rejeu est **une ligne brute supplémentaire**, pas une nouvelle transaction métier.
4. Laisser le Producer actif pour faire progresser le temps d'événement. Attendre la clôture de M et les règles de délai, puis un run batch. Ne pas annoncer un délai fixe garanti : la file et le rattrapage peuvent allonger l'attente.
5. Exécuter C15. L'état initial contrôlé est : canonical_count=1 ; une ligne Silver pour (MSFT,M), expected=valeurs de silver_candle ; métrique Gold publiée. Copier cette sortie vers trace_silver_avant.txt. Relever en SQL la bougie finalisée si elle existe.
6. Si l'activité gêne la comparaison, **uniquement après cette maturité**, arrêter le Producer avec docker compose stop producer. Ne pas arrêter Kafka, Bronze, Spark ni les writers batch en cours. Les autres projets restent inchangés.
7. Exécuter C16 : seul le même événement est republié. Attendre son lot Bronze, puis un run analytique, sans relancer plusieurs jobs simultanés.
8. Refaire C14/C15 et conserver trace_silver_apres.txt. Résultats attendus : nouveau message Kafka confirmé ; copie brute supplémentaire dans la recherche ; toujours une transaction canonique, même bougie de la minute mature et pas de nouvel épisode d'alerte créé par la seule duplication.
9. Reprendre le Producer si arrêté : docker compose up -d producer. Vérifier les confirmations et la progression. Son redémarrage réinitialise les prix et les compteurs de simulation : un saut artificiel de série et de nouvelles alertes peuvent en résulter. Ne pas présenter ce redémarrage comme une continuité économique de cours réels.

Le scénario ajoute volontairement un message dupliqué au journal brut ; il ne supprime rien et ne change pas le prix d'une transaction existante. Les copies du même trade_id peuvent avoir des coordonnées Kafka différentes ; les coordonnées canoniques Silver restent celles de la première version retenue.

### Comment obtenir et expliquer les valeurs attendues

Pour le trade capturé, la contribution théorique est q au volume et p×q au notional, **une seule fois** dans Silver. La bougie attendue utilise toutes les transactions canoniques de la minute ; demo_trace_iceberg.py produit les sept valeurs expected et les compare réellement.

Pour une petite référence numérique entièrement déterministe, utiliser C17 : a=(100,4), b=(101,3), c=(98,1), z=(103,2). Le rejeu de z ne change ni Volume=10 ni Notional=1007. Le VWAP est 100.7. Le jeu de vingt actions vérifie ensuite les cinq indicateurs et les épisodes indépendants, sans injecter ses données synthétiques dans le topic de production.

Le scénario de production et le test isolé sont complémentaires. Le premier relie Kafka aux sorties réelles ; le second donne des nombres fixes faciles à vérifier. Aucun ne doit être présenté comme l'autre.

### Mise à jour et suppression

Le projet ne permet pas de réviser le contenu d'un trade_id ni de supprimer des transactions depuis l'interface. Ne pas fabriquer un exemple de correction de prix sous le même identifiant. La modification métier disponible est la clôture manuelle d'une alerte **déjà résolue** ; elle ajoute une transition et conserve l'historique. Le test scripts/test_alerts_v2.py couvre cette action dans un schéma temporaire si l'on veut éviter de modifier l'historique réel.

## 5. Les horloges et les délais

| Moment / fréquence | Implémentation et signification |
|---|---|
| Génération | Producer ciblé à 220/s ; confirmations mesurées par fenêtres de 5 s |
| event_time | Instant déclaré par la simulation, utilisé pour les fenêtres |
| Date Kafka | Date du message côté Kafka utilisée notamment pour rejeter un événement plus de 30 s dans le futur |
| _ingested_at | Instant où Bronze constitue sa ligne d'archive ; différent d'event_time |
| Flush Bronze | 10 000 messages ou 30 s par défaut ; aucun lot vide archivé |
| Micro-lots streaming | rejected/validated/live toutes les 5 s ; final-postgres toutes les 10 s |
| Grain OHLCV | Une minute ; même grain dans les deux chaînes |
| Watermark | Deux minutes de temps d'événement ; la finalisation dépend des événements observés, pas seulement de l'horloge Windows |
| Gold | Minutes closes avec délai de deux minutes ; sélection de dirty minutes jusqu'à maintenant moins trois minutes |
| Dagster analytique/alertes | Cron */5 * * * * UTC ; enchaîne Silver, Gold puis alertes ; exécution réelle éventuellement différée par la file |
| Batch historique | Au plus 200 manifestes par passage ; le rattrapage peut demander plusieurs runs |
| Fin de journée | Schedule end_of_day à 00:07 UTC ; même job analytique |
| Grafana | Interrogation toutes les 5 s ; plage initiale now-30m |
| Streamlit | Fragment toutes les 5 s ; cache des métriques 15 s ; maximum 1 440 minutes et 30 jours par action |
| source_snapshot / published_at | Version d'origine et date de publication ; ce n'est pas une date de transaction |
| Alertes | opened_at/last_observed_at décrivent les observations ; updated_at/recorded_at décrivent les traitements/enregistrements |

Le débit de confirmations n'est pas le délai jusqu'au graphique. Aucun compteur de latence transaction → affichage n'est intégré. Soustraire event_time à l'heure d'ouverture d'une page peut inclure un historique, une attente de clôture, un batch et un cache ; ce n'est pas une mesure fiable de latence de bout en bout. Pour toute mesure ad hoc, conserver les deux horodatages, la méthode, l'identifiant et préciser exactement quels segments sont inclus. Aucun bonus FIN-09 ne doit être annoncé sur cette seule soustraction.

## 6. Fichiers, commandes et limites des scripts de démonstration

| Script | Rôle | Effets |
|---|---|---|
| scripts/demo_trace.py capture | Capture un nouvel événement et ses octets depuis Kafka | Lecture seule ; pas de commit d'offset ; sortie JSON à sauvegarder |
| scripts/demo_trace.py inspect | Vérifie le payload Kafka, recherche les copies Bronze et lit les agrégats PostgreSQL | Lecture seule ; recherche limitée aux derniers manifestes ; ne prouve pas une absence globale |
| scripts/demo_trace.py replay | Réémet exactement la trace et confirme son nouvel offset | Écriture intentionnelle d'un doublon brut ; aucun nouvel identifiant métier |
| scripts/demo_trace_iceberg.py | Prouve l'unicité du trade et recalcule sa bougie Silver | Lecture seule via un nouveau Spark ; Gold éventuellement encore absent |
| scripts/demo_quality_failure.py | Démontre le rejet d'un low incohérent | Spark en mémoire, aucune ligne de production insérée |
| scripts/check_restart.py | Compare vingt bougies finalisées et reprend les consommateurs | Redémarre Spark/Bronze ; préserve les volumes ; chemin /state/v2 corrigé |
| scripts/test_runtime_replay.py | Réévalue deux fois Gold et une demande de task identique | Évalue réellement les alertes et met à jour leurs états si de nouvelles observations existent ; pas une simple lecture |
| scripts/test_serving.py, test_alerts_v2.py | Tests PostgreSQL isolés | Création/suppression uniquement de leur propre schéma temporaire |
| scripts/test_all_symbols_gold.py | Test intégré vingt actifs et cinq indicateurs | Catalogue Iceberg local et schéma PostgreSQL temporaires, nettoyés ; ne remplace pas le catalogue de production |
| scripts/verify_all_symbols_runtime.py | Compare chaque valeur des projections au snapshot Gold exact | Lecture seule, prérequis de couverture et d'historique stricts |
| scripts/verify_all_symbols_ui.cjs | Test navigateur de vingt choix et cinq cartes | Nécessite Node/Playwright et un navigateur configuré ; pas installé par le Compose |
| scripts/test_gold.py, dbt/, soda/ | Ancienne chaîne V1 | Historiques ; ne pas utiliser leurs succès comme validation du Gold Spark actuel |

Les commandes Docker du guide montent les scripts en lecture seule : aucun besoin de modifier l'image à chaque changement du guide. Les petits scripts de traçage sont nouveaux ; leur syntaxe, leur aide et l'intégrité de chargement ont pu être contrôlées localement, mais leur parcours avec les services reste à exécuter lorsque Docker sera disponible.

## 7. Diagnostic pratique

| Symptôme | Vérifier en premier | Ce que cela ne permet pas de faire |
|---|---|---|
| dockerDesktopLinuxEngine introuvable | Docker Desktop démarré, moteur Linux prêt ; docker info | Déduire que les volumes ont disparu |
| Port déjà occupé | Get-NetTCPConnection et projet propriétaire | Tuer un processus d'un autre projet au hasard |
| init Exited (0) | État attendu d'un service d'initialisation fini | Le traiter comme une panne |
| init échoue | logs init, PostgreSQL/Kafka prêts et MinIO accessible | Recréer la base en effaçant ses données |
| Source Grafana inaccessible | datasource Finance PostgreSQL, postgres:5432, base finance | Utiliser localhost:5432 depuis Grafana |
| Grafana vide | Plage de temps UTC, dernières dates de candles | Conclure que le pipeline n'a jamais fonctionné |
| Streamlit HTTP sain mais écran en erreur | logs streamlit, existence des tables et publication | Présenter un healthcheck comme preuve d'indicateurs corrects |
| Broker reçoit mais candles stagne | Quatre requêtes Spark, batchId, rejected et erreur Kafka | Modifier les offsets sans établir l'origine |
| Spark signale offsets expirés | Rétention Kafka en durée ET taille ; offsets des checkpoints | Supprimer le checkpoint comme procédure normale |
| Dernière minute reste provisoire | Watermark et nouveaux événements ; Producer actif | Attendre seulement une minuterie après avoir coupé la source |
| Bronze présent, Silver absent | Manifeste complet, lakehouse_files et run Silver | Lire un fichier sans manifeste comme lot validé |
| Gold vieux malgré un run | Publication, minutes éligibles, backlog et 200 fichiers/run | Promettre un délai égal au cron |
| Indicateurs NULL | Fenêtres 20/50, veille disponible, retours consécutifs et couverture | Remplacer les manques par des zéros |
| Aucune alerte d'une action | observations, eligible_observations, breaches et historique | Déclencher arbitrairement une alerte sous le seuil |
| Too many runs / verrou occupé | Runs actifs et lakehouse_runs ; un seul writer | Relancer dix jobs CLI pour accélérer |
| Reprise du Producer avec saut de prix | Réinitialisation de la simulation à son démarrage | L'interpréter comme une cote de marché réelle |
| Anciens tests ou anciennes captures | Version et date dans docs/evidence, archives et validation | Les afficher comme une preuve du 5 novembre |
| Problème Git après déplacement | Chemin du dépôt et métadonnées Git, sans réinitialisation | Confondre un problème Git avec le fonctionnement des conteneurs |

Commandes communes dans A :

~~~powershell
docker compose logs --tail 80 init
docker compose logs --tail 80 producer consumer-bronze
docker compose logs --tail 80 spark lakehouse
docker compose logs --tail 80 dagster streamlit grafana
docker stats --no-stream
~~~

Diagnostic SQL dans B :

~~~sql
SELECT stage,status,started_at,completed_at,left(error,250) AS erreur
FROM lakehouse_runs ORDER BY started_at DESC LIMIT 12;
SELECT count(*) AS fichiers_engages FROM lakehouse_files;
SELECT count(*) AS minutes_sales FROM lakehouse_dirty;
SELECT published_at,snapshots FROM lakehouse_publication;
SELECT formula_version,published_at,row_counts FROM gold_serving_publication;
~~~

## 8. Checklist courte

Avant présentation :
- Docker Linux prêt ; ports disponibles ; dossier Tech_Data correct ; .env conservé et caché.
- Services prêts, débit observé, dates récentes et un run complet réellement réussi.
- Préparer une trace MSFT et ses preuves C14/C15 ; capturer les valeurs avant/après pour C16.
- Ouvrir les quatre interfaces et le bon intervalle UTC Grafana.
- Vérifier cinq cartes, une alerte hors AAPL ou son historique, et les vingt lignes de couverture.
- Faire les tests et les captures AVANT l'oral ; les preuves préparées portent leur vraie date.
- Aucun build, test lourd, redémarrage ou rattrapage massif à lancer pour la première fois pendant les sept minutes.
- Reprendre Producer et moteurs éventuellement arrêtés pour des tests.

Après présentation :
- Sauvegarder uniquement les preuves sans secrets.
- Quitter psql et les suivis de logs.
- Attendre les runs actifs, arrêter proprement le projet ciblé.
- Conserver tous les volumes, checkpoints et fichiers de configuration.

## 9. Bilan honnête du 5 novembre 2026

| Contrôle réellement tenté / exécuté | Statut du présent travail |
|---|---|
| Inventaire du dépôt, code actif, Compose, dépendances, scripts et interface Streamlit | Inspectés sur disque |
| Cahier Word | Exigences financières et durée vérifiées : soutenance 15 min, démo 7 min, rapport 10–15 pages |
| docker compose config --quiet | Réussi, sans démarrer de conteneur |
| Résolution des services et ports via Compose | Réussie ; 11 services, six ports hôte configurés ; PostgreSQL et API Spark internes non publiés |
| Tests test_contracts.py | 2 réussis |
| Tests de génération de test_pipeline.py | 2 réussis |
| Test Bronze Parquet local | 1 ignoré, dépendances boto3/PyArrow absentes dans le Python Windows utilisé |
| docker compose ps -a | Échoué : moteur dockerDesktopLinuxEngine inaccessible ; aucun état actuel de service déduit |
| Aide des nouveaux scripts de trace | Réussie localement |
| Contrôle des supports | Réussi : 38 fichiers Python analysés, 21 étapes, 9 rubriques par étape, 21 lignes de captures et chemins des scripts présents |
| Intégrité de la trace | Chargement valide et rejet d’un checksum altéré vérifiés en mémoire ; pas d’accès Kafka/S3 dans ce contrôle |
| Syntaxe Python, contenu des supports, liens et intégrité du chargement de trace | Contrôlés localement ; détails dans le relevé de rédaction si présent |
| Kafka, MinIO, PostgreSQL, Spark, Dagster, Grafana, Streamlit et reprise | Non réexécutés dans cette session, faute de moteur Docker accessible |
| Rejeu de production et rejet qualité Spark nouvellement décrits | Non exécutés ; procédures prêtes pour la machine démarrée |
| Captures d'interfaces du présent guide | À réaliser localement ; aucune capture nouvelle inventée |

Les succès du 4 novembre — tests neuf unités, intégration vingt actions, comparaison exhaustive et navigateur — restent documentés avec leurs dates dans docs/VALIDATION_MULTI_ACTIONS.md. Le fait que le moteur soit inaccessible aujourd'hui ne prouve ni une régression du code ni une perte des données ; il empêche simplement une nouvelle vérification en fonctionnement.

Ne déclarer le parcours « entièrement vérifié le 5 novembre » qu'après avoir exécuté ses étapes nécessaires, relevé les résultats et pris les captures correspondantes.

Un premier essai du contrôle local d’intégrité avec un fichier temporaire a rencontré
un refus d’écriture de l’environnement. Le contrôle a ensuite été réalisé en mémoire
et a réussi. Ce diagnostic ne constitue pas un test du parcours Kafka ni un échec
du traitement financier. Aucun fichier de données du projet n’a été supprimé.

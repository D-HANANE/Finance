# Marché financier — ingestion Kafka vers Bronze

Ce socle remplace l'ingestion directe par un producteur Kafka et un consommateur
Bronze. Il prépare FIN-01 et FIN-02 et l'archivage des transactions pour FIN-04.
La conformité au débit se vérifie sur les confirmations Kafka dans les logs.
Il ne constitue pas à lui seul la plateforme financière complète : les sorties
Iceberg/PostgreSQL, dbt/Soda, Dagster, Grafana et Streamlit restent à intégrer.

## Démarrage Windows PowerShell

Extraire le dossier `finance-kafka-bronze` depuis le ZIP vers `C:\`.
Docker Desktop doit être démarré en mode conteneurs Linux. Si le TP1 utilise
encore les ports 9000, 9001 et 9092, arrêter sa pile depuis son dossier avec
`docker compose stop`, ou choisir d'autres ports dans le nouveau `.env`.
Les services de cette base ont leurs propres volumes ; les données du TP1
ne sont pas importées ni supprimées.

Exécuter ces commandes dans PowerShell :

```powershell
cd C:\finance-kafka-bronze
Copy-Item .env.example .env
notepad .env
docker compose up -d --build
```

Dans le Bloc-notes, personnaliser le nom d'utilisateur et le mot de passe S3,
puis enregistrer avant la dernière commande. Aucun environnement Python local
n'est nécessaire : les scripts tournent dans Docker.

La cible est 220 transactions/s au total, afin de garder une marge sur le
minimum de 200/s. Les 20 actions sont FIN01 à FIN20. Le topic `trades`, avec
6 partitions, et les buckets Bronze/Silver/Gold sont créés automatiquement.
Un conteneur `init` terminé avec le code 0 est normal.

## Contrôles

```powershell
docker compose ps -a
docker compose logs -f producer consumer-bronze
```

`Ctrl+C` ferme le suivi des logs ; les services continuent à tourner.
Tous les cinq secondes, le producteur affiche `confirme=.../s` et
`symboles_confirmes=20`. Le compteur confirme les livraisons acquittées par
Kafka, pas seulement les demandes d'envoi. Après le démarrage, observer
plusieurs fenêtres consécutives avec au moins 200/s et sans erreurs ; le
premier intervalle peut être plus faible. Un message `SOUS_200` signale que
l'exigence n'est pas atteinte pendant cet intervalle. Le débit réel dépend
de la machine ; il n'est pas garanti par la seule configuration.

Le consommateur affiche `BRONZE lignes=...` après environ 30 secondes, ou
10 000 messages. Il reste actif et n'a aucune limite de 40 objets.

Décrire le topic :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic trades
```

Afficher cinq messages avec leurs clés et partitions (groupe indépendant) :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic trades --from-beginning --max-messages 5 --property print.key=true --property print.partition=true
```

Consulter le retard du consommateur Bronze :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group bronze-trades-writer
```

## Stockage

Console : http://localhost:9001, identifiants choisis dans `.env`.

Dans le bucket `bronze` :

- `json/trades/batch_date=YYYY-MM-DD/*.jsonl` : messages métier JSON archivés,
  un message par ligne, sans transformation de leur contenu.
- `parquet/trades/batch_date=YYYY-MM-DD/*.parquet` : mêmes transactions sous
  forme tabulaire, avec coordonnées Kafka et métadonnées d'ingestion.
- `manifests/trades/batch_date=YYYY-MM-DD/*.json` : objets et offsets d'un lot
  dont les écritures sont terminées. Le manifeste est publié en dernier.

`batch_date` est la date UTC du plus ancien événement du lot, choisie pour
stabiliser son chemin lors d'un rejeu identique. Ce n'est pas une partition
stricte par date de chaque ligne : un lot peut traverser minuit. La vraie
date d'arrivée est `_ingested_at`. Chaque ligne conserve aussi `_topic`,
`_partition`, `_offset` et `_batch_id`. `price` est un décimal (18,4),
`event_time` un timestamp UTC à la microseconde, lisible par Spark 3.5.
Une transaction correspond à une ligne, pas à un objet S3.

Le JSONL garde le brut. Parquet prépare les relectures analytiques. CSV n'est
pas ajouté, car il ne serait pas nécessaire à cette base finance. Si un
consommateur strict des lots est requis, utiliser les manifestes ; un glob
sur tous les fichiers Parquet peut aussi voir un lot sans manifeste.

## Reprise et limites

Les offsets sont validés après les trois écritures. La garantie est
**at-least-once**, pas exactly-once. Une erreur arrête le consommateur ;
Docker le redémarre et les offsets non validés sont relus. Un lot rejoué
avec exactement les mêmes messages conserve son identifiant ; si le
découpage des lots change au redémarrage, des doublons restent possibles.
Les traiter en Silver par `(topic, partition, offset)` ou `trade_id`.
Le watermark ne réalise pas à lui seul cette déduplication.

Un changement de prix d'une action dans la simulation n'est pas une mise à
jour d'une ancienne transaction : chaque `trade_id` identifie un nouvel
échange. Ne pas dédupliquer sur `symbol`, ce qui supprimerait presque tous
les échanges. Un choc de +8 % est injecté sur FIN01 après 360 secondes,
puis toutes les 300 secondes. Son affichage dans les logs ne prouve pas
encore FIN-08 : Spark et Streamlit doivent ensuite calculer et afficher
l'alerte sur la variation réellement mesurée sur cinq minutes.

Kafka conserve localement ses journaux, avec rétention configurée à 24 h
ou environ 1 Gio par partition (selon le premier seuil atteint, avec
nettoyage par segments). Ce n'est pas l'archive longue durée ; Bronze
la conserve. Le stockage grandit tant que le flux tourne. Pour arrêter
les événements, utiliser `docker compose stop producer`, attendre que
le retard Bronze revienne à zéro, puis `docker compose stop`.
Ne pas utiliser `docker compose down -v` si les volumes doivent être gardés.
Cette configuration mono-broker, sans TLS, est une maquette locale.
L'image SILO reprend celle utilisée dans les TP ; son tag `latest` devra
être figé par digest après validation pour une exécution reproductible.

## Raccordement à Spark et au reste du projet

| Client | Adresse Kafka |
|---|---|
| Scripts et Spark dans ce même réseau Compose | `kafka:29092` |
| Script ou Spark sur Windows hôte | `localhost:9092` |
| Spark dans une VM Ubuntu distincte | IP de l'hôte à configurer comme adresse annoncée, à la place de localhost |

Pour un Spark déjà dans un autre Compose, fusionner les services dans le
Compose du projet ou relier explicitement les réseaux. `kafka:29092` n'est
résolu que dans le réseau Docker partagé. Si un MinIO de projet existe,
réutiliser ce service et ses identifiants ; ne pas lancer un deuxième service
sur les mêmes ports. Ne pas remplacer le Compose complet existant par celui-ci.

`examples/spark_candles.py` fournit `build_candles(spark, bootstrap)` pour
préparer les chandeliers d'une minute, avec un watermark de deux minutes.
Le connecteur `org.apache.spark:spark-sql-kafka-0-10_2.12:<version Spark>` doit
être chargé avant de créer la session (par exemple 3.5.5 pour Spark 3.5.5 et
Scala 2.12). Adapter le suffixe Scala à l'installation réelle.

Exemple dans le job/notebook existant, depuis la racine du projet :

```python
from examples.spark_candles import build_candles

candles = build_candles(spark, bootstrap="kafka:29092")
query = (candles.writeStream.outputMode("update")
         .format("console").option("truncate", False)
         .option("checkpointLocation", "/chemin/persistant/checkpoints/candles-console")
         .trigger(processingTime="5 seconds").start())
# Après observation : query.stop()
```

Cet exemple calcule et affiche des chandeliers ; il n'écrit pas encore
dans Iceberg ou PostgreSQL. Pour PostgreSQL en direct, faire un upsert
par `(symbol, minute_start)` à chaque micro-lot. Pour Iceberg : soit
un `MERGE` de ces mises à jour, soit un flux append de fenêtres finalisées
après passage du watermark. Un append des sorties update créerait des
doublons. Donner à chaque requête un checkpoint persistant distinct.

Les prix open/close suivent l'heure d'événement, départagée par trade_id,
et non l'ordre d'arrivée Spark. `notional = somme(price * volume)` est
conservé pour un VWAP exact par `somme(notional) / somme(volume)`.
Spark lit `trades` indépendamment du groupe Bronze et suit ses offsets
dans son checkpoint : ne pas lui imposer le groupe `bronze-trades-writer`.
Hive Metastore référence ensuite les tables Iceberg ; il ne remplace ni
Kafka ni le stockage des transactions.

FIN-03 à FIN-08 restent à intégrer et à vérifier dans la plateforme :
Silver Iceberg, base PostgreSQL, indicateurs Gold dbt/DuckDB, contrôles
Soda, planification Dagster, Grafana et alertes Streamlit.

## Tests

Sans dépendances, les tests du générateur fonctionnent et le test Parquet
est ignoré explicitement. Dans l'image Python, exécuter tous les tests :

```powershell
docker compose run --rm -v "${PWD}/tests:/app/tests:ro" --no-deps producer python -m unittest discover -s tests -v
```

Ou, avec Python 3.12 local et les dépendances installées :

```powershell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Ces tests ne constituent pas une mesure du débit ni une validation de
l'ensemble Docker. Vérifier aussi les logs et les objets dans MinIO.

Références techniques :
- https://kafka.apache.org/38/getting-started/docker/
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html

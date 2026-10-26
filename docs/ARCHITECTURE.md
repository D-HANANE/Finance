# Architecture et décisions

## Autorité et existant

Le dépôt initial contenait génération, Kafka et Bronze. L'exemple Spark n'était
pas déployé. PostgreSQL, Iceberg, Hive, Gold et les interfaces étaient absents.
Le test du choc et le README visaient encore FIN01. Les obligations viennent
du sujet 4 ; les règles IOT présentes dans le document ne s'appliquent pas.

## Livraison et ordre

Le producteur utilise `symbol` comme clé, avec six partitions fixes, idempotence
et `acks=all`. Kafka ordonne dans une partition ; le mono-broker ne tolère pas
sa perte. Changer le nombre de partitions changerait le routage des symboles.
OHLC dépend de `(event_time,trade_id)`, indépendamment de l'arrivée.
Un identifiant dupliqué doit représenter le même événement. En cas de contenu
conflictuel, la première version observée gagne ; le conflit viole le contrat.

Bronze écrit JSONL, Parquet et manifeste avant le commit synchrone des prochains
offsets. Le manifeste sert au vérificateur et à l'audit. Même lot rejoué : mêmes
clés ; découpage différent : doublons possibles. Garantie **at-least-once**.
Aucune transaction distribuée S3/Kafka ni garantie exactly-once globale.

## Requêtes et watermark

Quatre requêtes persistantes tournent dans Spark local avec deux threads :

1. Rejets de contrat vers Parquet, checkpoint `rejected`.
2. Validation puis `dropDuplicatesWithinWatermark(trade_id)`, watermark deux
   minutes, sink Parquet natif, checkpoint `validated`.
3. Lecture de ce Parquet, fenêtres minute avec watermark deux minutes,
   sortie update vers PostgreSQL, checkpoint `live`.
4. Même source et agrégation, sortie append de fenêtres finalisées,
   MERGE Iceberg puis publication finale PostgreSQL, checkpoint `silver`.

Les checkpoints sont sous `/state/checkpoints`. Séparer déduplication et
agrégation évite de chaîner deux opérateurs avec état en mode update. Le sink
Parquet natif possède son journal de fichiers engagés, utilisé par la source.
Le volume `/state` est indispensable. Cette zone locale n'est pas purgée
automatiquement ; surveiller sa croissance. Les archives durables sont en S3.

Deux minutes tolèrent les petits retards réseau. Le watermark dépend des heures
reçues, pas de l'horloge murale. Sans nouvelles données, il n'avance pas. Les
deux étapes peuvent ajouter du délai : aucune promesse d'une finalisation
exactement deux minutes après l'horloge réelle. Les données tardives admises
corrigent la fenêtre provisoire. Les abandons hors borne sont comptés dans
`numRowsDroppedByWatermark`, conservés dans `/state/metrics`, et restent auditables
en Bronze. Une fenêtre finale n'est pas corrigée automatiquement hors borne.

## Iceberg et service

Hive conserve les pointeurs des tables dans la base PostgreSQL `metastore`.
Hive 4.0.0 est retenu : 4.0.1 a supprimé l'appel Thrift get_table utilisé par
le client de Spark 3.5. Le namespace administratif a un chemin local HMS ;
la table Iceberg possède une localisation S3 explicite.
Les chandeliers, manifests et metadata Iceberg sont dans `s3://silver/warehouse`.
Spark utilise S3FileIO et l'AWS SDK v2 du bundle Iceberg. Le serveur Hive vérifie
aussi les chemins externes : son Hadoop 3.3.6 et son SDK AWS v1.12.367, déjà inclus
dans l'image, sont activés uniquement dans ce conteneur. Les deux piles Java ne
sont pas mélangées dans le processus Spark. La table est partitionnée
par jour ; sa clé logique est `(symbol,minute_start)`. Seules les fenêtres
finalisées entrent dans Iceberg. Un MERGE rend chaque lot rejouable.

PostgreSQL contient les valeurs absolues OHLCV et des clés primaires. Les upserts
n'incrémentent jamais un compteur existant. Un writer provisoire ne remplace
jamais une ligne finale. Un verrou transactionnel sérialise les publications.
Les derniers prix avancent selon `(event_time,trade_id)` et ne régressent pas
avec un événement tardif. La transaction calcule aussi les alertes.

Le callback final exécute MERGE Iceberg, PostgreSQL puis export. Un échec empêche
le commit du micro-lot Spark et provoque un rejeu. Les effets sont idempotents
par clé ; il subsiste un intervalle de désynchronisation entre destinations.
Les volumes Kafka/Spark/PostgreSQL/MinIO doivent être préservés ensemble.

## Gold et orchestration

Chaque micro-lot final non vide exporte une lecture de la table Iceberg via son
catalogue vers un fichier Parquet complet. Un remplacement atomique publie le
pointeur `/exchange/silver.json` seulement après la fin de l'écriture. Un glob
sur les fichiers data Iceberg serait incorrect : anciennes versions et suppressions.
Dagster capture une seule version du pointeur pour tout le build. Les anciennes
générations restent au moins 24 h pour les lecteurs en cours.

Un verrou exclusif évite deux writers DuckDB. Les modèles sont reconstruits
intégralement, choix adapté au faible grain minute et aux journées courantes
encore modifiées. dbt puis Soda doivent réussir avant la publication atomique
des tables Gold dans PostgreSQL. Les interfaces ne lisent pas le DuckDB ouvert
en écriture. Dagster planifie toutes les cinq minutes et à 00:05 UTC, conserve
l'historique et effectue deux relances. Un rapport CSV est produit au même run.
DuckDB est fixé à 1.0.0 : Soda Core 3.5.6 impose une version inférieure à 1.1.
Soda utilise un environnement Python distinct de dbt, avec la même version du
moteur pour lire le même format de fichier.

## Alertes

À chaque mise à jour live, le dernier prix T est comparé au dernier close dont
`last_event_time <= T-5 minutes` et `>= T-6 minutes`. C'est une observation réelle,
sans interpolation, proche des cinq minutes. Sans référence : aucune alerte.
Le seuil est `abs(price/reference_price - 1) > 0.05`, en fraction.

L'évaluation porte sur les derniers prix de chaque micro-lot ; un pic qui revient
avant la publication peut être manqué. Cette limite est explicite. Les alertes
ont une clé symbole/créneau de cinq minutes et un délai minimal de cinq minutes
depuis la précédente. Le rejeu ne les duplique pas. Une correction tardive peut
modifier une référence future ; les alertes déjà émises ne sont pas réécrites.

# Architecture active version 2

Les précisions utilisateur remplacent les parties devenues incompatibles du cahier Word
et du schéma générique : Spark SQL construit Gold Iceberg, et les alertes résident
exclusivement dans PostgreSQL.

## Deux chaînes

Le Producer et Kafka sont conservés. Le consommateur Bronze conserve son contrat
JSONL/Parquet/manifeste et ses commits après archivage. Les deux lecteurs restent indépendants.

Le service spark utilise les contrôles de contrat, la déduplication temporelle,
les fenêtres d'une minute et le tri (event_time,trade_id) existants. Il publie
uniquement les chandeliers courants/finalisés et les derniers prix dans PostgreSQL.
Grafana conserve ses trois panneaux et son rafraîchissement cinq secondes.
Un échec du worker lakehouse ne provoque pas l'arrêt de ces requêtes.

Le service lakehouse possède une autre session Spark et n'a aucune source Kafka.
Il lit les manifestes Bronze, puis les Parquet MinIO. Le registre lakehouse_files
ne marque un fichier traité qu'après les écritures Silver. La table de transactions
canoniques évite de compter deux fois les copies d'un trade_id réparties dans plusieurs lots.
Les minutes concernées sont recalculées avec les transactions canoniques complètes,
puis fusionnées par (symbol,minute_start). Les retards historiques ne sont pas supprimés.

## Catalogue sans service supplémentaire

JdbcCatalog utilise les transactions PostgreSQL existantes. Les métadonnées du
catalogue sont dans iceberg_catalog. Les fichiers Iceberg sont dans les buckets
silver et gold, sous lakehouse-v2. Le JDBC driver 42.7.5 est inclus dans l'image.
Spark 3.5.5/Scala 2.12 utilise Iceberg 1.7.2, son AWS bundle v2 et S3FileIO.
Bronze est lu avec S3A : hadoop-aws 3.3.4 et AWS SDK v1.12.262, compatibles avec le
Hadoop 3.3.4 déjà fourni par Spark. Les espaces de packages SDK v1/v2 sont distincts.
Les dépendances sont résolues lors de la construction, pas au démarrage.

## Gold incrémental et visibilité

Les minutes sales sont consignées dans lakehouse_dirty après Silver. Gold
recalcule les jours touchés, avec 49 observations avant et après pour les effets
des moyennes mobiles. Le cumul VWAP exige de recalculer le jour complet concerné.
Les calculs quotidiens incluent les jours voisins nécessaires au rendement.
Les corrélations maintiennent les sommes et produits par paire et par jour,
puis agrègent ces statistiques plutôt que tous les rendements minute.

Chaque métrique minute possède metric_updated_at. Un MERGE ne modifie cette date
que si les valeurs financières changent. Ainsi, une correction ancienne est visible
du traitement d'alertes sans imposer un rescan de tout Gold à chaque passage.
Un curseur inclusif est rejouable grâce aux empreintes des observations.

Les tables Iceberg ne peuvent pas être engagées ensemble dans une transaction
PostgreSQL. Une publication conserve donc les identifiants de snapshots Gold après
le succès de toutes les écritures et contrôles. L'API de lecture et l'évaluation
d'alertes utilisent ces versions publiées, jamais un mélange de tables en cours.

## Alertes et cycle de vie

Gold stocke le changement sur cinq minutes, pas des alertes.
La référence est le close de la minute exactement cinq minutes auparavant.
Sans cette minute, change_5m est NULL. Dagster compare sa valeur absolue à 0.05.
Le job parcourt toutes les observations nouvelles ou corrigées depuis le curseur.

Les états sont active, resolved et closed. Le retour dans le seuil sur une
observation valide résout l'épisode. Une référence absente laisse l'état inchangé.
La clôture est une décision manuelle explicite, car le cahier ne définit pas
de règle automatique de clôture. Aucun nouveau seuil n'a été ajouté.

Les observations et épisodes sont identifiés par clé métier et UUID déterministe.
Une transaction PostgreSQL et un verrou couvrent l'évaluation, les upserts,
l'historique et le curseur. Les épisodes des actifs concernés sont reconstruits
à partir des observations conservées pour intégrer les corrections tardives.
L'historique des changements n'est pas dupliqué lors d'un rejeu.

## Dagster et reprise

Les assets de streaming sont des observations finies des services continus.
Compose gère leur cycle de vie. Les assets lakehouse sont bronze -> silver ->
gold -> alerts, avec des tâches bornées dans un worker analytique distinct.
Le schedule de cinq minutes applique les dépendances dans cet ordre.
Le job alerts_refresh peut être lancé séparément sans recalculer les métriques.

Un seul run est lancé à la fois par la file Dagster. Les verrous PostgreSQL et
la sérialisation du worker protègent également les appels manuels concurrents.
Un request_id déjà réussi renvoie son résultat sans réexécution. Un échec conserve
les fichiers et minutes à reprendre. Les anciens runs et données ne sont pas effacés.

## Publication Gold PostgreSQL — version 3

Spark publie ses résultats Gold dans les tables existantes gold_minute, gold_daily et
gold_correlations. Les valeurs sont copiées depuis les snapshots validés ; PostgreSQL
ne recalcule pas les indicateurs. gold_serving_publication et lakehouse_publication
changent dans la même transaction, après les upserts. Streamlit lit ces projections
dans une transaction repeatable read. L’API Gold reste disponible pour les contrôles
et pour les observations utilisées par le moteur d’alertes.

La première publication de gold-v3-vwap-precision répare l’historique une seule fois ;
les suivantes publient les métriques minute modifiées, avec un curseur inclusif et des upserts.
Les agrégats quotidiens et corrélations sont de petites tables de synthèse.

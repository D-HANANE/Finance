# Scénario de présentation au professeur

Édition du 5 novembre 2026. Le cahier Word demande une **soutenance de 15 minutes dont 7 minutes de démonstration en direct**. Organisation proposée : 3 min pour le besoin et le schéma, 7 min pour le parcours direct, 3 min pour calculs/limites, 2 min pour les questions. Si le jury gère les questions autrement, adapter cette répartition sans dépasser la durée de démonstration demandée.

## Préparer avant d'entrer, pas pendant le chronomètre

1. Lire GUIDE_COMPREHENSION_VERIFICATION.md. Démarrer Docker Desktop Linux, se placer dans le dossier Tech_Data et conserver .env et les volumes.
2. Vérifier les services et les sorties ; une interface accessible ne suffit pas. Avoir un run lakehouse_refresh complet et une publication réelle.
3. Ouvrir les onglets Grafana, MinIO, Dagster, Streamlit. Être déjà connectée sans exposer les mots de passe.
4. Préparer le Terminal A pour Compose et la trace ; garder psql dans le Terminal B en UTC. Fermer les tests Spark de préparation.
5. Capturer une transaction MSFT réellement nouvelle, préparer trace.json, son manifeste et sa bougie Silver, puis les résultats avant/après d'un rejeu volontaire. Utiliser le même identifiant dans toutes les pièces.
6. Choisir une alerte hors AAPL réellement présente, active ou historique ; vérifier son état plutôt que promettre une alerte à l'heure de passage.
7. Préparer C17 et, si exécutée, C18 ou C20. Ne pas lancer un build, un rattrapage historique et plusieurs tests pour la première fois devant le professeur.
8. Reprendre les composants éventuellement arrêtés pour des expériences. Garder les nouvelles captures datées et étiquetées « données simulées ».

Le premier choc arrive après six minutes, puis il faut encore des données de référence et les traitements Gold/alertes. Ces délais imposent une préparation en amont. Les contrôles de services et les nouvelles captures n'ont pas été refaits lors de la rédaction du 5 novembre : le moteur Docker n'était pas accessible.

## 00:00–03:00 : expliquer le besoin et l'architecture

### Première minute : besoin métier

Dire :
« Notre plateforme suit vingt actions simulées. Une analyste veut voir les cours, les tendances et les mouvements inhabituels. Nous conservons les événements bruts pour pouvoir retrouver leur origine et recalculer les résultats. Les entreprises ont des noms réels, mais les transactions et les prix sont générés. »

Montrer l'objectif : chandeliers, cinq indicateurs, corrélations et alertes.

### Deux minutes suivantes : le schéma

Montrer le Mermaid du guide dans une prévisualisation Markdown déjà ouverte.

Dire :
« Les messages Kafka alimentent deux chemins. Spark Streaming calcule les bougies servies par PostgreSQL à Grafana. En parallèle, le consommateur Bronze archive les transactions dans MinIO. Un autre Spark lit ces archives, construit Silver puis Gold dans Iceberg. Les indicateurs Gold sont publiés dans PostgreSQL pour Streamlit ; Dagster évalue les alertes toutes les cinq minutes. »

Ajouter :
« Le catalogue Iceberg est JDBC dans PostgreSQL ; les fichiers restent dans MinIO. Il n'y a pas de Hive. Compose démarre les services continus et Dagster enchaîne des tâches analytiques finies. »

## 03:00–10:00 : démonstration en direct, exactement 7 minutes

Le chronomètre de démonstration repart à 0:00.

| Temps de démo | Écran et action | Phrase simple | Preuve |
|---|---|---|---|
| 0:00–0:40 | Terminal A : confirmations Producer, topic et trace C02/C03 | « Le Producer confirme ses livraisons. Le topic a six partitions ; la clé est le symbole. Voici notre transaction MSFT. » | Débit observé, vingt symboles, trade_id et coordonnées |
| 0:40–1:20 | MinIO : manifeste exact C04 | « Le même événement est archivé en JSONL et Parquet. Le manifeste vient en dernier ; les offsets ne sont confirmés qu'après l'archive. » | Même identifiant, objets liés, row_count |
| 1:20–2:20 | psql puis Grafana C07/C08 | « Spark regroupe les échanges par action et par minute. Les trois graphiques lisent PostgreSQL ; le rafraîchissement de cinq secondes ne change pas le grain minute. » | OHLCV et minute commune, plage UTC |
| 2:20–3:15 | Dagster : assets, run et schedules C09 | « Ce run lit Bronze, construit Silver, calcule Gold et traite les alertes. Il ne reste pas bloqué dans une boucle de streaming. » | Dépendances, résultat réel, cron actif |
| 3:15–4:05 | Terminal : catalogue et trace Silver C10/C15 | « L'événement est canonique une seule fois. La bougie est recalculée depuis les transactions de cette minute. Les pointeurs sont dans PostgreSQL, les fichiers dans MinIO. » | canonical_count=1, expected identique |
| 4:05–5:20 | SQL Gold puis Streamlit C11/C12 ; changer d'action | « Spark calcule les cinq indicateurs dans Gold ; PostgreSQL en reçoit la publication. Streamlit lit ces résultats sans refaire les calculs financiers. » | Cinq cartes, dates, snapshot et SQL |
| 5:20–6:20 | Streamlit : couverture et alerte hors AAPL C13 | « Le seuil est commun à toutes les actions. Plusieurs dépassements maintiennent le même épisode ; le retour dans le seuil le résout. » | Symbole, valeur, seuil, statut et historique |
| 6:20–7:00 | Avant/après du rejeu C16, préparé et daté | « Le même événement reçoit un nouvel offset brut ; Silver conserve une seule identité métier et ne double pas la bougie canonique. » | SHA-256 identique et comparaison avant/après |

### Si le jury veut voir une action d'écriture effectuée sur place

Après la présentation de la trace initiale, lancer C16 replay pour une transaction déjà mature. Il ne change ni le prix ni l'identifiant : il confirme un nouveau message dupliqué dans Kafka. Continuer la visite pendant le flush et le traitement analytique.

À 6:20, montrer le résultat courant seulement s'il est effectivement terminé. Sinon dire :
« L'envoi est confirmé dans Kafka ; le traitement analytique est encore en cours. Voici séparément le résultat du même protocole exécuté et daté pendant la préparation. »

Ne jamais attribuer une sortie préparée au message qui vient juste d'être envoyé. La file, le cron et la charge ne permettent pas de garantir tous les délais en sept minutes à partir d'un démarrage froid.

Le parcours normal garde le Producer actif. Pour figer la comparaison, suivre la procédure du guide après maturité de la minute, puis le reprendre. Son redémarrage réinitialise la simulation et peut provoquer un saut de prix : il faut l'expliquer.

## 10:00–13:00 : calculs, contrôles et limites

### 10:00–11:20 : démontrer un résultat connu

Montrer le jeu de scripts/test_spark.py :

| ID | Temps | Prix | Quantité |
|---|---|---:|---:|
| a | 00 s | 100 | 4 |
| b | 00 s | 101 | 3 |
| c | 20 s | 98 | 1 |
| z | 40 s | 103 | 2 |

Dire :
« Le tri date puis identifiant départage a et b. Open vaut 100, High 103, Low 98 et Close 103. Le volume vaut 4+3+1+2=10 et le notional 400+303+98+206=1007 ; le VWAP vaut donc 100,7. Rejouer z ne rajoute pas sa quantité. »

Montrer C17 seulement si le test a été exécuté. Le test multi-actions compare MA20, MA50, VWAP, rendement et volatilité à des références Python indépendantes et vérifie vingt épisodes séparés. Il ne mesure pas la performance de production.

### 11:20–12:10 : reprise ou échec de qualité

Montrer une preuve préparée C18 ou C20, plutôt que lancer deux traitements lourds.

Dire :
« Les checkpoints conservent la progression. Le test de reprise compare vingt lignes finalisées avant et après redémarrage. Le test qualité prend un low=105 incompatible avec high=103 et attend son rejet, sans modifier une ligne réelle. »

Si le test n'a pas été exécuté : « La procédure est prête, mais je ne la présente pas comme une preuve obtenue aujourd'hui. »

### 12:10–13:00 : limites concrètes

Dire :
« La maquette utilise un broker unique, un stockage local et des prix simulés. Une absence de veille ou de cinquante minutes donne une valeur indisponible, pas zéro. La rétention Kafka, les fenêtres, le batch et le cache influencent les délais. Nous ne revendiquons ni une haute disponibilité, ni ClickHouse, ni une latence de bout en bout mesurée. »

## 13:00–15:00 : questions probables

| Question | Réponse fidèle |
|---|---|
| Pourquoi deux chaînes ? | Le suivi continu et l'analyse historique ont des rythmes et des usages différents ; le batch lit Bronze indépendamment du moteur streaming |
| Pourquoi Kafka et MinIO ? | Kafka transporte et conserve un journal limité ; MinIO archive les bruts et les tables analytiques |
| Pourquoi JSONL et Parquet ? | Préservation du contenu et lecture typée efficace sont complémentaires |
| Un offset est-il un trade_id ? | Non : il s'agit d'une position dans une partition, distincte de l'identité métier |
| Pourquoi la watermark ? | Elle borne le retard dans le streaming et permet de gérer son état ; le batch peut reprendre des données plus anciennes |
| Pourquoi Iceberg si Streamlit lit PostgreSQL ? | Iceberg conserve le calcul analytique versionné ; PostgreSQL sert une projection cohérente et conserve les alertes relationnelles |
| Où est Hive ? | Il n'est pas utilisé ; JdbcCatalog stocke les pointeurs dans iceberg_catalog |
| Comment éviter les doublons ? | Identifiant canonique, clés action/minute et upserts absolus ; les contrôles de rejeu vérifient cette logique |
| Pourquoi une action n'a pas d'alerte ? | Vérifier observations, références et dépassements ; zéro dépassement n'est pas un symbole ignoré |
| Pourquoi MA50 absente ? | Moins de cinquante clôtures minute observées |
| Pourquoi Daily Return absent ? | Le jour UTC précédent n'a pas de clôture dans les données |
| Peut-on changer un prix avec le même ID ? | Le contrat le considère immuable ; ce n'est pas une modification métier prise en charge |
| Comment clôturer une alerte ? | Après résolution, avec l'action Streamlit qui conserve l'historique |
| Une corrélation implique-t-elle une causalité ? | Non |
| Refresh 5 s signifie latence 5 s ? | Non ; c'est une fréquence de lecture |
| Une capture du 4 novembre prouve quoi ? | Le résultat observé ce jour-là, pas un nouveau contrôle du 5 novembre |

## Si un composant tombe en panne

Ne pas réinitialiser devant le jury. Montrer le service, le message et la dernière sortie valide. Distinguer run en attente, cache, plage temporelle et moteur arrêté. Une preuve préparée peut être montrée avec sa vraie date et la mention explicite que le contrôle direct est indisponible.

## Checklist de sortie

Sauvegarder les preuves sans secrets, quitter psql et les suivis de logs, remettre les composants suspendus en route si l'on continue le travail. Sinon attendre les tâches en cours puis suivre C21 pour arrêter uniquement ce projet. Conserver tous les volumes et checkpoints.

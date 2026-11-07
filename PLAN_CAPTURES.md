# Plan des captures, preuves et légendes

Édition du 5 novembre 2026. Ce document prépare les captures à réaliser ; aucune nouvelle capture d'interface n'est revendiquée, car Docker Desktop était inaccessible lors de la rédaction.

Créer artifacts/captures dans le dossier réel du projet Tech_Data. Utiliser les noms C01_services.png, C02_producer.png, etc. En cas de deux vues indispensables, nommer C09a et C09b. Les commandes complètes et les neuf rubriques de chaque étape figurent dans GUIDE_COMPREHENSION_VERIFICATION.md.

## Tableau de captures

Les légendes sont utilisables seulement après observation du résultat.

| Numéro | Composant / vérification | Action précise | Preuve attendue | Légende proposée | Caractère |
|---|---|---|---|---|---|
| C01 | Démarrer depuis l'arrêt avec configuration et volumes conservés | Démarrer Compose avec volumes conservés ; vérifier ps et ports | Services prêts ; init Exited (0) | Les services du projet finance-market sont démarrés avec les volumes conservés ; l'initialisation s'est terminée sans erreur. | Essentielle |
| C02 | Prouver la génération et le débit confirmé | Lire les confirmations DEBIT et vingt symboles | Débit réellement confirmé, vingt symboles et erreurs relevées | Le producteur confirme la livraison Kafka de vingt symboles ; le débit mesuré figure dans les journaux. | Essentielle |
| C03 | Vérifier le topic, les partitions et une transaction identifiable | Décrire trades puis capturer une nouvelle transaction MSFT | 6 partitions, clé conforme, trade_id et coordonnées conservés | La transaction identifiée par son trade_id a été observée dans le topic trades avec une clé conforme à son symbole. | Essentielle |
| C04 | Vérifier les archives Bronze et le manifeste | Ouvrir le manifeste exact et retrouver le trade dans son Parquet | Même identifiant et offsets, row_count et objets liés | Le message suivi est archivé dans un lot Bronze complet, identifié par un manifeste JSON. | Essentielle |
| C05 | Lire la structure du Parquet Bronze | Lire les champs typés et la provenance | Types et coordonnées de provenance identiques | L'archive Parquet conserve les types métier et les coordonnées Kafka nécessaires à la traçabilité. | Facultative |
| C06 | Vérifier les requêtes Spark Streaming et leur progression | Lire /health et comparer deux relevés des métriques | Quatre requêtes actives et batchId en progression | Les requêtes Structured Streaming sont actives et leurs micro-lots progressent avec le temps d'événement. | Essentielle |
| C07 | Vérifier les bougies PostgreSQL et leur cohérence | Interroger candles et les contraintes OHLC | Données présentes, zéro incohérence et clés uniques | Les chandeliers minute sont persistés dans PostgreSQL, avec des clés uniques et des bornes OHLC cohérentes. | Essentielle |
| C08 | Relier Grafana aux données PostgreSQL | Comparer une minute SQL à un panneau Grafana | Trois panneaux et minute identique aux données SQL | Grafana représente les chandeliers PostgreSQL de trois actions, avec un rafraîchissement configuré à cinq secondes. | Essentielle |
| C09 | Contrôler l'orchestration Dagster, les dépendances et les schedules | Afficher dépendances, schedules et run complet | bronze → silver → gold → alerts ; schedules actifs | Dagster enchaîne Bronze, Silver, Gold et alertes dans un run traçable ; les schedules de cinq minutes sont actifs. | Essentielle |
| C10 | Vérifier Silver, les snapshots et le catalogue sans Hive | Lister les pointeurs JDBC et relire les tables/snapshots | Sept tables, snapshots relus et aucun Hive | Les tables Silver et Gold sont relues par un processus Spark indépendant grâce au catalogue JDBC, sans service Hive. | Essentielle |
| C11 | Vérifier les cinq indicateurs Gold et leur publication PostgreSQL | Comparer les cinq indicateurs PostgreSQL aux snapshots | Valeurs et provenance de publication contrôlées | Les indicateurs calculés dans Gold Iceberg sont publiés dans PostgreSQL avec leur provenance de snapshot et contrôlés par comparaison. | Essentielle |
| C12 | Expliquer chaque élément de Streamlit | Choisir une action puis une autre et lire les cinq cartes | Cinq cartes, action, dates et source PostgreSQL | Streamlit présente les cinq indicateurs de l'action sélectionnée à partir des projections PostgreSQL du Gold. | Essentielle |
| C13 | Prouver la couverture des alertes, leurs états et leur historique | Afficher couverture, alerte hors AAPL et transitions | Couverture par symbole, seuil commun et historique | Les alertes sont évaluées par symbole, persistées dans PostgreSQL et suivies par un historique de transitions. | Essentielle |
| C14 | Suivre la transaction capturée jusqu'aux sorties de service | Rassembler la trace du même trade jusqu'aux agrégats | Correspondances d'identifiants et de minute | La transaction suivie est reliée à son archive Kafka/Bronze et aux agrégats minute servis par les deux chaînes. | Facultative |
| C15 | Recalculer la bougie canonique du trade suivi | Recalculer la bougie depuis les transactions canoniques | canonical_count=1 ; expected identique à Silver | La transaction suivie est unique dans Silver et la bougie correspondante concorde avec son recalcul depuis les transactions canoniques. | Essentielle |
| C16 | Opération contrôlée : rejouer exactement le même événement | Réémettre exactement le payload et comparer avant/après | Nouvel offset brut, même SHA-256 et identité canonique | Un doublon volontaire est conservé dans le journal brut tandis que Silver garde une seule transaction canonique. | Essentielle |
| C17 | Démontrer des calculs à valeurs connues | Exécuter les fixtures déterministes et multi-actions | OHLCV connu ; vingt séries et références indépendantes | Les transformations sont vérifiées sur des jeux déterministes avec résultats attendus et références indépendantes. | Essentielle |
| C18 | Vérifier la reprise sans supprimer les checkpoints | Redémarrer Spark/Bronze avec leurs volumes puis comparer | Vingt lignes finalisées inchangées et reprise | Après redémarrage contrôlé, les vingt bougies finalisées comparées conservent leurs valeurs et les requêtes reprennent. | Facultative |
| C19 | Vérifier l'indépendance des deux chaînes | Arrêter un seul moteur et observer l'autre | Progression de l'autre chaîne malgré arrêt ciblé | Les deux traitements fonctionnent indépendamment l'un de l'autre, tout en partageant la source et certaines infrastructures. | Facultative |
| C20 | Montrer un échec de qualité sans corrompre les données | Rejeter un low incohérent en mémoire | Rejet métier attendu, aucune donnée réelle modifiée | Le contrôle de cohérence OHLC rejette un plus bas incompatible avec le plus haut et l'ouverture, dans un test isolé. | Facultative |
| C21 | Conserver les preuves et arrêter proprement | Sauvegarder les preuves puis arrêter ce projet | Conteneurs arrêtés et volumes conservés | Le projet est arrêté proprement en conservant les archives, les tables, les checkpoints et les preuves collectées. | Facultative |

## Ce qui doit être visible

- Symbole/action, minute ou jour UTC, identifiant de transaction lorsqu'il existe, commande ou composant concerné et résultat.
- Pour Grafana : plage temporelle et rafraîchissement. Pour Streamlit : action, cinq cartes, date de la minute et de la publication.
- Pour les alertes : actif financier, seuil, statut, dates et distinction entre observations et épisodes.
- Pour les tests : nom du script et résultat complet. Un FAILED, une exception inattendue ou un test ignoré doit être mentionné.
- Ne pas montrer .env, mots de passe, DSN, clés S3 ni identité de connexion. Un trade_id, offset ou snapshot n'est pas un mot de passe : il est utile à la preuve.

## Éviter les répétitions

C03 fixe le trade_id de référence ; reprendre le même dans C04, C15 et C16. C04 peut intégrer les colonnes Parquet de C05, sans nouvelle capture. C09a/C09b se complètent si graphe et schedules ne tiennent pas lisiblement sur la même vue.

C11 prouve la persistance et la correspondance ; C12 prouve la consultation. C13 peut être composé d'une vue de couverture et d'une alerte MSFT filtrée. C14 est facultative si les autres images du fil rouge font déjà le lien. C15/C16 se présentent en comparaison avant/après, avec les mêmes champs.

Pour C17, conserver les résultats ciblés et les commandes ; ne pas ajouter plusieurs pages de démarrage Java. C18/C19/C20 sont des preuves complémentaires préparées hors oral. Pendant les sept minutes, montrer au plus une preuve de reprise ou de rejet déjà vérifiée.

## Pièces à conserver avec les images

| Pièce dans artifacts/demo | Provenance | Usage |
|---|---|---|
| trace.json | C03 | Payload exact, SHA-256 et coordonnées Kafka originales |
| trace_inspection.txt | C04/C14 | Correspondances Kafka/Bronze/PostgreSQL |
| trace_silver_avant.txt, trace_silver_apres.txt | Sorties C15 avant/après C16, copiées sous ces noms | Comparaison de l'identité canonique et de la bougie |
| replay_delivery.txt | C16 | Confirmation d'envoi du même événement à un nouvel offset |
| trace_spark.log | C15, sortie d'erreur du processus Spark | Diagnostic de la lecture indépendante |
| journaux_fin.txt | C21 | Contexte des services, à contrôler avant partage |

Ajouter un relevé manuscrit ou un fichier indiquant pour chaque étape : date UTC, commande, réussie/échouée/non exécutée, résultat réellement observé et fichier associé.

## Règles de publication des preuves

Une image d'écran vide peut illustrer un échec, pas une réussite. Une capture ancienne de docs/evidence ou artifacts/ui garde sa date réelle. Les nombres historiques du 4 novembre ne doivent pas être attribués à une nouvelle exécution.

Chaque légende doit rester à la portée de son contrôle : vingt lignes comparées ne prouvent pas l'absence de perte sur tout l'historique ; une recherche dans trente manifestes ne prouve pas l'absence globale d'un événement. Afficher « données simulées » et distinguer les fixtures aux prix fixes du flux de production.

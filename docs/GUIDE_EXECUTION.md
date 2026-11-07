# Guide d'exécution et de mise à jour des conteneurs

Toutes les commandes suivantes sont à exécuter dans PowerShell, avec Docker Desktop démarré en mode conteneurs Linux.

## 1. Ouvrir le bon dossier

~~~powershell
Set-Location "C:\M2\P1\Tech_Data\Projet\Marche_Financier_Kafka_Bronze\finance-kafka-bronze"
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose config --quiet
~~~

L'ancien chemin Outils_Big_Data a été remplacé par Tech_Data.
Conserver le fichier .env existant et le nom de projet Compose finance-market : les volumes existants restent associés à ce projet.

## 2. Actualiser les anciens conteneurs après modification du code

~~~powershell
docker compose up -d --build --force-recreate --remove-orphans
docker compose ps -a
~~~

Cette commande reconstruit les images locales, recrée les conteneurs avec les configurations courantes et retire les anciens services de ce projet absents du Compose actuel.
Les volumes nommés PostgreSQL, MinIO, Kafka et les checkpoints sont conservés.
Aucun téléchargement systématique de nouvelles versions des images n'est nécessaire.
Un simple docker compose restart ne reconstruit pas les images et n'intègre donc pas les modifications du code copié dans une image.

Attendre la fin des démarrages : les services disposant d'un healthcheck doivent afficher healthy.
Le service init doit terminer avec Exited (0) : c'est normal.
Producer et consumer-bronze doivent afficher Up ; ils n'ont pas de healthcheck.
Le premier build et le démarrage de Spark peuvent prendre plusieurs minutes.

## 3. Démarrer aux utilisations suivantes

~~~powershell
docker compose up -d
docker compose ps -a
~~~

Il n'est pas nécessaire de reconstruire les images à chaque ouverture.

## 4. Exécuter les traitements

Producer, Kafka, l'archivage Bronze et Spark Streaming sont des services continus.
Les schedules Dagster doivent être RUNNING ; le traitement analytique et les alertes sont planifiés toutes les cinq minutes.

Pour lancer immédiatement l'analyse après que les services sont prêts :

~~~powershell
docker compose exec dagster dagster job execute -f definitions.py -j lakehouse_refresh
~~~

Pour vérifier le streaming indépendamment :

~~~powershell
docker compose exec dagster dagster job execute -f definitions.py -j streaming_supervision
~~~

Pour réévaluer les alertes depuis Gold déjà publié, sans recalcul des métriques :

~~~powershell
docker compose exec dagster dagster job execute -f definitions.py -j alerts_refresh
~~~

Éviter de lancer plusieurs fois le même job en parallèle. Vérifier d'abord les runs dans Dagster.
Au premier passage, le rattrapage des archives historiques peut demander plusieurs lots.
Une bougie est d'une minute ; Gold attend sa clôture et le délai de retard ; les alertes sont évaluées toutes les cinq minutes.

## 5. Ouvrir les interfaces

| Interface | URL | Connexion |
|---|---|---|
| Grafana | http://localhost:3001/d/finance-live | Utilisateur admin ; mot de passe GRAFANA_PASSWORD du .env |
| Streamlit | http://localhost:8501 | Pas de connexion configurée |
| Dagster | http://localhost:3000 | Pas de connexion configurée |
| MinIO | http://localhost:9001 | S3_ROOT_USER et S3_ROOT_PASSWORD du .env |

PostgreSQL : utilisateur finance, base finance ; mot de passe POSTGRES_PASSWORD du .env.
La base n'est pas exposée sur un port Windows dans le Compose actuel. Ouvrir sa console dans le conteneur :

~~~powershell
docker compose exec postgres psql -U finance -d finance
~~~

Dans psql, exemple de contrôle :

~~~sql
SELECT count(*) FROM candles;
SELECT origin, status, count(*) FROM alerts GROUP BY origin, status;
~~~

Quitter psql avec \q.

## 6. Consulter les erreurs

~~~powershell
docker compose logs --tail 80 init
docker compose logs --tail 80 producer consumer-bronze
docker compose logs --tail 80 spark lakehouse
docker compose logs --tail 80 dagster streamlit
~~~

Pour suivre les logs en continu :

~~~powershell
docker compose logs -f --tail 30 spark lakehouse
~~~

Ctrl+C ferme le suivi des logs sans arrêter les conteneurs.
Exited (137) indique un arrêt par SIGKILL ; les logs et les ressources Docker doivent être vérifiés pour en déterminer la cause.
Prévoir environ 8 Go de mémoire attribués à Docker Desktop pour cette maquette.

## 7. Arrêter et reprendre sans perdre les données

~~~powershell
docker compose stop
docker compose up -d
~~~

Si une recréation complète des conteneurs est nécessaire :

~~~powershell
docker compose down --remove-orphans
docker compose up -d --build
~~~

Ne pas ajouter -v à down : cette option supprime les volumes de données.
Ne pas supprimer les volumes dans Docker Desktop et ne pas utiliser de nettoyage global des volumes.
Les anciens conteneurs d'autres projets ne sont pas concernés par ces commandes.

## 8. Vérification avant présentation

1. Les services attendus sont Up et les healthchecks sont healthy.
2. Grafana affiche AAPL, MSFT et NVDA.
3. MinIO contient les archives Bronze.
4. Un run lakehouse_refresh a terminé avec succès dans Dagster.
5. Streamlit affiche les métriques Gold et l'historique des alertes.
6. Les schedules cinq minutes sont RUNNING.

Les anciennes données ne sont pas effacées par le lancement : voir VALIDATION.md pour les contrôles déjà exécutés et docs/DEMO.md pour la présentation.

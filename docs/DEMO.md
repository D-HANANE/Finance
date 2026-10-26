# Démonstration de sept minutes

Préparation hors chronomètre : construire la pile, attendre au moins dix minutes,
vérifier Gold et une alerte réelle. Avec moins de 50 minutes, expliquer les MA
nulles. Ne pas fabriquer une ancienne clôture quotidienne.

| Temps | Action | Preuve |
|---|---|---|
| 0:00-0:45 | Schéma et prix simulés | Deux chemins, trois couches |
| 0:45-1:30 | Producer et topic | >=200 confirmations/s, 20 symboles, six partitions |
| 1:30-2:15 | MinIO Bronze et manifeste | Brut, Parquet, offsets après stockage |
| 2:15-3:00 | PostgreSQL et Iceberg | Provisoire/final, grain et unicité |
| 3:00-3:45 | Grafana | Trois actions et refresh 5s |
| 3:45-4:45 | Streamlit | Alerte détectée, prix, référence et heures |
| 4:45-5:45 | Dagster, dbt, Soda | Run et contrôles réellement exécutés |
| 5:45-6:30 | Reprise Spark/Bronze | Checkpoints, chandelier final inchangé |
| 6:30-7:00 | Limites | Pas de prédiction ni exactly-once global |

Pour une alerte neuve, redémarrer le producteur six minutes avant le créneau.
Le choc de +8 % intervient après 360 secondes. Constater CHOC dans les logs,
puis une nouvelle ligne dans alerts et son affichage Streamlit. Un redémarrage
réinitialise aussi les prix simulés ; le signaler si une autre variation apparaît.

## Reprise

Consigner un chandelier déjà finalisé et son OHLCV/notional. Redémarrer avec
`docker compose restart consumer-bronze spark`. Après rattrapage, comparer les
valeurs et vérifier `SELECT symbol,minute_start,count(*) FROM candles GROUP BY
1,2 HAVING count(*)>1;`. Le nombre total de lignes peut augmenter normalement.

## Échec qualité

Utiliser le test offline sur sa base temporaire pour injecter un chandelier
invalide. Montrer l'échec réel dbt/Soda, sans modifier la base live. En production,
un échec conserve la dernière version Gold publiée et reste visible dans Dagster.

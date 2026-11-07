# Dictionnaire version 2

Tous les jours et horodatages métier sont en UTC. Les prix sont simulés.

| Objet | Grain | Rôle |
|---|---|---|
| Kafka trades | Un échange, trade_id | symbol, price décimal(18,4), quantity entier, event_time |
| Bronze JSONL/Parquet | Une transaction et ses coordonnées Kafka | Archive inchangée ; ancien volume accepté comme alias de quantity |
| Bronze manifeste | Un lot complet | Objets, nombre de lignes, prochains offsets |
| lake.silver.trades | Un trade_id immuable | Canonique, élimine les duplications entre lots |
| lake.silver.rejected | Un objet source et un événement rejeté | Motif de rejet et provenance |
| lake.silver.candles | Une action et une minute | OHLCV, notional, trade_count, dernier événement |
| lake.gold.minute_metrics | Une action et une minute | MA20/50, VWAP cumulatif, log-rendement, référence et variation cinq minutes |
| lake.gold.daily_metrics | Une action et un jour | Rendement, volatilité, VWAP et couverture |
| lake.gold.correlation_daily | Une paire non ordonnée et un jour | n, sommes, carrés et produits des log-rendements communs |
| lake.gold.correlations | Une paire non ordonnée | Corrélation Pearson et nombre d'observations |
| PostgreSQL candles/latest_prices | Une action/minute ou une action | Source de Grafana, chaîne streaming uniquement |
| PostgreSQL alerts | Un épisode de dépassement | UUID, actif, règle, seuil, déclenchement, dernière observation, statut |
| PostgreSQL alert_observations | Une action et une minute Gold | Valeurs existantes de Gold, empreinte et date de modification |
| PostgreSQL alert_history | Une modification d'un épisode | Statut, action, date métier, date d'enregistrement, détails |
| PostgreSQL alert_state | Un lecteur de Gold | Curseur et snapshot évalué |
| PostgreSQL lakehouse_files | Un manifeste Bronze | ETag et dernière ingestion réussie |
| PostgreSQL lakehouse_dirty | Une minute à recalculer | État opérationnel incrémental |
| PostgreSQL lakehouse_publication | Une génération Gold publiée | Identifiants de snapshots cohérents |

## Formules

Open/Close suivent le minimum/maximum du couple (event_time,trade_id), pas l'arrivée.
High/Low sont les extrêmes. Volume = somme(quantity).
Notional = somme(price × quantity), conservé en décimal pour pondérer le VWAP.

VWAP quotidien = somme(notional)/somme(volume). La version minute est cumulative
depuis minuit. MA20/MA50 sont les moyennes des 20/50 closes minute observés.
Un trou temporel peut allonger leur durée réelle ; sans assez d'observations : NULL.

Log-rendement = ln(close_t/close_précédent), uniquement entre minutes consécutives.
Volatilité quotidienne = écart type échantillonnal des log-rendements minute du jour,
au moins deux rendements. Pas d'annualisation.
Rendement quotidien = close_jour/close_veille-1, uniquement si le jour UTC précédent existe.
is_complete signifie que le jour est passé ; minutes expose sa couverture réelle.

change_5m = close_t/close_t-5minutes -1. La référence est une minute exacte, pas une
interpolation. Référence absente : NULL. Le seuil est strict : abs(change_5m)>0.05.

Le grain minute et le schedule cinq minutes sont différents. Chaque observation
minute nouvelle/corrigée est évaluée. Une condition persistante ne crée pas un nouvel
épisode à chaque exécution. La résolution utilise le même seuil, sans nouvelle règle.

## Projections PostgreSQL Gold — version 3

- gold_minute : close, MA20/MA50, vwap_day, log_return, référence cinq minutes, change_5m ; clé symbole/minute.
- gold_daily : daily_return, daily_volatility, vwap, couverture et état complet ; clé symbole/jour.
- gold_correlations : corrélations copiées depuis Gold.
- gold_serving_publication : snapshots, curseur, version de formule et date de publication.
- alert_symbol_coverage : observations, références disponibles, dépassements et épisodes actifs par action.

Chaque projection conserve source_snapshot et published_at. Les calculs restent Spark SQL.
Le VWAP convertit les sommes en double avant division pour éviter l’arrondi décimal prématuré.

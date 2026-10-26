# Dictionnaire et formules

Heures et jours en UTC. Prix en USD simulés. NULL signifie historique insuffisant.

| Objet | Grain et clé | Champs principaux |
|---|---|---|
| trades | Un échange, trade_id UUID hex | symbol, price décimal(18,4)>0, quantity entier>0, event_time ISO8601 avec fuseau |
| Bronze JSONL | Un message brut par ligne | Payload inchangé ; ancien volume accepté comme quantité |
| Bronze Parquet | Une coordonnée Kafka | Champs typés, quantity, _topic, _partition, _offset, _batch_id, _ingested_at |
| Manifeste | Un lot écrit complètement | Clés JSON/Parquet, nombre de lignes, prochains offsets |
| Validated Parquet | Un échange valide dédupliqué dans la borne | Passage persistant vers les deux agrégations |
| Rejected Parquet | Un message invalide | raw, topic, partition, offset, reason=invalid_contract |
| Silver Iceberg | Une action/minute, symbol+minute_start | OHLC, volume, notional, trade_count, last_event_time, last_trade_id |
| candles PostgreSQL | Même grain, état courant | is_final et updated_at en plus |
| latest_prices | Une action | price, event_time, trade_id |
| alerts / gold_alerts | Une action/créneau de cinq minutes | Prix, référence, heures, variation fractionnaire, détection |
| gold_minute | Une action/minute finalisée | close, ma20, ma50 |
| gold_daily | Une action/jour UTC | close, daily_return, daily_volatility, vwap, minutes, is_complete |
| gold_correlations | Une paire non ordonnée | Corrélation et nombre de minutes communes |
| pipeline_runs | Une publication Gold réussie | Horodatage et chemin du snapshot exact |

- Fenêtres : `[minute_start,minute_end[`.
- Open/Close : prix du min/max lexicographique `(event_time,trade_id)`.
- High/Low : maximum/minimum du prix dans la fenêtre.
- Volume : somme(quantity). Notional : somme(price × quantity), en décimal.
- VWAP : somme(notional)/somme(volume), depuis minuit UTC. Zéro volume : NULL.
- Rendement quotidien : close_jour/close_veille-1, seulement si la clôture du
  jour UTC immédiatement précédent existe. Le jour courant est partiel.
- Log-rendement minute : ln(close_t/close_t-1), seulement si les deux minutes
  sont consécutives. Les trous ne sont pas convertis en rendements minute.
- Volatilité quotidienne : écart type échantillonnal des log-rendements minute
  disponibles du jour, au moins deux rendements. Unité : rendement par minute.
  Aucune annualisation ni facteur sqrt(1440). C'est une mesure intrajournalière
  regroupée par jour, pas une volatilité annuelle.
- MA20/MA50 : moyennes des 20/50 closes minute observés. Un trou allonge la durée
  murale ; il ne crée pas de valeur. Ce ne sont pas des moyennes sur 20/50 jours.
- Corrélation : Pearson des log-rendements aux minutes communes de tout
  l'historique conservé, au moins trois observations. Série constante : NULL possible.

is_complete signifie que le jour UTC est passé ; ce n'est pas une certification
de 1 440 minutes présentes. Le champ minutes expose la couverture réelle.
La simulation tourne 24 h/24 et ne reproduit pas un calendrier de bourse.

# Vérification du socle

Vérifications réalisées lors de la préparation :

- Syntaxe des sept fichiers Python : valide.
- Lecture du YAML, résolution des ancres et contrôle des paramètres clés : valide.
- Tests du générateur : 20 symboles, identifiants distincts, prix et volumes
  positifs, timestamps avec fuseau, sérialisation JSON : réussis.
- Test du choc contrôlé : réussi.
- Test de conservation des payloads, calcul des prochains offsets par
  partition, lecture des fichiers Parquet et publication du manifeste en
  dernier : réussi.

Résultat : trois tests exécutés, trois réussis, aucun ignoré, avec les
dépendances de requirements.txt.

Docker n'est pas disponible dans l'environnement de préparation. La
construction des images, le démarrage des services, l'intégration Kafka/S3,
le débit de 200 transactions/s et l'exécution de l'exemple Spark restent à
vérifier sur la machine du projet. Les commandes de contrôle sont dans README.md.

# Ma Watchlist Boursière, version corrigée

## Ce qui change

Cette version corrige les défauts structurels du prototype initial :

- plus de boucle `sleep(1) + rerun()`
- cache propre des prix pendant 30 minutes
- téléchargement par paquets pour éviter un appel monolithique fragile
- validation des tickers Google Finance -> Yahoo
- affichage explicite des tickers non résolus ou non récupérés
- calcul de variation basé sur des barres `30m`, cohérent avec un refresh toutes les 20 a 30 minutes

## Limites assumées

- source gratuite et non officielle : Yahoo Finance via `yfinance`
- ce n'est pas une infrastructure de marché professionnelle
- certaines places ou certains tickers exotiques peuvent demander un override manuel
- les données peuvent etre absentes ou irrégulières selon la place, l'heure et Yahoo

## Déploiement Streamlit Cloud

Fichiers à pousser dans le dépôt GitHub :

- `app.py`  -> prends `app_corrected.py` et renomme-le en `app.py`
- `tickers.csv`
- `requirements.txt`

## Format attendu pour `tickers.csv`

Colonnes obligatoires :

- `gf_ticker`
- `portif`
- `name`

Colonnes optionnelles :

- `note`
- `buy`
- `fair`
- `trim`
- `exit`

## Paramètres principaux

Dans `app.py`, tu peux modifier :

- `REFRESH_TTL_SECONDS = 30 * 60`
- `DOWNLOAD_PERIOD = "5d"`
- `DOWNLOAD_INTERVAL = "30m"`
- `BATCH_SIZE = 75`

## Lecture correcte du fonctionnement

- l'app ne tourne pas en tâche de fond toute seule
- elle rafraîchit les données quand un utilisateur ouvre ou recharge l'interface
- le bouton `Actualiser maintenant` force un nouveau fetch en vidant le cache

Si tu veux un vrai refresh autonome, il faut un scheduler externe ou une architecture différente.

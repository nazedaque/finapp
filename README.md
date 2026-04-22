# 📈 Watchlist Boursière

## Déploiement Streamlit Cloud (gratuit)

### Étape 1 — Rendre le Google Sheet public
Dans ton Google Sheet :
**Partager → Modifier → "Toute personne disposant du lien" → Lecteur**

Sans ça, l'app ne peut pas lire le sheet et utilisera `tickers.csv` en fallback.

### Étape 2 — Repo GitHub
Crée un dépôt GitHub **public** et dépose ces 4 fichiers :
- `app.py`
- `tickers.csv` (fallback si le sheet est privé)
- `requirements.txt`
- `README.md`

### Étape 3 — Déploiement
1. Va sur **share.streamlit.io**
2. Connecte avec GitHub
3. "New app" → ton dépôt → branche `main` → `app.py`
4. Deploy

→ Tu obtiens une URL accessible depuis Android (ajoute en raccourci écran d'accueil via Chrome)

---

## Ajouter / modifier des tickers

**Méthode principale (recommandée) :** directement dans l'onglet `Travail` de ton Google Sheet.
L'app relit le sheet toutes les heures automatiquement.

Colonnes utilisées par l'app :
| Colonne Google Sheet | Rôle |
|---|---|
| `Ticker` | Ticker format Google Finance |
| `Société` | Nom de la société |
| `Portif` | 1 = Portefeuille, 0 = Watchlist |
| `Note` | Score de qualité (0–100) |
| `Buy / Fair / Trim / Exit` | Niveaux de prix |
| `URL` | Lien analyse ChatGPT |

**Fallback :** si le sheet est privé, modifie `tickers.csv` et pousse sur GitHub.

---

## Overrides de tickers manuels

Si un ticker est mal converti, ajoute-le dans `MANUAL_OVERRIDES` dans `app.py` :
```python
MANUAL_OVERRIDES = {
    "EPA:HAVAS": "HAVAS.AS",   # Havas → Amsterdam
    "TSE:DHT.U": "DHT-U.TO",  # DRI Healthcare Trust
    "TSE:CTC.A": "CTC-A.TO",  # Canadian Tire classe A
    "CPH:VAR":   "VAR.OL",    # Vår Energi → Oslo
    "MOUR":      "MOUR.BR",   # Moury Construct → Bruxelles
    # ajouter ici...
}
```

## Paramètres
```python
REFRESH_TTL       = 30 * 60   # cache des cours (secondes)
DOWNLOAD_PERIOD   = "5d"      # historique Yahoo pour calcul variation
DOWNLOAD_INTERVAL = "30m"     # granularité des barres Yahoo
BATCH_SIZE        = 75        # tickers par paquet Yahoo
```

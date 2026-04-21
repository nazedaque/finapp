# 📈 Ma Watchlist Boursière

Application Streamlit alimentée par yfinance — remplace Google Finance dans Google Sheets.

## 🚀 Déploiement sur Streamlit Cloud (gratuit)

### Étape 1 — Préparer GitHub
1. Crée un compte sur [github.com](https://github.com) si pas encore fait
2. Crée un **nouveau dépôt** (ex: `my-watchlist`), **Public**
3. Upload ces 3 fichiers dans le dépôt :
   - `app.py`
   - `tickers.csv`
   - `requirements.txt`

### Étape 2 — Déployer sur Streamlit Cloud
1. Va sur [share.streamlit.io](https://share.streamlit.io)
2. Connecte-toi avec GitHub
3. Clique **"New app"**
4. Sélectionne ton dépôt, branche `main`, fichier `app.py`
5. Clique **"Deploy"** → ton app tourne en quelques minutes

### Étape 3 — Accès Android
- L'app génère une URL publique (ex: `https://xxx.streamlit.app`)
- Ouvre-la dans Chrome sur Android
- Pour la garder en icône : **⋮ → Ajouter à l'écran d'accueil**

## 🔄 Mise à jour des tickers
Pour ajouter/supprimer des titres, modifie `tickers.csv` et redéploie (push sur GitHub).
Les colonnes : `gf_ticker, portif, name, note, buy, fair, trim, exit`
- `portif` = 1 pour Portefeuille, 0 pour Watchlist
- `gf_ticker` = format Google Finance (ex: `EPA:VIL`, `AAPL`, `LON:GSK`)

## ⚡ Rafraîchissement
Les cours se rafraîchissent automatiquement toutes les **20 minutes**.
Tu peux forcer un rafraîchissement avec le bouton 🔄.

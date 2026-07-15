# Finapp SOL

Vue Streamlit privée et en lecture seule de l'onglet `Registre` du Sheet `SOL input`.

## Confidentialité

- Le Sheet reste privé : ne pas activer « Publier sur le Web ».
- La connexion Google utilise un compte de service en lecture seule.
- Les identifiants Google et le code d'accès restent dans les secrets Streamlit.
- Aucune position ni copie CSV n'est enregistrée dans GitHub.

## Configuration Streamlit

1. Copier la structure de `.streamlit/secrets.toml.example` dans les secrets de l'application.
2. Remplacer les valeurs d'exemple par celles du compte de service.
3. Partager `SOL input` avec l'adresse `client_email` du compte de service en lecteur.
4. Choisir un `access_code` d'au moins 12 caractères.

Le fichier local `.streamlit/secrets.toml` est ignoré par Git.

## Lancement local

```powershell
streamlit run app.py
```

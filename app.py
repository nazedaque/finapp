from __future__ import annotations

import json
import html
import hmac
import logging
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import date, datetime, timezone
import pandas as pd
import streamlit as st
import yfinance as yf

from finapp_logic import (
    clean_sheet_text,
    coalesce_alias_columns,
    compute_ratio,
    compute_score,
    configure_gsheets_timeout,
    country_code,
    finite_float,
    find_sheet_errors,
    is_suspended_underwriting,
    merge_quote_cache,
    parse_number,
    parse_sheet_date,
    stale_quote_tickers,
)

LOGGER = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Finapp SOL", page_icon="assets/favicon-champion.png", layout="wide",
                   initial_sidebar_state="collapsed")

APP_TITLE         = "Finapp SOL"
SHEET_ID          = "1P6f-aDWgS6a9qstyazQlITGv6NBraU9yG3uN1Fu8R1o"
SHEET_NAME        = "Registre"
SCREENING_SHEET_NAME = "Screening"
AUDITS_SHEET_NAME = "Audits"
REFRESH_TTL       = 15 * 60
BATCH_SIZE        = 50
YF_META_BATCH_SIZE = 10
YF_BATCH_PAUSE_SEC = 0.2
HTTP_RETRIES      = 3
PROFILE_CACHE_TTL = 7 * 24 * 60 * 60
GSHEETS_HTTP_TIMEOUT = (5, 15)


def _secret(path: tuple[str, ...], default=None):
    try:
        value = st.secrets
        for key in path:
            value = value[key]
        return value
    except (FileNotFoundError, KeyError, TypeError):
        return default


def _render_access_styles() -> None:
    """Habillage dédié à l'écran privé, sans modifier le tableau de bord."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        [data-testid="stAppViewContainer"] {
          background:
            radial-gradient(circle at 18% 18%, rgba(37, 99, 235, .18), transparent 34%),
            radial-gradient(circle at 82% 82%, rgba(14, 165, 233, .10), transparent 30%),
            #090d15 !important;
        }
        [data-testid="stAppViewContainer"] > .main { background: transparent !important; }
        [data-testid="stHeader"], [data-testid="stToolbar"],
        [data-testid="stDecoration"], #MainMenu, footer { display: none !important; }
        .block-container {
          max-width: 440px !important;
          padding: clamp(2.25rem, 10vh, 7rem) 1.25rem 2rem !important;
        }
        html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

        [data-testid="stForm"], .finapp-config-card {
          background: rgba(17, 24, 39, .94);
          border: 1px solid rgba(148, 163, 184, .16);
          border-radius: 22px;
          padding: 2.2rem 2.15rem 1.75rem;
          box-shadow: 0 28px 70px rgba(0, 0, 0, .46), inset 0 1px 0 rgba(255, 255, 255, .035);
          backdrop-filter: blur(16px);
        }
        .finapp-config-card { text-align: center; }
        .finapp-config-card h1 {
          color: #f8fafc;
          font-size: 1.35rem;
          line-height: 1.2;
          letter-spacing: -.025em;
          margin: 0 0 .55rem;
        }
        .finapp-config-card p {
          color: #8f9bb0;
          font-size: .86rem;
          line-height: 1.55;
          margin: 0;
        }
        [data-testid="stTextInput"] { margin-bottom: .45rem; }
        [data-testid="stTextInput"] label p {
          color: #cbd5e1 !important;
          font-size: .76rem !important;
          font-weight: 600 !important;
        }
        [data-testid="stTextInput"] input {
          height: 46px;
          color: #f8fafc !important;
          background: #0b1220 !important;
          border: 1px solid #263247 !important;
          border-radius: 10px !important;
          box-shadow: none !important;
        }
        [data-testid="stTextInput"] input:focus {
          border-color: #3b82f6 !important;
          box-shadow: 0 0 0 3px rgba(59, 130, 246, .14) !important;
        }
        [data-testid="stFormSubmitButton"] button {
          min-height: 46px;
          margin-top: .35rem;
          color: #fff !important;
          font-size: .82rem !important;
          font-weight: 650 !important;
          border: 0 !important;
          border-radius: 10px !important;
          background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
          box-shadow: 0 10px 22px rgba(37, 99, 235, .26) !important;
          transition: transform .16s ease, box-shadow .16s ease !important;
        }
        [data-testid="stFormSubmitButton"] button:hover {
          transform: translateY(-1px);
          box-shadow: 0 13px 28px rgba(37, 99, 235, .34) !important;
        }
        .finapp-login-note {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: .45rem;
          color: #64748b;
          font-size: .68rem;
          margin-top: 1.1rem;
        }
        .finapp-login-note::before {
          content: '';
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #22c55e;
          box-shadow: 0 0 0 3px rgba(34, 197, 94, .11);
        }
        [data-testid="stAlert"] {
          border-radius: 10px !important;
          font-size: .76rem !important;
        }
        @media (max-width: 520px) {
          .block-container { padding-top: 2.25rem !important; }
          [data-testid="stForm"], .finapp-config-card { padding: 1.8rem 1.35rem 1.4rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def access_guard() -> None:
    """Bloque tout chargement du Sheet avant validation du code privé."""
    expected = str(_secret(("app", "access_code"), "")).strip()
    invalid = not expected or expected.lower().startswith("replace") or len(expected) < 4

    if invalid:
        _render_access_styles()
        st.markdown(
            """
            <div class="finapp-config-card">
              <h1>Configuration requise</h1>
              <p>Ajoutez le code d'accès et la connexion Google dans les secrets Streamlit.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.stop()

    if st.session_state.get("access_granted"):
        return

    _render_access_styles()
    with st.form("access_form", clear_on_submit=True):
        candidate = st.text_input(
            "Code d'accès",
            type="password",
            autocomplete="current-password",
            placeholder="Saisissez votre code",
        )
        submitted = st.form_submit_button(
            "Accéder à Finapp", type="primary", use_container_width=True
        )
        st.markdown(
            '<div class="finapp-login-note">Connexion privée et sécurisée</div>',
            unsafe_allow_html=True,
        )
    if submitted:
        if hmac.compare_digest(candidate.encode(), expected.encode()):
            st.session_state["access_granted"] = True
            st.rerun()
        st.error("Code incorrect.")
    st.stop()


access_guard()

# ══════════════════════════════════════════════════════════════════════════════
# Colonnes & layout — identiques entre onglets
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "MAJ", "Audit", "JRS", "Pays", "Ticker", "Société", "Qual", "Prix", "Var %", "Upside",
    "Score", "Buy", "Fair", "Trim", "Exit", "Industrie",
]
COL_WIDTHS = {
    "MAJ": "46px", "Audit": "42px", "JRS": "38px", "Pays": "36px",
    "Ticker": "49px", "Société": "145px", "Qual": "44px",
    "Prix": "45px", "Var %": "55px", "Upside": "51px",
    "Score": "62px",
    "Buy": "51px", "Fair": "51px", "Trim": "51px", "Exit": "51px", "Industrie": "145px",
}
CENTER = {"MAJ", "Audit", "JRS", "Pays", "Prix", "Var %", "Upside", "Score",
          "Buy", "Fair", "Trim", "Exit", "Qual"}
GROUP_STARTS = {"Prix", "Score", "Buy", "Industrie"}
HEADER_CENTER = CENTER
HEADER_LABELS = {"Pays": "EXC"}
SORTABLE_COLUMNS = {
    "MAJ": "number",
    "Audit": "number",
    "JRS": "number",
    "Pays": "text",
    "Ticker": "text",
    "Société": "text",
    "Qual": "number",
    "Prix": "number",
    "Upside": "number",
    "Var %": "number",
    "Score": "number",
    "Industrie": "text",
}

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def parse_num(v) -> float | None:
    return parse_number(v)


# ══════════════════════════════════════════════════════════════════════════════
# Chargement du sheet
# ══════════════════════════════════════════════════════════════════════════════

SHEET_COL_NORMALIZED = {
    "ticker":      "gf_ticker",
    "entreprise":  "name",
    "societe":     "name",
    "portif":      "portif",
    "date d'achat": "purchase_date",
    "date d achat": "purchase_date",
    "verif":       "verif",
    "v":           "verif",
    "note":        "note",
    "qualite /100": "note",
    "buy":         "buy",
    "fair":        "fair",
    "trim":        "trim",
    "exit":        "exit",
    "url":         "url",
    "commentaire": "comments",
    "commentaires": "comments",
    "comments":    "comments",
    "spot":        "spot_sheet",
    "cours":       "spot_sheet",
    "devise":      "currency",
    "score mixte": "score_sheet",
    "score global": "score_sheet",
    "score global /100": "score_sheet",
    "zone actuelle": "zone",
    "upside fair": "upside_fair_sheet",
    "upside trim": "upside_trim_sheet",
    "confiance": "confidence",
    "sensibilite normalisation": "normalization_sensitivity",
    "date analyse": "last_update",
    "date comptes": "accounts_date",
    "version prompt": "prompt_version",
    "audit": "verif",
    "audit impact": "audit_impact",
    "action suivante": "next_action",
    "last update": "last_update",
    "yf ticker":   "yf_ticker",
    "yf_ticker":   "yf_ticker",
}
SCREENING_COL_NORMALIZED = {
    "ticker": "gf_ticker",
    "entreprise": "name",
    "societe": "name",
    "cours": "spot_sheet",
    "devise": "currency",
    "qualite provisoire": "note",
    "buy provisoire": "buy",
    "fair provisoire": "fair",
    "trim provisoire": "trim",
    "exit provisoire": "exit",
    "verdict": "screening_verdict",
    "confiance": "confidence",
    "point decisif": "screening_key_point",
    "date screening": "last_update",
    "version prompt": "screening_prompt_version",
    "statut": "screening_status",
}
AUDIT_COL_NORMALIZED = {
    "ticker": "gf_ticker",
    "statut audit": "audit_status",
}
NUMERIC_COLS = [
    "note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet",
    "upside_fair_sheet", "upside_trim_sheet",
]
REGISTER_TEXT_COLS = [
    "gf_ticker", "yf_ticker", "name", "currency", "url", "comments", "zone",
    "confidence", "normalization_sensitivity", "accounts_date", "prompt_version",
    "verif", "audit_impact", "next_action",
]


def _normalize_col(s: str) -> str:
    """Normalisation agressive : supprime BOM, accents, espaces, casse."""
    s = str(s).replace("\ufeff", "").replace("\u202f", "").replace("\xa0", "")
    nfkd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()


def _private_sheet_connection():
    """Retourne la connexion Google privée configurée dans Streamlit."""
    from streamlit_gsheets import GSheetsConnection

    connection = st.connection("gsheets", type=GSheetsConnection)
    configure_gsheets_timeout(connection, GSHEETS_HTTP_TIMEOUT)
    return connection


def _read_private_sheet(ttl: str | int = "5m") -> pd.DataFrame:
    """Lit les valeurs du Registre via Google."""
    connection = _private_sheet_connection()
    return connection.read(worksheet=SHEET_NAME, ttl=ttl)


def _read_screening_sheet(ttl: str | int = "5m") -> pd.DataFrame:
    """Lit les valeurs de Screening ; aucun Score global n'y est disponible."""
    connection = _private_sheet_connection()
    return connection.read(worksheet=SCREENING_SHEET_NAME, ttl=ttl)


def _read_audits_sheet(ttl: str | int = "5m") -> pd.DataFrame:
    """Lit les audits réels, source de vérité de la lumière verte."""
    connection = _private_sheet_connection()
    return connection.read(worksheet=AUDITS_SHEET_NAME, ttl=ttl)


def load_tickers(force_refresh: bool = False) -> tuple[pd.DataFrame, str]:
    """Charge et normalise SOL input sans rendre le Sheet public."""
    try:
        df = _read_private_sheet(ttl=0 if force_refresh else "5m")
    except Exception as exc:
        raise RuntimeError(
            "Impossible de lire SOL input / Registre avec la connexion Google privée. "
            "Vérifiez les secrets Streamlit et le partage en lecture avec le compte de service."
        ) from exc

    raw_columns = list(df.columns)
    st.session_state["sheet_errors"] = find_sheet_errors(df)
    df, alias_collisions = coalesce_alias_columns(df, SHEET_COL_NORMALIZED)
    st.session_state["column_alias_collisions"] = alias_collisions

    # Colonnes manquantes → NA
    for col in SHEET_COL_NORMALIZED.values():
        if col not in df.columns:
            df[col] = pd.NA
    for col in REGISTER_TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].map(clean_sheet_text)
    df["verif_display"] = df["verif"].apply(fmt_verif)
    df["flagged"] = False

    # Nettoyage
    df["gf_ticker"] = df["gf_ticker"].mask(df["gf_ticker"].eq(""), df["yf_ticker"])
    df["yf_ticker"] = df["yf_ticker"].mask(df["yf_ticker"].eq(""), df["gf_ticker"])
    df["gf_ticker"] = df["gf_ticker"].str.upper()
    df["yf_ticker"] = df["yf_ticker"].str.upper()
    df = df[~df["gf_ticker"].isin(
        ["", "TICKER", "GF_TICKER", "NAN", "NONE", "<NA>"]
    )].copy()

    if df.empty:
        raise RuntimeError(
            f"DataFrame vide après filtrage. Colonnes trouvées : "
            f"{[_normalize_col(c) for c in raw_columns] or ['(aucune)']}. "
            f"Colonnes brutes du CSV : voir onglet Debug."
        )

    df["portif"] = df["portif"].map(
        lambda v: 1
        if parse_num(v) == 1 or str(v).strip().upper() in ("OUI", "TRUE", "VRAI")
        else 0
    )
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_num)
    if "last_update" in df.columns:
        df["last_update"] = df["last_update"].apply(parse_sheet_date)
    else:
        df["last_update"] = None
    if "purchase_date" in df.columns:
        df["purchase_date"] = df["purchase_date"].apply(parse_sheet_date)

    # Détection des doublons
    dupes = df[df["gf_ticker"].duplicated(keep=False)][["gf_ticker", "yf_ticker"]].copy()
    st.session_state["ticker_dupes"] = dupes.to_dict("records") if not dupes.empty else []

    return df.reset_index(drop=True), "SOL input / Registre (privé)"


def _empty_screening_candidates() -> pd.DataFrame:
    columns = list(dict.fromkeys(SHEET_COL_NORMALIZED.values()))
    columns.extend([
        "screening_verdict", "screening_status", "screening_key_point",
        "screened_only",
    ])
    return pd.DataFrame(columns=columns)


def _normalize_screening_candidates(
    raw_df: pd.DataFrame,
    registry_tickers,
) -> pd.DataFrame:
    """Convertit les screenings APPROFONDIR vers le format du tableau Finapp."""
    df, alias_collisions = coalesce_alias_columns(raw_df, SCREENING_COL_NORMALIZED)
    st.session_state["screening_alias_collisions"] = alias_collisions
    if "gf_ticker" not in df.columns or "screening_verdict" not in df.columns:
        return _empty_screening_candidates()

    for column in (
        "gf_ticker", "name", "currency", "screening_verdict", "confidence",
        "screening_key_point", "screening_prompt_version", "screening_status",
    ):
        if column in df.columns:
            df[column] = df[column].map(clean_sheet_text)
    df["gf_ticker"] = df["gf_ticker"].str.upper()
    df = df[~df["gf_ticker"].isin(["", "TICKER", "NAN", "NONE"])].copy()
    normalized_verdict = df["screening_verdict"].apply(_normalize_col)
    df = df[normalized_verdict == "approfondir"].copy()

    registry_set = {
        str(ticker).strip().upper()
        for ticker in registry_tickers
        if pd.notna(ticker) and str(ticker).strip()
    }
    df = df[~df["gf_ticker"].isin(registry_set)].copy()
    df = df.drop_duplicates(subset=["gf_ticker"], keep="last")

    required_columns = dict.fromkeys((
        *SHEET_COL_NORMALIZED.values(),
        *SCREENING_COL_NORMALIZED.values(),
    ))
    for column in required_columns:
        if column not in df.columns:
            df[column] = pd.NA

    for column in ("note", "buy", "fair", "trim", "exit", "spot_sheet"):
        df[column] = df[column].apply(parse_num)
    df["last_update"] = df["last_update"].apply(parse_sheet_date)

    # Screening ne produit pas de Score global : la colonne Score affichera la zone d'achat.
    df["score_sheet"] = pd.NA

    df["yf_ticker"] = df["gf_ticker"]
    df["portif"] = 0
    df["prompt_version"] = pd.NA
    df["verif"] = pd.NA
    df["verif_display"] = ""
    df["flagged"] = False
    df["screened_only"] = True
    return df.reset_index(drop=True)


def load_screening_candidates(
    registry_tickers,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Charge les titres APPROFONDIR encore absents du Registre."""
    try:
        raw_df = _read_screening_sheet(ttl=0 if force_refresh else "5m")
    except Exception as exc:
        raise RuntimeError(
            "Impossible de lire SOL input / Screening avec la connexion Google privée."
        ) from exc
    st.session_state["screening_sheet_errors"] = find_sheet_errors(raw_df)
    return _normalize_screening_candidates(raw_df, registry_tickers)


def _normalize_audit_statuses(raw_df: pd.DataFrame) -> dict[str, str]:
    """Conserve le dernier statut d'audit non vide pour chaque ticker."""
    df, alias_collisions = coalesce_alias_columns(raw_df, AUDIT_COL_NORMALIZED)
    st.session_state["audit_alias_collisions"] = alias_collisions
    if "gf_ticker" not in df.columns or "audit_status" not in df.columns:
        return {}

    df = df[df["gf_ticker"].notna() & df["audit_status"].notna()].copy()
    df["gf_ticker"] = df["gf_ticker"].map(clean_sheet_text).str.upper()
    df["audit_status"] = df["audit_status"].map(clean_sheet_text)
    df = df[
        ~df["gf_ticker"].isin(["", "TICKER", "NAN", "NONE"])
        & df["audit_status"].ne("")
    ].copy()
    df = df.drop_duplicates(subset=["gf_ticker"], keep="last")
    return dict(zip(df["gf_ticker"], df["audit_status"]))


def load_audit_statuses(force_refresh: bool = False) -> dict[str, str]:
    """Charge les statuts depuis Audits ; le Registre ne suffit plus à valider un audit."""
    try:
        raw_df = _read_audits_sheet(ttl=0 if force_refresh else "5m")
    except Exception as exc:
        raise RuntimeError(
            "Impossible de lire SOL input / Audits avec la connexion Google privée."
        ) from exc
    st.session_state["audit_sheet_errors"] = find_sheet_errors(raw_df)
    return _normalize_audit_statuses(raw_df)

# ══════════════════════════════════════════════════════════════════════════════
# Métadonnées (noms) — parallèle, cache 7j
# ══════════════════════════════════════════════════════════════════════════════

def iter_completed(futures: dict, timeout: int = 60):
    """Renvoie les futures terminées sans faire échouer tout le batch en cas de timeout."""
    try:
        yield from as_completed(futures, timeout=timeout)
    except TimeoutError:
        return

def _fetch_search_profile(ticker: str) -> tuple[str, dict[str, str]]:
    """Nom et industrie Yahoo via le résultat exact de la recherche du ticker."""
    symbol = str(ticker or "").strip().upper()
    empty = {"name": "", "industry": ""}
    if not symbol:
        return symbol, empty

    encoded = urllib.parse.quote(symbol, safe="")
    url = (
        "https://query1.finance.yahoo.com/v1/finance/search"
        f"?q={encoded}&quotesCount=5&newsCount=0&listsCount=0&enableFuzzyQuery=false"
    )
    for attempt in range(HTTP_RETRIES):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            quote = next(
                (
                    item for item in payload.get("quotes", [])
                    if str(item.get("symbol", "")).strip().upper() == symbol
                ),
                {},
            )
            name = str(quote.get("shortname") or quote.get("longname") or "").strip()
            industry = str(quote.get("industry") or quote.get("sector") or "").strip()
            return symbol, {"name": name, "industry": industry}
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            retryable = not isinstance(exc, urllib.error.HTTPError) or exc.code == 429 or exc.code >= 500
            if not retryable or attempt + 1 >= HTTP_RETRIES:
                break
            time.sleep(0.4 * (2 ** attempt))
        except Exception:
            break
    return symbol, empty


@st.cache_data(ttl=PROFILE_CACHE_TTL, show_spinner=False)
def fetch_profiles(
    yf_tickers: tuple[str, ...],
    refresh_nonce: int = 0,
) -> dict[str, dict[str, str]]:
    del refresh_nonce  # Permet de réessayer les profils manquants lors d'un rafraîchissement.
    profiles: dict[str, dict[str, str]] = {}
    tickers = list(yf_tickers)
    # Requêtes unitaires Yahoo : petits lots et cache long, l'industrie change rarement.
    for i in range(0, len(tickers), YF_META_BATCH_SIZE):
        batch = tickers[i: i + YF_META_BATCH_SIZE]
        executor = ThreadPoolExecutor(max_workers=8)
        try:
            futures = {executor.submit(_fetch_search_profile, t): t for t in batch}
            for future in iter_completed(futures):
                try:
                    ticker, profile = future.result(timeout=15)
                    profiles[ticker] = profile
                except Exception:
                    profiles[str(futures[future]).upper()] = {"name": "", "industry": ""}
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        if i + YF_META_BATCH_SIZE < len(tickers):
            time.sleep(YF_BATCH_PAUSE_SEC)
    return profiles

# ══════════════════════════════════════════════════════════════════════════════
# Cours Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def _chunked(items, size):
    for i in range(0, len(items), size): yield items[i: i + size]

def _closes(data, ticker, multi):
    if data is None or getattr(data, "empty", True):
        return pd.Series(dtype=float)
    candidates = []
    try:
        if isinstance(data.columns, pd.MultiIndex):
            candidates.extend([
                lambda: data[ticker]["Close"],
                lambda: data["Close"][ticker],
                lambda: data[(ticker, "Close")],
                lambda: data[("Close", ticker)],
            ])
        else:
            candidates.append(lambda: data["Close"])
        for getter in candidates:
            try:
                series = getter()
                if isinstance(series, pd.DataFrame):
                    series = series.iloc[:, 0]
                series = series.dropna().astype(float)
                if isinstance(series, pd.Series):
                    return series
            except (KeyError, TypeError, IndexError, AttributeError):
                continue
    except Exception:
        pass
    return pd.Series(dtype=float)

def _num_or_none(v):
    return finite_float(v)

def _fetch_chart_quote(ticker: str) -> tuple[str, dict]:
    symbol = str(ticker or "").strip().upper()
    empty = {"price": None, "chg": None, "name": "", "currency": "", "error": ""}
    if not symbol:
        return symbol, empty

    encoded = urllib.parse.quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=1d&interval=1d"
    last_error = ""
    for attempt in range(HTTP_RETRIES):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            result = (payload.get("chart", {}).get("result") or [None])[0]
            meta = (result or {}).get("meta", {})
            price = _num_or_none(meta.get("regularMarketPrice"))
            prev = _num_or_none(meta.get("chartPreviousClose") or meta.get("previousClose"))
            chg = (price - prev) / prev * 100 if price is not None and prev else None
            name = str(meta.get("shortName") or meta.get("longName") or "").strip()
            currency = str(meta.get("currency") or "").strip()
            return symbol, {
                "price": price,
                "chg": chg,
                "name": name,
                "currency": currency,
                "error": "",
            }
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            retryable = not isinstance(exc, urllib.error.HTTPError) or exc.code == 429 or exc.code >= 500
            if not retryable or attempt + 1 >= HTTP_RETRIES:
                break
            time.sleep(0.4 * (2 ** attempt))
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            break
    return symbol, {**empty, "error": last_error}

def _fetch_quote_batch(batch: list[str]) -> dict[str, dict]:
    """Prix et Var % Yahoo via chart 1d, alignés sur la variation affichée par Yahoo."""
    quotes: dict[str, dict] = {}
    executor = ThreadPoolExecutor(max_workers=10)
    try:
        futures = {executor.submit(_fetch_chart_quote, t): t for t in batch}
        for future in iter_completed(futures, timeout=30):
            try:
                ticker, quote = future.result(timeout=5)
                quotes[str(ticker).upper()] = quote
            except Exception:
                t = futures[future]
                quotes[str(t).upper()] = {"price": None, "chg": None}
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return quotes

def _previous_close(daily_closes, ref_date=None):
    if daily_closes.empty:
        return None
    daily_dates = pd.to_datetime(daily_closes.index).tz_localize(None).normalize()
    if ref_date is not None and len(daily_closes) >= 2 and daily_dates[-1] >= ref_date:
        return float(daily_closes.iloc[-2])
    return float(daily_closes.iloc[-1])

def _price_chg(intraday_closes, daily_closes):
    if intraday_closes.empty and daily_closes.empty:
        return None, None
    if intraday_closes.empty:
        price = float(daily_closes.iloc[-1])
        prev_close = float(daily_closes.iloc[-2]) if len(daily_closes) >= 2 else None
    else:
        price = float(intraday_closes.iloc[-1])
        ref_date = pd.to_datetime(intraday_closes.index).tz_localize(None).normalize()[-1]
        prev_close = _previous_close(daily_closes, ref_date)
    if prev_close:
        return price, (price - prev_close) / prev_close * 100
    return price, None

@st.cache_data(ttl=REFRESH_TTL, show_spinner=False)
def fetch_prices(yf_tickers: tuple[str, ...], refresh_nonce: int = 0) -> dict[str, dict]:
    del refresh_nonce  # Sert uniquement à forcer une nouvelle clé de cache.
    results: dict[str, dict] = {}
    tickers = list(yf_tickers)
    # Priorité au chart Yahoo officiel ; OHLC en fallback si le prix ou la Var % manque.
    for i, batch in enumerate(_chunked(tickers, BATCH_SIZE)):
        quote_data = _fetch_quote_batch(batch)
        missing = [t for t in batch if quote_data.get(t.upper(), {}).get("price") is None or quote_data.get(t.upper(), {}).get("chg") is None]

        intra_data = None
        daily_data = None
        if missing:
            try:
                intra_data = yf.download(tickers=" ".join(missing), period="5d", interval="30m",
                                         auto_adjust=False, progress=False, group_by="ticker",
                                         threads=True, prepost=False)
            except Exception:
                pass
            try:
                daily_data = yf.download(tickers=" ".join(missing), period="10d", interval="1d",
                                         auto_adjust=False, progress=False, group_by="ticker",
                                         threads=True, prepost=False)
            except Exception:
                pass

        multi = len(missing) > 1
        for t in batch:
            quote = quote_data.get(t.upper(), {})
            price, chg = quote.get("price"), quote.get("chg")
            if price is None or chg is None:
                intraday_closes = _closes(intra_data, t, multi) if intra_data is not None else pd.Series(dtype=float)
                daily_closes = _closes(daily_data, t, multi) if daily_data is not None else pd.Series(dtype=float)
                fallback_price, fallback_chg = _price_chg(intraday_closes, daily_closes)
                if price is None:
                    price = fallback_price
                if chg is None:
                    chg = fallback_chg
            results[t] = {
                "price": price,
                "chg": chg,
                "name": quote.get("name", ""),
                "currency": quote.get("currency", ""),
                "error": quote.get("error", ""),
            }
        if i + 1 < (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE:
            time.sleep(YF_BATCH_PAUSE_SEC)
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Calculs métier
# ══════════════════════════════════════════════════════════════════════════════

def compute_upside(price, fair, trim) -> float | None:
    """Upside entre prix actuel et moyenne(Fair, Trim)."""
    try:
        target = (float(fair) + float(trim)) / 2
        return (target - float(price)) / float(price) * 100
    except Exception: return None


def html_screening_zone(price, buy, fair) -> tuple[str, int]:
    """Libellé Buy / Strong Buy destiné à la colonne Score de Screenés."""
    price_value = safe_float(price)
    buy_value = safe_float(buy)
    fair_value = safe_float(fair)
    if (
        price_value is None
        or buy_value is None
        or fair_value is None
        or price_value > fair_value
    ):
        return "—", 0

    if price_value <= buy_value:
        label = "Strong Buy"
        rank = 2
    else:
        label = "Buy"
        rank = 1

    safe_label = html.escape(label, quote=True)
    return (
        f'<span class="screening-zone-label" title="{safe_label}">{safe_label}</span>',
        rank,
    )


def screening_confidence_rank(value) -> int:
    """Rang de confiance utilisé uniquement pour départager les screenings."""
    normalized = _normalize_col(value).replace("-", " ")
    if "tres haute" in normalized or "tres elevee" in normalized:
        return 5
    if "moyenne haute" in normalized:
        return 4
    if "haute" in normalized or "elevee" in normalized:
        return 4
    if "moyenne basse" in normalized:
        return 2
    if "moyenne" in normalized:
        return 3
    if "basse" in normalized or "faible" in normalized:
        return 1
    return 0


def screening_priority(
        zone_rank: int,
        quality,
        price,
        fair,
        confidence,
        screening_date) -> int:
    """Clé de tri : zone, qualité, proximité Fair, confiance, date."""
    quality_value = safe_float(quality)
    quality_rank = round(max(0.0, min(100.0, quality_value or 0.0)))

    proximity_rank = 0
    price_value = safe_float(price)
    fair_value = safe_float(fair)
    if (
        zone_rank == 0
        and price_value is not None
        and fair_value is not None
        and fair_value > 0
    ):
        gap_ratio = max(0.0, (price_value - fair_value) / fair_value)
        proximity_rank = max(0, 10_000 - min(10_000, round(gap_ratio * 10_000)))

    confidence_rank = screening_confidence_rank(confidence)
    date_rank = (
        screening_date.toordinal()
        if isinstance(screening_date, date) and not pd.isna(screening_date)
        else 0
    )

    # Pondérations de tri lexicographiques, toutes sous la précision entière JS.
    return int(
        zone_rank * 1_000_000_000_000_000
        + quality_rank * 1_000_000_000_000
        + proximity_rank * 10_000_000
        + confidence_rank * 1_000_000
        + date_rank
    )


def _canonical_currency(value) -> str:
    """Normalise les codes sans confondre les livres (GBP) et les pence (GBX/GBp)."""
    raw = "" if value is None else str(value).strip()
    if raw == "GBp" or raw.upper() == "GBX":
        return "GBX"
    return raw.upper()


def normalize_quote_price(price, quote_currency, sheet_currency, ticker="") -> float | None:
    """Convertit le cours Yahoo dans l'unité monétaire utilisée par le Sheet."""
    value = safe_float(price)
    if value is None:
        return None

    target_currency = _canonical_currency(sheet_currency)
    source_currency = _canonical_currency(quote_currency)
    if not source_currency and str(ticker or "").strip().upper().endswith(".L"):
        # Le chart Yahoo omet parfois la devise en fallback. Pour Londres, on ne
        # déduit les pence que si le Sheet attend explicitement GBP ou GBX.
        if target_currency in {"GBP", "GBX"}:
            source_currency = "GBX"

    if source_currency == "GBX" and target_currency == "GBP":
        return value / 100
    if source_currency == "GBP" and target_currency == "GBX":
        return value * 100
    return value


def safe_float(v) -> float | None:
    return finite_float(v)


def score_gradient_color(value) -> str | None:
    """Reproduit le dégradé du Sheet pour un score calculé en direct."""
    score = safe_float(value)
    if score is None:
        return None
    stops = (
        (30.0, (255, 0, 0)),
        (50.0, (255, 217, 102)),
        (80.0, (106, 168, 79)),
    )
    if score <= stops[0][0]:
        rgb = stops[0][1]
    elif score >= stops[-1][0]:
        rgb = stops[-1][1]
    else:
        lower, upper = next(
            (left, right)
            for left, right in zip(stops, stops[1:])
            if left[0] <= score <= right[0]
        )
        ratio = (score - lower[0]) / (upper[0] - lower[0])
        rgb = tuple(
            round(start + ratio * (end - start))
            for start, end in zip(lower[1], upper[1])
        )
    return "#{:02x}{:02x}{:02x}".format(*rgb)

# ══════════════════════════════════════════════════════════════════════════════
# Formatage HTML
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(v) -> str:
    value = safe_float(v)
    if value is None: return "—"
    return f"{value:,.0f}" if value > 1_000 else f"{value:,.2f}"


def fmt_target(v, hide_decimals: bool = False) -> str:
    value = safe_float(v)
    if value is None: return "—"
    return f"{value:,.0f}" if hide_decimals else f"{value:,.2f}"

def fmt_note(v) -> str:
    value = safe_float(v)
    if value is None: return "—"
    return str(int(value))

def fmt_maj(maj_date) -> str:
    """
    MAJ rouge si la mise à jour a plus de 30 jours.
    """
    if maj_date is None or (isinstance(maj_date, float) and pd.isna(maj_date)):
        return "—"
    try:
        d = maj_date if isinstance(maj_date, date) else pd.to_datetime(maj_date).date()
        s = d.strftime("%d-%m")
        today = date.today()
        red = (today - d).days > 30
        return f'<span style="color:#ef4444">{s}</span>' if red else s
    except Exception:
        return "—"

def html_var(chg) -> str:
    if chg is None or (isinstance(chg, float) and pd.isna(chg)):
        return '<span style="color:#4a5980">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "+" if chg >= 0 else ""
    return f'<span style="color:{c}">{a}{chg:.2f}%</span>'

def html_upside(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    a = "+" if v >= 0 else ""
    return f"{a}{v:.1f}%"

def fmt_verif(v) -> str:
    if v is None or pd.isna(v):
        return ""
    value = str(v).strip()
    if not value:
        return ""
    if re.fullmatch(r"[+-]?[0-9]+,[0-9]+", value):
        return value.replace(",", ".")
    return value


def html_audit(
        v,
        underwritten: bool,
        screening_key_point=None,
        audit_impact=None,
        analytic_complete: bool = True,
        registry_audit=None) -> tuple[str, int]:
    value = fmt_verif(v)
    normalized = _normalize_col(value)
    registry_value = fmt_verif(registry_audit)
    registry_normalized = _normalize_col(registry_value)
    impact = _normalize_col(fmt_verif(audit_impact))

    if underwritten and impact == "material":
        if analytic_complete:
            color, label, rank = "#facc15", "Actualisation matérielle — nouvel audit requis", 1
        else:
            color, label, rank = "#ef4444", "Non auditable / décision suspendue", -1
    elif normalized == "non auditable" or registry_normalized == "non auditable":
        color, label, rank = "#ef4444", "Non auditable", -1
        if value:
            label += f" — {value}"
    elif value:
        color, label, rank = "#22c55e", "Audité — aucun changement matériel depuis", 2
        label += f" — {value}"
    elif underwritten:
        color, label, rank = "#facc15", "Underwrité mais non audité", 1
    else:
        color, rank = "#f97316", 0
        if screening_key_point is None or pd.isna(screening_key_point):
            label = "Screené uniquement"
        else:
            label = str(screening_key_point).strip() or "Screené uniquement"
    light = (
        f'<span class="audit-light" title="{html.escape(label, quote=True)}" '
        f'role="img" aria-label="{html.escape(label, quote=True)}" '
        f'style="color:{color};background:{color}"></span>'
    )
    return light, rank


def html_score_cell(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        score = max(0.0, min(100.0, float(v)))
    except Exception:
        return "—"
    color = score_gradient_color(score) or "#e5e7eb"
    return (
        '<div class="score-cell" style="background:{}" '
        'title="Score global calculé avec le cours Yahoo : {:.0f}/100" '
        'role="img" aria-label="Score global {:.0f} sur 100">'
        '<span>{:.0f}</span>'
        '</div>'
    ).format(color, score, score, score)

def holding_days(v) -> int | None:
    if v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip():
        return None
    try:
        purchase_date = pd.to_datetime(v, dayfirst=True, errors="raise").date()
        return (date.today() - purchase_date).days
    except Exception:
        return None


def fmt_holding_days(v, required: bool = False) -> str:
    days = holding_days(v)
    if days is None:
        return "N/A" if required else "—"
    if 150 <= days <= 180:
        return f'<span style="color:#ef4444;font-weight:700">{days}</span>'
    return str(days)

def html_ticker_link(yf_ticker: str, gf_ticker: str) -> str:
    encoded_ticker = urllib.parse.quote(str(yf_ticker), safe="") if yf_ticker else ""
    url = f"https://finance.yahoo.com/quote/{encoded_ticker}/" if encoded_ticker else "#"
    label = html.escape(str(gf_ticker))
    return (f'<a href="{url}" target="_blank" rel="noopener" title="Yahoo Finance" '
            f'style="color:#93c5fd;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:.78rem;font-weight:500;text-decoration:none;'
            f'letter-spacing:.02em">{label}</a>')
def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith(("http://", "https://")): return ""
    safe_url = html.escape(u, quote=True)
    return (f'<a href="{safe_url}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
            f'style="color:#93c5fd;font-size:.78rem;font-weight:600;'
            f'text-decoration:none;font-family:monospace">↗</a>')
def html_country_flag(ticker: str) -> str:
    code = country_code(ticker)
    if not code:
        return ""
    return f'<span class="fi fi-{code.lower()} wl-country-flag" title="{code}"></span>'

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict,
               names: dict,
               industries: dict,
               holding_required: bool = False) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t = r.get("yf_ticker")
        yf_s = str(yf_t).strip().upper() if pd.notna(yf_t) else ""
        q = prices.get(yf_s, {})

        yahoo_price = q.get("price")
        price = normalize_quote_price(
            yahoo_price,
            q.get("currency"),
            r.get("currency"),
            yf_s,
        )
        chg = q.get("chg")
        sheet_name = (r.get("name") or "") if pd.notna(r.get("name")) else ""
        name = str(names.get(yf_s, "") or sheet_name)
        name_u = name.upper() if name else ""
        industry = str(industries.get(yf_s, "") or "").strip()

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        target_values = tuple(safe_float(value) for value in (buy, fair, trim, exit_))
        hide_target_decimals = any(
            value is not None and value > 1_000 for value in target_values
        )
        quality = safe_float(r.get("note"))
        score = compute_score(compute_ratio(price, buy, exit_), quality)
        upside = compute_upside(price, fair, trim)
        prompt_version = (
            "" if pd.isna(r.get("prompt_version")) else str(r.get("prompt_version")).strip()
        )
        screened_value = r.get("screened_only", False)
        screened_only = bool(screened_value) if pd.notna(screened_value) else False
        underwritten = not screened_only and (
            bool(prompt_version) or sum(value is not None for value in target_values) >= 3
        )
        analytic_complete = quality is not None and all(
            value is not None for value in target_values
        )
        screening_zone_html, screening_zone_rank = (
            html_screening_zone(price, buy, fair) if screened_only else ("", 0)
        )
        screening_sort_score = (
            screening_priority(
                screening_zone_rank,
                quality,
                price,
                fair,
                r.get("confidence"),
                r.get("last_update"),
            )
            if screened_only
            else score
        )
        audit_html, audit_rank = html_audit(
            r.get("_audit_status"),
            underwritten,
            r.get("screening_key_point"),
            r.get("audit_impact"),
            analytic_complete,
            r.get("verif"),
        )
        days = holding_days(r.get("purchase_date"))
        gf = str(r["gf_ticker"])
        name_html = name_u if name_u else gf
        flagged = bool(r.get("flagged", False))

        rows.append({
            "_score":        screening_sort_score,
            "_chg":          chg,
            "_maj":          r.get("last_update"),
            "_upside":       upside,
            "_quality":      quality,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_flagged":      flagged,
            "_sort": {
                "MAJ": (
                    r.get("last_update").toordinal()
                    if isinstance(r.get("last_update"), date) and not pd.isna(r.get("last_update"))
                    else None
                ),
                "Audit": audit_rank,
                "JRS": None if screened_only else days,
                "Pays": country_code(yf_s),
                "Ticker": gf,
                "Société": name_u,
                "Qual": quality,
                "Prix": price,
                "Upside": upside,
                "Var %": chg,
                "Score": screening_sort_score,
                "Industrie": industry,
            },
            "MAJ":      fmt_maj(r.get("last_update")),
            "Audit":    audit_html,
            "JRS":      (
                "—"
                if screened_only
                else fmt_holding_days(r.get("purchase_date"), holding_required)
            ),
            "Pays":     html_country_flag(yf_s),
            "Ticker":   html_ticker_link(yf_s, gf),
            "Société":  f'<span title="{html.escape(name_u, quote=True)}">{html.escape(name_html)}</span>',
            "Qual":     fmt_note(r.get("note")),
            "Prix":     fmt_price(price),
            "Var %":    html_var(chg),
            "Upside":   html_upside(upside),
            "Score":    (
                screening_zone_html
                if screened_only
                else html_score_cell(score)
            ),
            "Buy":      fmt_target(buy, hide_target_decimals),
            "Fair":     fmt_target(fair, hide_target_decimals),
            "Trim":     fmt_target(trim, hide_target_decimals),
            "Exit":     fmt_target(exit_, hide_target_decimals),
            "Industrie": (
                f'<span title="{html.escape(industry, quote=True)}">'
                f'{html.escape(industry)}</span>'
                if industry else "—"
            ),
        })
    return rows
# ══════════════════════════════════════════════════════════════════════════════
# Tableau HTML
# ══════════════════════════════════════════════════════════════════════════════

CSS = """<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/lipis/flag-icons@7.3.2/css/flag-icons.min.css">
<style>
.wl-wrap {
  overflow-x: auto;
  max-height: none;
  overflow-y: visible;
  border-radius: 10px;
  border: 1px solid #252d3d;
  background: #141824;
  box-shadow: 0 4px 24px rgba(0,0,0,.4);
}
.wl-table {
  width: 100%;
  border-collapse: collapse;
  font-family: 'Inter', sans-serif;
  font-size: .76rem;
  color: #c8d4e8;
  table-layout: fixed;
}
.wl-table thead tr {
  position: sticky;
  top: 0;
  z-index: 2;
  box-shadow: 0 6px 12px rgba(0,0,0,.34);
}
.wl-table th {
  background: #0f1320;
  color: #4a5980;
  font-weight: 600;
  font-size: .7rem;
  letter-spacing: .08em;
  text-transform: uppercase;
  padding: 9px 8px;
  text-align: left;
  border-bottom: 1px solid #252d3d;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.wl-table th.c { text-align: center; }
.wl-table th.sortable {
  cursor: pointer;
  user-select: none;
}
.wl-table th.sortable::after {
  content: "↕";
  margin-left: 4px;
  color: #6f83ad;
  opacity: .55;
}
.wl-table th.sortable[aria-sort="ascending"]::after {
  content: "▲";
  opacity: 1;
  color: #93c5fd;
}
.wl-table th.sortable[aria-sort="descending"]::after {
  content: "▼";
  opacity: 1;
  color: #93c5fd;
}
.wl-table th.sortable:focus-visible {
  outline: 1px solid #3b82f6;
  outline-offset: -2px;
}
.wl-sort-help {
  display: flex;
  align-items: center;
  min-height: 2.35rem;
  padding: 0 4px;
  color: #4a5980;
  font-size: .72rem;
  letter-spacing: .03em;
}
.wl-table td {
  height: 25px;
  padding: 2px 8px;
  border-bottom: 1px solid #1a2030;
  vertical-align: middle;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}
.wl-table td.c { text-align: center; }
.wl-table td.score-col { padding: 0 !important; }
.wl-table tbody tr { height: 25px; }
.wl-table a,
.wl-table a:hover,
.wl-table a:focus,
.wl-table a:visited { text-decoration: none !important; }
.wl-table th.group-start,
.wl-table td.group-start {
  border-left: 1px solid rgba(109,130,168,.28);
}
.wl-table tbody tr:nth-child(even) td { background: rgba(255,255,255,.018); }
.wl-table tbody tr:hover td { background: rgba(59,130,246,.08) !important; }
.wl-flagged td { background: #2d1f5e !important; }
.wl-flagged:hover td { background: #3a2875 !important; }
.screening-zone-label {
  display: inline-block;
  font-size: .63rem;
  font-weight: 600;
  line-height: 1;
  white-space: nowrap;
}
.wl-country-flag {
  display: inline-block;
  width: 15px;
  line-height: 10px;
  border-radius: 2px;
  vertical-align: middle;
}
.audit-light {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  box-shadow: 0 0 7px currentColor;
  vertical-align: middle;
}
.score-cell {
  height: 20px;
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #111827;
  font-size: .74rem;
  font-weight: 800;
  line-height: 1;
}
</style>"""

def _sort_attr(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return html.escape(str(value), quote=True)


def render_table(rows: list[dict], key: str,
                 display_cols: list[str] | None = None) -> None:
    if not rows:
        empty_messages = {
            "pf": "Aucun titre en portefeuille.",
            "wl": "Aucun titre dans la watchlist.",
            "screening": "Aucun screening à afficher.",
        }
        st.info(empty_messages.get(key, "Aucun titre à afficher."))
        return

    cols = display_cols or DISPLAY_COLS
    table_id = f"wl-table-{key}"
    colgroup = "<colgroup>" + "".join(
        f'<col style="width:{COL_WIDTHS.get(c, "auto")}">' for c in cols
    ) + "</colgroup>"

    th_parts = []
    for idx, column in enumerate(cols):
        label = HEADER_LABELS.get(column, column)
        sortable = column in SORTABLE_COLUMNS
        classes = " ".join(filter(None, (
            "c" if column in HEADER_CENTER else "",
            "group-start" if column in GROUP_STARTS else "",
            "sortable" if sortable else "",
        )))
        initial_sort = "descending" if column == "Score" else "none"
        sort_attrs = (
            f' data-column="{idx}" data-sort-type="{SORTABLE_COLUMNS[column]}"'
            f' data-default-direction="{-1 if column == "Score" else 1}"'
            f' aria-sort="{initial_sort}" tabindex="0" role="button"'
            if sortable else ""
        )
        title = f"{label} — cliquer pour trier" if sortable else label

        th_parts.append(
            f'<th class="{classes}" title="{title}"{sort_attrs}>{label}</th>'
        )

    trs = []
    for row in rows:
        row_classes = []
        if row["_flagged"]:
            row_classes.append("wl-flagged")
        row_class = " ".join(row_classes)
        td_parts = []
        for column in cols:
            classes = " ".join(filter(None, (
                "c" if column in CENTER else "",
                "group-start" if column in GROUP_STARTS else "",
                "score-col" if column == "Score" else "",
            )))
            sort_value = _sort_attr(row.get("_sort", {}).get(column))
            td_parts.append(
                f'<td class="{classes}" data-sort-value="{sort_value}">{row[column]}</td>'
            )
        trs.append(f'<tr class="{row_class}">{"".join(td_parts)}</tr>')

    st.markdown(
        CSS + f'<div class="wl-wrap"><table id="{table_id}" class="wl-table">'
        f'{colgroup}<thead><tr>{"".join(th_parts)}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True,
    )

    script = """
<script>
(function () {
  const tableId = __TABLE_ID__;

  function bindSort(attempt) {
    const doc = window.parent.document;
    const table = doc.getElementById(tableId);
    if (!table) {
      if (attempt < 10) {
        window.setTimeout(function () { bindSort(attempt + 1); }, 50);
      }
      return;
    }
    if (table.dataset.sortBound === "1") return;
    table.dataset.sortBound = "1";

    const headers = Array.from(table.querySelectorAll("th.sortable"));
    const tbody = table.tBodies[0];
    const collator = new Intl.Collator("fr", {
      numeric: true,
      sensitivity: "base"
    });
    const initialHeader = headers.find(function (header) {
      return header.getAttribute("aria-sort") !== "none";
    });
    let activeColumn = initialHeader ? Number(initialHeader.dataset.column) : -1;
    let direction = initialHeader?.getAttribute("aria-sort") === "descending" ? -1 : 1;

    function isBlank(value) {
      return value === null || value === undefined || String(value).trim() === "";
    }

    function compareValues(a, b, type) {
      const blankA = isBlank(a);
      const blankB = isBlank(b);
      if (blankA && blankB) return 0;
      if (blankA) return 1;
      if (blankB) return -1;

      if (type === "number") {
        return (Number(a) - Number(b)) * direction;
      }

      if (type === "auto") {
        const numberA = Number(a);
        const numberB = Number(b);
        if (Number.isFinite(numberA) && Number.isFinite(numberB)) {
          return (numberA - numberB) * direction;
        }
      }

      return collator.compare(String(a), String(b)) * direction;
    }

    function sortBy(header) {
      const column = Number(header.dataset.column);
      const type = header.dataset.sortType;
      direction = activeColumn === column
        ? -direction
        : Number(header.dataset.defaultDirection || 1);
      activeColumn = column;

      const rows = Array.from(tbody.rows);
      rows.sort(function (rowA, rowB) {
        const valueA = rowA.cells[column]?.dataset.sortValue ?? "";
        const valueB = rowB.cells[column]?.dataset.sortValue ?? "";
        return compareValues(valueA, valueB, type);
      });
      rows.forEach(function (row) { tbody.appendChild(row); });

      headers.forEach(function (item) {
        item.setAttribute("aria-sort", "none");
      });
      header.setAttribute("aria-sort", direction === 1 ? "ascending" : "descending");
    }

    headers.forEach(function (header) {
      header.addEventListener("click", function () { sortBy(header); });
      header.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          sortBy(header);
        }
      });
    });
  }

  bindSort(0);
})();
</script>
""".replace("__TABLE_ID__", json.dumps(table_id))
    st.iframe(script, height=1, tab_index=-1)

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

def render_tab(rows: list[dict], key: str, display_cols: list[str] | None = None) -> None:
    # Conserve la vue initiale historique : Score du plus grand au plus petit.
    rows.sort(key=lambda row: (
        row["_score"] is None,
        -(row["_score"] or 0),
    ))

    render_table(rows, key=key, display_cols=display_cols)

    missing = [row["_ticker"] for row in rows if not row["_price_ok"]]
    if missing:
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Sheet en premier ───────────────────────────────────────────────────────
force_sheet_refresh = st.session_state.get("last_action") == "refresh"
cached_tickers_df = st.session_state.get("tickers_df")
cached_screening_df = st.session_state.get("screening_df")
cached_audit_statuses = st.session_state.get("audit_statuses")

if (
    cached_tickers_df is not None
    and cached_screening_df is not None
    and cached_audit_statuses is not None
    and not force_sheet_refresh
):
    tickers_df = cached_tickers_df.copy(deep=True)
    screening_df = cached_screening_df.copy(deep=True)
    audit_statuses = dict(cached_audit_statuses)
    data_source = st.session_state.get("data_source", "Google Sheet")
else:
    with st.spinner("Chargement du Google Sheet…"):
        try:
            tickers_df, data_source = load_tickers(force_refresh=force_sheet_refresh)
        except Exception as exc:
            LOGGER.exception("Échec du chargement Google Sheets : Registre")
            if cached_tickers_df is None:
                st.error(str(exc))
                st.stop()
            st.warning(f"Google Sheet indisponible : données précédentes conservées ({exc}).")
            tickers_df = cached_tickers_df.copy(deep=True)
            data_source = st.session_state.get("data_source", "Cache de session")
        try:
            screening_df = load_screening_candidates(
                tickers_df["gf_ticker"],
                force_refresh=force_sheet_refresh,
            )
        except Exception as exc:
            LOGGER.exception("Échec du chargement Google Sheets : Screening")
            if cached_screening_df is not None:
                screening_df = cached_screening_df.copy(deep=True)
                st.warning(f"Screening indisponible : données précédentes conservées ({exc}).")
            else:
                screening_df = _empty_screening_candidates()
                st.warning(str(exc))

        try:
            audit_statuses = load_audit_statuses(force_refresh=force_sheet_refresh)
        except Exception as exc:
            LOGGER.exception("Échec du chargement Google Sheets : Audits")
            if cached_audit_statuses is not None:
                audit_statuses = dict(cached_audit_statuses)
                st.warning(f"Audits indisponibles : données précédentes conservées ({exc}).")
            else:
                # Règle conservatrice : sans preuve dans Audits, aucun feu vert.
                audit_statuses = {}
                st.warning(str(exc))

        st.session_state["tickers_df"] = tickers_df.copy(deep=True)
        st.session_state["screening_df"] = screening_df.copy(deep=True)
        st.session_state["audit_statuses"] = dict(audit_statuses)
        st.session_state["data_source"] = data_source
if tickers_df.empty:
    st.error("L'onglet Registre ne contient aucun titre exploitable.")
    st.stop()

tickers_df["_audit_status"] = (
    tickers_df["gf_ticker"].astype(str).str.strip().str.upper().map(audit_statuses).fillna("")
)
screening_df["_audit_status"] = ""

suspended_underwriting_mask = tickers_df.apply(is_suspended_underwriting, axis=1)
pf_df = tickers_df[tickers_df["portif"] == 1].copy()
wl_df = tickers_df[
    (tickers_df["portif"] != 1) & ~suspended_underwriting_mask
].copy()
to_analyze_df = screening_df.copy()
non_portfolio_count = len(wl_df) + len(to_analyze_df)

# ── CSS global en premier (avant tout élément UI) ─────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Fond & layout ── */
[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"] { background: #0f1117 !important; }
[data-testid="stHeader"] { background: rgba(15,17,23,.85) !important; backdrop-filter: blur(8px); }
.block-container {
  padding-top: 2.4rem !important;
  padding-left: 2.5rem !important;
  padding-right: 2.5rem !important;
  max-width: 100% !important;
}
@media (max-width: 900px) {
  .block-container {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
  }
}
html { font-size: 80%; }
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* ── Header custom ── */
.wl-topbar {
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #161b2a 0%, #111624 100%);
  border: 1px solid #252d3d;
  border-radius: 11px;
  padding: 9px 18px;
  margin-bottom: 8px;
  box-shadow: 0 2px 16px rgba(0,0,0,.4);
}
.wl-stats {
  display: flex;
  align-items: center;
  gap: 0;
  flex: 1;
  justify-content: center;
}
.wl-stat {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 0 19px;
}
.wl-stat + .wl-stat {
  border-left: 1px solid #252d3d;
}
.wl-stat-label {
  font-size: .65rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: .1em;
  color: #4a5980;
  margin-bottom: 2px;
}
.wl-stat-val {
  font-size: 1.2rem;
  font-weight: 700;
  color: #e2e8f4;
  font-variant-numeric: tabular-nums;
}
.wl-stat-val.muted { font-size: 1rem; color: #8899bb; }
.wl-stat-val.ok    { color: #22c55e; }
.wl-stat-val.warn  { color: #fbbf24; }
@media (max-width: 900px) {
  .wl-topbar {
    justify-content: flex-start;
    overflow-x: auto;
    padding: 8px 10px;
  }
  .wl-stats {
    min-width: max-content;
    justify-content: flex-start;
  }
  .wl-stat { padding: 0 11px; }
  .wl-stat-label { letter-spacing: .06em; }
  .stTabs [data-baseweb="tab-list"] {
    overflow-x: auto;
    flex-wrap: nowrap;
  }
}

/* ── Boutons ── */
.stButton > button[kind="primary"] {
  background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
  border: none !important; border-radius: 6px !important;
  color: #fff !important; font-weight: 600 !important;
  font-size: .8rem !important; padding: 0 14px !important;
  box-shadow: 0 2px 8px rgba(59,130,246,.4) !important;
  white-space: nowrap !important;
}
.stButton > button[kind="primary"]:hover { opacity: .88 !important; }
.stButton > button {
  background: #1a1f2e !important; border: 1px solid #252d3d !important;
  border-radius: 6px !important; color: #8899bb !important;
  font-size: .8rem !important; font-weight: 500 !important;
  white-space: nowrap !important;
}
.stButton > button:hover { border-color: #3b82f6 !important; color: #93c5fd !important; }

/* ── Download ── */
.stDownloadButton > button {
  background: #1a1f2e !important; border: 1px solid #252d3d !important;
  border-radius: 6px !important; color: #5a6a8a !important; font-size: .75rem !important;
}

/* ── Onglets ── */
.stTabs [data-baseweb="tab-list"] {
  background: #141824; border-radius: 8px; padding: 3px; gap: 2px;
  border: 1px solid #252d3d;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important; border-radius: 6px !important;
  color: #5a6a8a !important; font-size: .8rem !important;
  font-weight: 500 !important; padding: 5px 14px !important; border: none !important;
}
.stTabs [aria-selected="true"] { background: #252d3d !important; color: #e2e8f4 !important; }


/* ── Misc ── */
hr { border-color: #1e2535 !important; }
.stCaption, .stCaption p { color: #3a4560 !important; font-size: .72rem !important; }
.stWarning {
  background: rgba(251,191,36,.07) !important; border: 1px solid rgba(251,191,36,.3) !important;
  border-radius: 8px !important; color: #fbbf24 !important;
}
.stInfo {
  background: rgba(59,130,246,.07) !important; border: 1px solid rgba(59,130,246,.2) !important;
  border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

st.iframe("""
<script>
(function () {
  const parentWindow = window.parent;
  const parentDocument = parentWindow.document;
  if (parentWindow.__watchlistCacheShortcutBound) return;
  parentWindow.__watchlistCacheShortcutBound = true;

  function shouldIgnore(event) {
    const target = event.target;
    const tag = target && target.tagName ? target.tagName.toLowerCase() : "";
    return tag === "input" || tag === "textarea" || tag === "select" || target?.isContentEditable;
  }

  function redispatchCacheShortcut(target) {
    parentWindow.__watchlistAllowCacheC = true;
    const event = new parentWindow.KeyboardEvent("keydown", {
      key: "c",
      code: "KeyC",
      bubbles: true,
      cancelable: true
    });
    target.dispatchEvent(event);
    parentWindow.__watchlistAllowCacheC = false;
  }

  function handleShortcut(event) {
    if (shouldIgnore(event) || event.ctrlKey || event.metaKey || event.altKey) return;
    const key = (event.key || "").toLowerCase();
    if (key === "c" && !parentWindow.__watchlistAllowCacheC) {
      event.preventDefault();
      event.stopImmediatePropagation();
      return false;
    }
    if (key === "j") {
      event.preventDefault();
      event.stopImmediatePropagation();
      redispatchCacheShortcut(parentDocument);
      redispatchCacheShortcut(parentWindow);
      return false;
    }
  }

  const targets = [
    parentWindow,
    parentDocument,
    parentDocument.documentElement,
    parentDocument.body
  ].filter(Boolean);
  for (const target of targets) {
    target.addEventListener("keydown", handleShortcut, true);
    target.addEventListener("keypress", handleShortcut, true);
    target.addEventListener("keyup", handleShortcut, true);
  }
})();
</script>
""", height=1, tab_index=-1)

# ── Alertes de qualité des sources ────────────────────────────────────────────
def warn_sheet_errors(errors: list[dict], sheet_label: str) -> None:
    if not errors:
        return
    examples = ", ".join(
        f"{item.get('ticker') or 'ligne ' + str(item.get('row'))} · {item.get('column')}"
        for item in errors[:5]
    )
    suffix = f" (+{len(errors) - 5})" if len(errors) > 5 else ""
    st.warning(
        f"{sheet_label} contient {len(errors)} erreur(s) de formule masquée(s) : "
        f"{examples}{suffix}."
    )


def warn_alias_collisions(collisions: dict, sheet_label: str) -> None:
    if not collisions:
        return
    merged_headers = "; ".join(
        f"{target} ← {', '.join(sources)}"
        for target, sources in sorted(collisions.items())
    )
    st.warning(f"En-têtes synonymes fusionnés dans {sheet_label} : {merged_headers}")


dupes = st.session_state.get("ticker_dupes", [])
if dupes:
    tickers_en_double = sorted({d["gf_ticker"] for d in dupes})
    st.warning(f"⚠️ {len(tickers_en_double)} ticker(s) en double : {', '.join(tickers_en_double)}")

warn_sheet_errors(st.session_state.get("sheet_errors", []), "Registre")
warn_sheet_errors(st.session_state.get("screening_sheet_errors", []), "Screening")
warn_sheet_errors(st.session_state.get("audit_sheet_errors", []), "Audits")
warn_alias_collisions(st.session_state.get("column_alias_collisions", {}), "Registre")
warn_alias_collisions(st.session_state.get("screening_alias_collisions", {}), "Screening")
warn_alias_collisions(st.session_state.get("audit_alias_collisions", {}), "Audits")

# ── Header bar : stats + boutons ──────────────────────────────────────────────
last_ts = st.session_state.get("last_fetch_ts", "—")


def _tab_slug_from_label(label) -> str | None:
    text = str(label or "")
    prefixes = {
        "Portefeuille": "portfolio",
        "Watchlist": "watchlist",
        "Screenés": "screening",
    }
    return next((slug for prefix, slug in prefixes.items() if text.startswith(prefix)), None)


def remember_active_tab() -> None:
    slug = _tab_slug_from_label(st.session_state.get("finapp_tabs"))
    if slug:
        st.session_state["active_tab_slug"] = slug


def mark_refresh() -> None:
    """Actualise le Sheet et tous les cours depuis un point unique."""
    remember_active_tab()
    st.session_state.pop("finapp_tabs", None)
    st.session_state["last_action"] = "refresh"
    st.session_state["refresh_nonce"] = time.time_ns()


# L'actualisation est intégrée à la synthèse plutôt que répétée dans les onglets.
stats_col, refresh_col = st.columns(
    [9, 1.45], gap="small", vertical_alignment="center",
)
with stats_col:
    stats_placeholder = st.empty()
with refresh_col:
    st.button(
        "↻ Actualiser",
        key="refresh_all",
        help="Actualiser le Google Sheet et tous les cours",
        width="stretch",
        on_click=mark_refresh,
    )

def render_topbar(pf_count, wl_count, last_ts, ok=None, total=None):
    ok_str   = f"{ok}/{total}" if ok is not None else "…"
    ok_cls   = "ok" if ok == total else "warn" if ok is not None else "muted"
    stats_placeholder.markdown(f"""
<div class="wl-topbar">
  <div class="wl-stats">
    <div class="wl-stat">
      <div class="wl-stat-label">Portefeuille</div>
      <div class="wl-stat-val">{pf_count}</div>
    </div>
    <div class="wl-stat">
      <div class="wl-stat-label">Hors portefeuille</div>
      <div class="wl-stat-val">{wl_count}</div>
    </div>
    <div class="wl-stat">
      <div class="wl-stat-label">Prix récupérés</div>
      <div class="wl-stat-val {ok_cls}">{ok_str}</div>
    </div>
    <div class="wl-stat">
      <div class="wl-stat-label">Mise à jour</div>
      <div class="wl-stat-val muted">{last_ts}</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# Affichage initial (avant fetch)
render_topbar(len(pf_df), non_portfolio_count, last_ts)

def tickers_for(df: pd.DataFrame) -> tuple[str, ...]:
    normalized = (str(t).strip().upper() for t in df["yf_ticker"].dropna())
    return tuple(dict.fromkeys(t for t in normalized if t))

def table_cols_with_holding_days() -> list[str]:
    """Colonnes synthétiques adaptées au Registre SOL."""
    return DISPLAY_COLS

pf_yf = tickers_for(pf_df)
wl_yf = tickers_for(wl_df)
to_analyze_yf = tickers_for(to_analyze_df)
all_yf = tuple(dict.fromkeys((*pf_yf, *wl_yf, *to_analyze_yf)))

last_action = st.session_state.pop("last_action", "")
active_yf = all_yf

# ── 2. Cours, noms et industries Yahoo ───────────────────────────────────────
data_key = all_yf
same_data_key = st.session_state.get("data_key") == data_key
cached_prices = dict(st.session_state.get("prices_data", {}))
quote_attempt_times = dict(st.session_state.get("quote_attempt_times", {}))
fresh_prices: dict[str, dict] = {}

quote_check_time = time.time()
price_scope = (
    active_yf
    if last_action == "refresh"
    else stale_quote_tickers(
        all_yf,
        quote_attempt_times,
        quote_check_time,
        REFRESH_TTL,
    )
)
if price_scope:
    prices_spinner = "Actualisation des cours en temps réel…" if last_action == "refresh" else "Cours en temps réel…"
    refresh_nonce = st.session_state.get("refresh_nonce", 0) if last_action == "refresh" else 0
    with st.spinner(prices_spinner):
        fresh_prices = fetch_prices(price_scope, refresh_nonce)
    for ticker in price_scope:
        quote_attempt_times[ticker] = quote_check_time
    st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

prices = merge_quote_cache(cached_prices, fresh_prices, all_yf)
quote_attempt_times = {
    ticker: quote_attempt_times[ticker]
    for ticker in all_yf
    if ticker in quote_attempt_times
}
st.session_state["prices_data"] = prices
st.session_state["quote_attempt_times"] = quote_attempt_times

# Le endpoint chart fournit généralement le nom avec le prix.
names = dict(st.session_state.get("names_data", {}))
for ticker, quote in fresh_prices.items():
    if quote.get("name"):
        names[ticker] = quote["name"]

had_profile_cache = "profiles_data" in st.session_state
profiles = dict(st.session_state.get("profiles_data", {}))
profile_scope = active_yf if last_action == "refresh" else all_yf
should_resolve_profiles = last_action == "refresh" or not same_data_key or not had_profile_cache
missing_profile_tickers = tuple(
    ticker for ticker in profile_scope
    if not profiles.get(ticker, {}).get("industry") or not names.get(ticker)
)
if should_resolve_profiles and missing_profile_tickers:
    with st.spinner("Industries Yahoo…"):
        profile_nonce = st.session_state.get("refresh_nonce", 0) if last_action == "refresh" else 0
        profiles.update(fetch_profiles(missing_profile_tickers, profile_nonce))

for ticker, profile in profiles.items():
    if not names.get(ticker) and profile.get("name"):
        names[ticker] = profile["name"]

names = {ticker: names[ticker] for ticker in all_yf if ticker in names}
profiles = {ticker: profiles[ticker] for ticker in all_yf if ticker in profiles}
industries = {
    ticker: profiles.get(ticker, {}).get("industry", "")
    for ticker in all_yf
}
st.session_state["names_data"] = names
st.session_state["profiles_data"] = profiles
st.session_state["data_key"] = data_key

last_ts = st.session_state.get("last_fetch_ts", "—")

ok = sum(1 for t in all_yf if prices.get(t, {}).get("price") is not None)
stale_price_tickers = [
    ticker for ticker in all_yf if prices.get(ticker, {}).get("_stale", False)
]
if stale_price_tickers:
    preview = ", ".join(stale_price_tickers[:8])
    suffix = f" (+{len(stale_price_tickers) - 8})" if len(stale_price_tickers) > 8 else ""
    st.warning(
        "Yahoo n'a pas répondu pour certaines cotations : anciennes valeurs conservées "
        f"pour {preview}{suffix}."
    )

# Mise à jour du topbar avec les prix récupérés
render_topbar(len(pf_df), non_portfolio_count, last_ts, ok=ok, total=len(all_yf))

# Construire les rows des vues une seule fois
rows_pf = build_rows(pf_df, prices, names, industries, True)
rows_wl = build_rows(wl_df, prices, names, industries, False)
rows_to_analyze = build_rows(to_analyze_df, prices, names, industries, False)

tab_labels = [
    f"Portefeuille ({len(pf_df)})",
    f"Watchlist ({len(wl_df)})",
    f"Screenés ({len(to_analyze_df)})",
]
active_tab_slug = st.session_state.get("active_tab_slug", "portfolio")
default_tab = next(
    (label for label in tab_labels if _tab_slug_from_label(label) == active_tab_slug),
    tab_labels[0],
)
tab1, tab2, tab3 = st.tabs(
    tab_labels,
    default=default_tab,
    key="finapp_tabs",
    on_change=remember_active_tab,
)
main_cols = table_cols_with_holding_days()
with tab1:
    render_tab(rows_pf, key="pf", display_cols=main_cols)
with tab2:
    render_tab(rows_wl, key="wl", display_cols=main_cols)
with tab3:
    render_tab(rows_to_analyze, key="screening", display_cols=main_cols)


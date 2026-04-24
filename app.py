from __future__ import annotations

import io
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import openpyxl
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Watchlist", page_icon=None, layout="wide",
                   initial_sidebar_state="collapsed")

SHEET_ID      = "1KQ0eolfB-UH-N-jQo2WDxsmVNT3I4IhiTEbdIfcPvbA"
SHEET_NAME    = "Travail"
SHEET_CSV_URL = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
                 f"/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}")
CSV_FALLBACK      = "tickers.csv"
AUTO_REFRESH_SEC  = 15 * 60
REFRESH_TTL       = AUTO_REFRESH_SEC
SHEET_TTL         = 3_600
NAME_TTL          = 7 * 86_400
BE_TTL            = 86_400
SPARK_TTL         = 6 * 3_600
BATCH_SIZE        = 50
YF_META_BATCH_SIZE = 10
YF_BATCH_PAUSE_SEC = 0.2

STATUT_ORDER = {"Strong buy": 0, "Buy": 1, "Fair": 2, "Trim": 3, "Exit": 4, "": 9}
STATUT_COLOR = {
    "Strong buy": "#1f8b4c", "Buy": "#6dbf4b", "Fair": "#d4b000",
    "Trim": "#e67e22",       "Exit": "#c0392b", "": "#64748b",
}

# ══════════════════════════════════════════════════════════════════════════════
# Colonnes & layout — identiques entre onglets
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "MAJ", "Ticker", "Société", "Prix", "Var %", "Upside", "Spark",
    "Score", "Buy", "Fair", "Trim", "Exit", "Qualité", "Beta",
    "Statut", "Earnings", "↗",
]
COL_WIDTHS = {
    "MAJ": "92px", "Ticker": "82px", "Société": "210px",
    "Prix": "78px", "Var %": "80px", "Upside": "72px", "Spark": "88px",
    "Score": "52px", "Buy": "74px", "Fair": "74px", "Trim": "74px", "Exit": "74px",
    "Qualité": "58px", "Beta": "56px", "Statut": "90px",
    "Earnings": "98px", "↗": "36px",
}
CENTER = {"MAJ", "Prix", "Var %", "Upside", "Spark", "Score",
          "Buy", "Fair", "Trim", "Exit", "Qualité", "Beta",
          "Statut", "Earnings", "↗"}

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def normalize_col(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", str(s))
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").strip().lower()

def parse_num(v) -> float | None:
    if v is None: return None
    s = str(v).strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    if not s or s in ("#REF!", "#N/A", "#VALUE!", "#ERROR!", "—", ""): return None
    if re.match(r"^\d{1,3}(,\d{3})+$", s): return float(s.replace(",", ""))
    if re.match(r"^\d{1,3}(,\d{3})+,\d{1,2}$", s):
        parts = s.split(","); return float("".join(parts[:-1]) + "." + parts[-1])
    if "," in s: return float(s.replace(".", "").replace(",", "."))
    if re.match(r"^\d{1,3}(\.\d{3})+$", s): return float(s.replace(".", ""))
    try: return float(s)
    except ValueError: return None

def stockopedia_url(gf_ticker: str, name: str) -> str:
    """Construit l'URL Stockopedia depuis le nom + ticker GF."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") if name else ""
    if slug:
        return f"https://www.stockopedia.com/share-prices/{slug}/{gf_ticker}/"
    # Fallback : recherche Stockopedia
    sym = gf_ticker.split(":")[-1]
    return f"https://www.stockopedia.com/search/?q={sym}"

# ══════════════════════════════════════════════════════════════════════════════
# Chargement du sheet
# ══════════════════════════════════════════════════════════════════════════════

SHEET_COL_NORMALIZED = {
    "ticker":      "gf_ticker",
    "societe":     "name",
    "portif":      "portif",
    "note":        "note",
    "buy":         "buy",
    "fair":        "fair",
    "trim":        "trim",
    "exit":        "exit",
    "url":         "url",
    "spot":        "spot_sheet",
    "score mixte": "score_sheet",
    "last update": "last_update",
    "yf ticker":   "yf_ticker",
}
NUMERIC_COLS = ["note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet"]


def _normalize_col(s: str) -> str:
    """Normalisation agressive : supprime BOM, accents, espaces, casse."""
    s = str(s).replace("\ufeff", "").replace("\u202f", "").replace("\xa0", "")
    nfkd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()


def load_tickers() -> tuple[pd.DataFrame, str]:
    """Toujours re-fetché depuis le sheet — pas de cache."""
    import time as _t
    bust  = int(_t.time())
    url   = SHEET_CSV_URL + f"&_cb={bust}"
    source = "Google Sheet"
    df = None

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(url, encoding=enc, header=0, dtype=str)
            if not df.empty:
                break
        except Exception:
            continue

    # Essai 2 : CSV local
    if df is None or df.empty:
        try:
            df = pd.read_csv(CSV_FALLBACK, encoding="utf-8-sig", header=0, dtype=str)
            source = "tickers.csv (fallback)"
        except Exception as exc:
            raise RuntimeError(f"Impossible de charger les données : {exc}") from exc

    # Renommage robuste avec normalisation agressive
    rename_map: dict[str, str] = {}
    for col in df.columns:
        norm = _normalize_col(col)
        if norm in SHEET_COL_NORMALIZED:
            rename_map[col] = SHEET_COL_NORMALIZED[norm]
    df = df.rename(columns=rename_map)

    # Colonnes manquantes → NA
    for col in SHEET_COL_NORMALIZED.values():
        if col not in df.columns:
            df[col] = pd.NA

    # Si gf_ticker est toujours vide, essai positionnel (col C = index 2)
    if df["gf_ticker"].isna().all() and len(df.columns) > 2:
        candidate = df.iloc[:, 2].dropna().astype(str)
        # Vérifier que ça ressemble à des tickers (pas de valeurs numériques pures)
        looks_like_tickers = candidate.str.match(r"^[A-Z0-9:\.\-]+$").sum() > len(candidate) * 0.5
        if looks_like_tickers:
            df["gf_ticker"] = df.iloc[:, 2]

    # Colonne A = case à cocher (TRUE/FALSE) — lue par position avant tout renommage
    df["flagged"] = df.iloc[:, 0].apply(
        lambda v: str(v).strip().upper() in ("TRUE", "1", "VRAI")
    )

    # Nettoyage
    df = df[df["gf_ticker"].notna()].copy()
    df = df[~df["gf_ticker"].astype(str).str.strip().isin(
        ["", "Ticker", "gf_ticker", "nan", "None"])].copy()

    if df.empty:
        raise RuntimeError(
            f"DataFrame vide après filtrage. Colonnes trouvées : "
            f"{[_normalize_col(c) for c in rename_map.keys() or ['(aucune)']]}. "
            f"Colonnes brutes du CSV : voir onglet Debug."
        )

    df["portif"] = df["portif"].map(
        lambda v: 1 if str(v).strip() in ("1", "TRUE", "True", "true") else 0)
    df["name"] = df["name"].apply(
        lambda v: "" if (pd.isna(v) or str(v).strip().startswith("#")) else str(v).strip())
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_num)
    if "last_update" in df.columns:
        df["last_update"] = pd.to_datetime(
            df["last_update"], format="%d/%m/%Y", errors="coerce").dt.date
    else:
        df["last_update"] = None

    # yf_ticker : lu directement depuis le sheet (colonne "yf ticker")
    # Si absent ou vide, on utilise gf_ticker comme fallback (même ticker)
    if "yf_ticker" not in df.columns or df["yf_ticker"].isna().all():
        df["yf_ticker"] = df["gf_ticker"].astype(str)
    else:
        df["yf_ticker"] = df["yf_ticker"].where(
            df["yf_ticker"].notna() &
            ~df["yf_ticker"].astype(str).str.strip().isin(["", "nan", "None"]),
            other=df["gf_ticker"].astype(str)
        )

    # Détection des doublons
    dupes = df[df["gf_ticker"].duplicated(keep=False)][["gf_ticker", "yf_ticker"]].copy()
    st.session_state["ticker_dupes"] = dupes.to_dict("records") if not dupes.empty else []

    return df.reset_index(drop=True), source

# ══════════════════════════════════════════════════════════════════════════════
# Métadonnées (nom, beta, earnings) — parallèle, cache 24h
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_name(t: str) -> tuple[str, str]:
    """Récupère uniquement le nom — rapide, via history_metadata."""
    try:
        tk = yf.Ticker(t)
        tk.history(period="2d", interval="1d")
        meta = getattr(tk, "history_metadata", None) or {}
        name = (meta.get("shortName") or meta.get("longName") or "").strip()
        if not name:
            info = tk.fast_info
            name = (getattr(info, "shortName", None) or "").strip()
        return t, name
    except Exception:
        return t, ""

@st.cache_data(ttl=NAME_TTL, show_spinner=False)
def fetch_name_cached(ticker: str) -> str:
    return _fetch_one_name(ticker)[1]

def fetch_names(yf_tickers: tuple[str, ...]) -> dict[str, str]:
    import time
    names: dict[str, str] = {}
    tickers = list(yf_tickers)
    # Requetes unitaires Yahoo : petits batches + courte pause pour limiter la pression.
    for i in range(0, len(tickers), YF_META_BATCH_SIZE):
        batch = tickers[i: i + YF_META_BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_name_cached, t): t for t in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    t = futures[future]
                    names[t] = future.result(timeout=15)
                except Exception:
                    names[futures[future]] = ""
        if i + YF_META_BATCH_SIZE < len(tickers):
            time.sleep(YF_BATCH_PAUSE_SEC)
    return names


def _coerce_date(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        try:
            return value.date()
        except Exception:
            return None
    if isinstance(value, (list, tuple, set)):
        dates = [_coerce_date(v) for v in value]
        dates = [d for d in dates if d is not None]
        return min(dates) if dates else None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc).date()
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s[:10], fmt).date()
            except Exception:
                continue
    return None


def _extract_earnings_from_calendar(cal):
    if cal is None:
        return None
    if isinstance(cal, dict):
        for key in ("Earnings Date", "EarningsDate"):
            if key in cal:
                return _coerce_date(cal.get(key))
        return None
    if hasattr(cal, "loc"):
        for key in ("Earnings Date", "EarningsDate"):
            try:
                return _coerce_date(cal.loc[key].iloc[0])
            except Exception:
                continue
    return None


def _pick_earnings_dates(candidates: list[date]) -> tuple[date | None, date | None]:
    today = date.today()
    clean = sorted({d for d in candidates if isinstance(d, date)})
    last_earnings = max((d for d in clean if d < today), default=None)
    next_earnings = min((d for d in clean if d >= today), default=None)
    return next_earnings, last_earnings


def _fetch_one_be(t: str) -> tuple[str, dict]:
    """Récupère beta + earnings — plus lent, via .info et .calendar."""
    result: dict = {"beta": None, "earnings": None, "_diag": []}
    try:
        tk = yf.Ticker(t)
        info = {}
        try:
            info = tk.info or {}
            b = info.get("beta")
            if b is None:
                b = info.get("beta3Year")
            if b is not None:
                result["beta"] = float(b)
                result["_diag"].append("beta:info")
            else:
                result["_diag"].append("beta:missing")
        except Exception:
            result["_diag"].append("beta:info_error")
        try:
            result["earnings"] = _extract_earnings_from_calendar(tk.calendar)
            if result["earnings"] is not None:
                result["_diag"].append("earnings:calendar")
        except Exception:
            result["_diag"].append("earnings:calendar_error")

        if result["earnings"] is None:
            try:
                for key in ("earningsTimestamp", "earningsDate",
                            "earningsTimestampStart", "earningsTimestampEnd"):
                    parsed = _coerce_date(info.get(key))
                    if parsed is not None:
                        result["earnings"] = parsed
                        result["_diag"].append(f"earnings:info:{key}")
                        break
            except Exception:
                result["_diag"].append("earnings:info_error")

        if result["earnings"] is None:
            try:
                earn_df = tk.get_earnings_dates(limit=4)
                if earn_df is not None and len(earn_df.index) > 0:
                    parsed = _coerce_date(list(earn_df.index))
                    if parsed is not None:
                        result["earnings"] = parsed
                        result["_diag"].append("earnings:get_earnings_dates")
            except Exception:
                result["_diag"].append("earnings:get_earnings_dates_error")

        if result["earnings"] is None:
            result["_diag"].append("earnings:missing")
    except Exception:
        result["_diag"].append("ticker:init_error")
    return t, result

@st.cache_data(ttl=BE_TTL, show_spinner=False)
def fetch_be_cached(ticker: str) -> dict:
    return _fetch_one_be(ticker)[1]

def fetch_be(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    """Beta + Earnings — déclenchement manuel via bouton."""
    import time
    results: dict[str, dict] = {}
    empty = {"beta": None, "earnings": None}
    tickers = list(yf_tickers)
    for i in range(0, len(tickers), YF_META_BATCH_SIZE):
        batch = tickers[i: i + YF_META_BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_be_cached, t): t for t in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    t = futures[future]
                    results[t] = future.result(timeout=15)
                except Exception:
                    results[futures[future]] = dict(empty)
        if i + YF_META_BATCH_SIZE < len(tickers):
            time.sleep(YF_BATCH_PAUSE_SEC)
    st.session_state["be_debug"] = [
        {
            "ticker": t,
            "beta": data.get("beta"),
            "earnings": data.get("earnings"),
            "diag": ", ".join(data.get("_diag", [])),
        }
        for t, data in results.items()
        if data.get("beta") is None or data.get("earnings") is None
    ]
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Cours Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def _chunked(items, size):
    for i in range(0, len(items), size): yield items[i: i + size]

def _closes(data, ticker, multi):
    try:
        s = data[ticker]["Close"] if multi else data["Close"]
        s = s.dropna().astype(float)
        return s if isinstance(s, pd.Series) else pd.Series(dtype=float)
    except Exception: return pd.Series(dtype=float)

def _price_chg(closes):
    if closes.empty: return None, None
    price = float(closes.iloc[-1])
    dates = pd.to_datetime(closes.index).tz_localize(None).normalize()
    prev = closes[dates < dates[-1]]
    chg = None
    if not prev.empty:
        p0 = float(prev.iloc[-1])
        if p0: chg = (price - p0) / p0 * 100
    return price, chg

@st.cache_data(ttl=REFRESH_TTL, show_spinner=False)
def fetch_prices(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    import time
    results: dict[str, dict] = {}
    tickers = list(yf_tickers)
    # Requetes mutualisees Yahoo : batches plus gros, mais on garde une pause courte.
    for i, batch in enumerate(_chunked(tickers, BATCH_SIZE)):
        try:
            data = yf.download(tickers=" ".join(batch), period="5d", interval="30m",
                               auto_adjust=False, progress=False, group_by="ticker",
                               threads=True, prepost=False)
        except Exception:
            for t in batch: results[t] = {"price": None, "chg": None}
            continue
        multi = len(batch) > 1
        for t in batch:
            price, chg = _price_chg(_closes(data, t, multi))
            results[t] = {"price": price, "chg": chg}
        if i + 1 < (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE:
            time.sleep(YF_BATCH_PAUSE_SEC)
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Sparklines 52 semaines — cache 24h
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=SPARK_TTL, show_spinner=False)
def fetch_sparklines(yf_tickers: tuple[str, ...]) -> dict[str, list[float]]:
    import time
    result: dict[str, list[float]] = {}
    tickers = list(yf_tickers)
    for i, batch in enumerate(_chunked(tickers, BATCH_SIZE)):
        try:
            data = yf.download(tickers=" ".join(batch), period="1y", interval="1wk",
                               auto_adjust=True, progress=False, group_by="ticker",
                               threads=True)
            multi = len(batch) > 1
            for t in batch:
                try:
                    s = data[t]["Close"] if multi else data["Close"]
                    closes = s.dropna().astype(float).tolist()
                    if len(closes) >= 4:
                        result[t] = closes
                except Exception:
                    pass
        except Exception:
            pass
        if i + 1 < (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE:
            time.sleep(YF_BATCH_PAUSE_SEC)
    return result

# ══════════════════════════════════════════════════════════════════════════════
# Calculs métier
# ══════════════════════════════════════════════════════════════════════════════

def compute_statut(price, buy, fair, trim, exit_) -> str:
    if any(v is None or (isinstance(v, float) and pd.isna(v))
           for v in [price, buy, fair, trim, exit_]): return ""
    p, b, f, k, e = float(price), float(buy), float(fair), float(trim), float(exit_)
    if p <= b: return "Strong buy"
    if p <= f: return "Buy"
    if p <= k: return "Fair"
    if p <= e: return "Trim"
    return "Exit"

def compute_ratio(price, buy, exit_) -> float | None:
    try:
        p, b, e = float(price), float(buy), float(exit_)
        if e <= b: return None
        return max(0.0, min(1.0, (e - p) / (e - b)))
    except Exception: return None

def compute_score(ratio, note) -> float | None:
    try: return (0.6 * float(ratio) + 0.4 * float(note) / 100) * 100
    except Exception: return None

def compute_upside(price, fair, trim) -> float | None:
    """Upside entre prix actuel et moyenne(Fair, Trim)."""
    try:
        target = (float(fair) + float(trim)) / 2
        return (target - float(price)) / float(price) * 100
    except Exception: return None

# ══════════════════════════════════════════════════════════════════════════════
# Formatage HTML
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{float(v):,.2f}"

def fmt_note(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return str(int(float(v)))

def fmt_score(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return str(round(float(v)))

def fmt_beta(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{float(v):.2f}"

def fmt_maj(maj_date, earnings_date) -> str:
    """
    MAJ rouge si :
    - Earnings existe ET est dans le passé ET MAJ < Earnings
      (l'analyse précède les derniers résultats publiés → potentiellement obsolète)
    - Pas d'Earnings ET MAJ > 30 jours
    """
    if maj_date is None or (isinstance(maj_date, float) and pd.isna(maj_date)):
        return "—"
    try:
        d = maj_date if isinstance(maj_date, date) else pd.to_datetime(maj_date).date()
        s = d.strftime("%d-%m-%Y")
        today = date.today()
        red = False
        if earnings_date is not None:
            # Seulement si les earnings sont dans le passé
            if earnings_date < today:
                red = d < earnings_date  # analyse antérieure aux derniers résultats
        else:
            red = (today - d).days > 30
        return f'<span style="color:#ef4444">{s}</span>' if red else s
    except Exception:
        return "—"

def fmt_earnings(d) -> str:
    if d is None or (isinstance(d, float) and pd.isna(d)): return "—"
    try:
        if not isinstance(d, date): d = pd.to_datetime(d).date()
        return d.strftime("%d-%m-%Y")
    except Exception: return "—"

def html_var(chg) -> str:
    if chg is None or (isinstance(chg, float) and pd.isna(chg)):
        return '<span style="color:#4a5980">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "+" if chg >= 0 else ""
    return f'<span style="color:{c}">{a}{chg:.2f}%</span>'

def html_upside(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '<span style="color:#4a5980">—</span>'
    c = "#22c55e" if v >= 0 else "#ef4444"
    a = "+" if v >= 0 else ""
    return f'<span style="color:{c};font-weight:600">{a}{v:.1f}%</span>'

def html_statut(statut) -> str:
    cfg = {
        "Strong buy": ("#22c55e", "rgba(34,197,94,.15)"),
        "Buy":        ("#86efac", "rgba(134,239,172,.12)"),
        "Fair":       ("#fbbf24", "rgba(251,191,36,.12)"),
        "Trim":       ("#f97316", "rgba(249,115,22,.12)"),
        "Exit":       ("#ef4444", "rgba(239,68,68,.12)"),
    }
    color, bg = cfg.get(statut, ("#4a5980", "transparent"))
    if not statut:
        return '<span style="color:#4a5980">—</span>'
    return (f'<span style="color:{color};background:{bg};'
            f'padding:2px 8px;border-radius:20px;font-size:.75rem;'
            f'font-weight:600;white-space:nowrap">{statut}</span>')

def html_sparkline(closes: list[float]) -> str:
    if not closes or len(closes) < 4: return ""
    mn, mx = min(closes), max(closes)
    if mx == mn: return ""
    w, h = 84, 24
    pts = " ".join(
        f"{i / (len(closes) - 1) * w:.1f},{h - (v - mn) / (mx - mn) * (h - 4) - 2:.1f}"
        for i, v in enumerate(closes)
    )
    up    = closes[-1] >= closes[0]
    color = "#22c55e" if up else "#ef4444"
    # Zone remplie sous la courbe
    first_x = "0"
    last_x  = str(w)
    fill_pts = f"0,{h} {pts} {last_x},{h}"
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'style="display:inline-block;vertical-align:middle;opacity:.9">'
        f'<polygon points="{fill_pts}" fill="{color}" fill-opacity=".12"/>'
        f'<polyline points="{pts}" fill="none" stroke="{color}" '
        f'stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>'
        f'</svg>'
    )

def html_ticker_link(yf_ticker: str, gf_ticker: str) -> str:
    url = f"https://finance.yahoo.com/quote/{yf_ticker}/" if yf_ticker else "#"
    return (f'<a href="{url}" target="_blank" rel="noopener" title="Yahoo Finance" '
            f'style="color:#93c5fd;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:.78rem;font-weight:500;text-decoration:none;'
            f'letter-spacing:.02em">{gf_ticker}</a>')

def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith("http"): return ""
    return (f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
            f'style="color:#93c5fd;font-size:.78rem;font-weight:600;'
            f'text-decoration:none;font-family:monospace">↗</a>')

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict,
               names: dict, be_data: dict, sparklines: dict,
               highlight_radar: bool = False) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t   = r.get("yf_ticker")
        yf_s   = str(yf_t) if pd.notna(yf_t) else ""
        q      = prices.get(yf_s, {})
        be     = be_data.get(yf_s, {})

        price  = q.get("price") or (r.get("spot_sheet") if pd.notna(r.get("spot_sheet")) else None)
        chg    = q.get("chg")
        name   = (r.get("name") or "") if pd.notna(r.get("name")) else ""
        name   = name or names.get(yf_s, "")
        name_u = name.upper() if name else ""

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        statut  = compute_statut(price, buy, fair, trim, exit_)
        ratio   = compute_ratio(price, buy, exit_)
        score   = compute_score(ratio, r.get("note"))
        if score is None and pd.notna(r.get("score_sheet")):
            score = r.get("score_sheet")
        upside  = compute_upside(price, fair, trim)
        beta    = be.get("beta")
        earnings = be.get("earnings")
        sparks  = sparklines.get(yf_s, [])

        gf = str(r["gf_ticker"])
        name_html = name_u if name_u else f'<span style="color:#475569;font-style:italic">{gf}</span>'

        # Mise en surbrillance "sous le radar"
        radar   = (highlight_radar and score is not None and float(score) >= 85)
        flagged = bool(r.get("flagged", False))

        rows.append({
            "_statut_order": STATUT_ORDER.get(statut, 9),
            "_score":        float(score) if score is not None else -1.0,
            "_chg":          chg,
            "_maj":          r.get("last_update"),
            "_upside":       upside if upside is not None else -999.0,
            "_beta":         float(beta) if beta is not None else None,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_statut":       statut,
            "_radar":        radar,
            "_flagged":      flagged,
            # Données brutes pour export XLS
            "_raw": {
                "MAJ": r.get("last_update").strftime("%d-%m-%Y") if pd.notna(r.get("last_update")) and r.get("last_update") else "",
                "Ticker":   gf, "Société": name_u,
                "Prix":     price, "Var %": chg, "Upside %": upside,
                "Score":    round(float(score)) if score is not None else "",
                "Buy":      buy, "Fair":  fair, "Trim":  trim, "Exit":  exit_,
                "Qualité":  int(float(r["note"])) if r.get("note") and pd.notna(r["note"]) else "",
                "Beta":     beta,
                "Statut":   statut,
                "Earnings": earnings.strftime("%d-%m-%Y") if earnings else "",
            },
            # HTML
            "MAJ":      fmt_maj(r.get("last_update"), earnings),
            "Ticker":   html_ticker_link(yf_s, gf),
            "Société":  f'<span title="{name_u}">{name_html}</span>',
            "Prix":     fmt_price(price),
            "Var %":    html_var(chg),
            "Upside":   html_upside(upside),
            "Spark":    html_sparkline(sparks),
            "Score":    fmt_score(score),
            "Buy":      fmt_price(buy),
            "Fair":     fmt_price(fair),
            "Trim":     fmt_price(trim),
            "Exit":     fmt_price(exit_),
            "Qualité":  fmt_note(r.get("note")),
            "Beta":     fmt_beta(beta),
            "Statut":   html_statut(statut),
            "Earnings": fmt_earnings(earnings),
            "↗":        html_link(r.get("url")),
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# Export XLS
# ══════════════════════════════════════════════════════════════════════════════

def export_xlsx(rows: list[dict]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    cols = ["MAJ", "Ticker", "Société", "Prix", "Var %", "Upside %",
            "Score", "Buy", "Fair", "Trim", "Exit",
            "Qualité", "Beta", "Statut", "Earnings"]
    ws.append(cols)
    for r in rows:
        raw = r["_raw"]
        ws.append([raw.get(c, "") for c in cols])
    # Largeurs
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 14
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# ══════════════════════════════════════════════════════════════════════════════
# Tableau HTML
# ══════════════════════════════════════════════════════════════════════════════

CSS = """<style>
.wl-wrap {
  overflow-x: auto;
  max-height: 70vh;
  overflow-y: auto;
  border-radius: 12px;
  border: 1px solid #252d3d;
  background: #141824;
  box-shadow: 0 4px 24px rgba(0,0,0,.4);
}
.wl-table {
  width: 100%;
  border-collapse: collapse;
  font-family: 'Inter', sans-serif;
  font-size: .82rem;
  color: #c8d4e8;
  table-layout: fixed;
}
.wl-table thead tr { position: sticky; top: 0; z-index: 2; }
.wl-table th {
  background: #0f1320;
  color: #4a5980;
  font-weight: 600;
  font-size: .7rem;
  letter-spacing: .08em;
  text-transform: uppercase;
  padding: 11px 10px;
  text-align: left;
  border-bottom: 1px solid #252d3d;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.wl-table th.c { text-align: center; }
.wl-table td {
  padding: 7px 10px;
  border-bottom: 1px solid #1a2030;
  vertical-align: middle;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}
.wl-table td.c { text-align: center; }
.wl-table tbody tr:nth-child(even) td { background: rgba(255,255,255,.018); }
.wl-table tbody tr:hover td { background: rgba(59,130,246,.08) !important; }
.wl-radar td { background: rgba(34,197,94,.07) !important; }
.wl-radar:hover td { background: rgba(34,197,94,.12) !important; }
.wl-flagged td { background: #2d1f5e !important; }
.wl-flagged:hover td { background: #3a2875 !important; }
</style>"""

def render_table(rows: list[dict]) -> None:
    if not rows: st.info("Aucun titre."); return
    colgroup = "<colgroup>" + "".join(
        f'<col style="width:{COL_WIDTHS.get(c,"auto")}">' for c in DISPLAY_COLS
    ) + "</colgroup>"
    th = "".join(
        f'<th class="{"c" if c in CENTER else ""}" title="{c}">{c}</th>'
        for c in DISPLAY_COLS
    )
    trs = []
    for r in rows:
        if r["_flagged"]:
            cls = "wl-flagged"
        elif r["_radar"]:
            cls = "wl-radar"
        else:
            cls = ""
        tds = "".join(
            f'<td class="{"c" if c in CENTER else ""}">{r[c]}</td>'
            for c in DISPLAY_COLS
        )
        trs.append(f'<tr class="{cls}">{tds}</tr>')
    st.markdown(
        CSS + f'<div class="wl-wrap"><table class="wl-table">'
        f'{colgroup}<thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

def render_tab(rows: list[dict], key: str) -> None:
    c1, c2 = st.columns([1, 1])
    with c1:
        sort_choice = st.selectbox("Tri", [
            "Statut + Score", "Ticker A→Z", "Score ↓", "Qualité ↓",
            "Upside ↓", "Var % ↑", "Var % ↓", "MAJ ↓", "Beta ↓",
        ], key=f"{key}_t")
    with c2:
        sf = st.selectbox("Statut",
            ["Tous", "Strong buy", "Buy", "Fair", "Trim", "Exit"], key=f"{key}_f")

    if sf != "Tous":
        rows = [r for r in rows if r["_statut"] == sf]

    sort_map = {
        "Statut + Score": lambda r: (r["_statut_order"], -r["_score"]),
        "Ticker A→Z":     lambda r: r["_ticker"],
        "Score ↓":        lambda r: -r["_score"],
        "Qualité ↓":      lambda r: -r["_score"],
        "Upside ↓":       lambda r: -r["_upside"],
        "Var % ↑":        lambda r: (r["_chg"] is None, -(r["_chg"] or 0)),
        "Var % ↓":        lambda r: (r["_chg"] is None, r["_chg"] or 0),
        "MAJ ↓":          lambda r: r["_maj"] or date.max,
        "Beta ↓":         lambda r: (r["_beta"] is None, -(r["_beta"] or 0)),
    }
    key_fn = sort_map.get(sort_choice)
    if key_fn:
        rows.sort(key=key_fn, reverse=(sort_choice == "MAJ ↓"))

    render_table(rows)

    missing = [r["_ticker"] for r in rows if not r["_price_ok"]]
    if missing:
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

    # Export Excel — sous le tableau, aligné à droite, avec espacement
    if rows:
        st.markdown("<div style='margin-top:12px'></div>", unsafe_allow_html=True)
        _, right = st.columns([3, 1])
        with right:
            xls_bytes = export_xlsx(rows)
            st.download_button(
                "Export Excel", data=xls_bytes,
                file_name=f"watchlist_{key}_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key}_xls",
                use_container_width=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# Onglet Debug
# ══════════════════════════════════════════════════════════════════════════════

def render_debug(tickers_df: pd.DataFrame, prices: dict, names: dict, be_data: dict) -> None:
    st.subheader("Diagnostic colonnes")
    st.write(f"**{len(tickers_df)} titres chargés.** Colonnes internes :")
    st.code(str(list(tickers_df.columns)))

    # Affichage brut CSV pour vérifier les noms originaux
    with st.expander("Colonnes brutes du CSV (2 premières lignes)"):
        try:
            df_raw = pd.read_csv(SHEET_CSV_URL, encoding="utf-8-sig", header=0,
                                 dtype=str, nrows=2)
        except Exception:
            try:
                df_raw = pd.read_csv(CSV_FALLBACK, encoding="utf-8-sig", header=0,
                                     dtype=str, nrows=2)
            except Exception as e:
                st.error(str(e)); df_raw = None
        if df_raw is not None:
            st.code(str(list(df_raw.columns)))
            st.dataframe(df_raw, use_container_width=True)

    if tickers_df.empty:
        st.error("DataFrame vide — impossible d'afficher les diagnostics.")
        return

    id_cols = [c for c in ["gf_ticker", "yf_ticker", "name"] if c in tickers_df.columns]

    st.subheader("Tickers sans prix Yahoo")
    if "yf_ticker" in tickers_df.columns:
        mask = tickers_df["yf_ticker"].apply(
            lambda t: prices.get(str(t), {}).get("price") is None if pd.notna(t) else True)
        st.dataframe(tickers_df.loc[mask, id_cols], use_container_width=True, hide_index=True)
    else:
        st.info("Colonne yf_ticker absente.")

    st.subheader("Tickers sans nom")
    def _no_name(r):
        n = str(r.get("name", "") or "")
        yf = str(r.get("yf_ticker", "") or "")
        return not n.strip() and not names.get(yf, "")
    st.dataframe(tickers_df.loc[tickers_df.apply(_no_name, axis=1), id_cols],
                 use_container_width=True, hide_index=True)

    st.subheader("Tickers sans earnings")
    missing_earnings = []
    for _, row in tickers_df.iterrows():
        yf = str(row.get("yf_ticker", "") or "")
        data = be_data.get(yf, {})
        if yf and data.get("earnings") is None:
            missing_earnings.append({
                "yf_ticker": yf,
                "name": row.get("name", ""),
                "beta": data.get("beta"),
            })
    if missing_earnings:
        st.dataframe(pd.DataFrame(missing_earnings), use_container_width=True, hide_index=True)
    else:
        st.success("Aucun ticker sans earnings dans les données chargées.")

    be_debug = st.session_state.get("be_debug", [])
    if be_debug:
        st.subheader("Diagnostic Beta & Earnings")
        st.dataframe(pd.DataFrame(be_debug), use_container_width=True, hide_index=True)
    else:
        st.info("Charge Beta & Earnings pour voir les diagnostics détaillés.")

    st.subheader("Mapping complet gf_ticker → yf_ticker")
    st.dataframe(tickers_df[id_cols] if id_cols else tickers_df,
                 use_container_width=True, hide_index=True, height=400)

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Sheet en premier ───────────────────────────────────────────────────────
with st.spinner("Chargement du Google Sheet…"):
    try:
        tickers_df, data_source = load_tickers()
    except Exception as exc:
        st.error(str(exc)); st.stop()

if tickers_df.empty:
    st.error("Le DataFrame est vide après chargement. Voici les colonnes brutes du sheet :")
    try:
        df_raw = pd.read_csv(SHEET_CSV_URL, encoding="utf-8", header=0, dtype=str, nrows=3)
    except Exception:
        df_raw = pd.read_csv(CSV_FALLBACK, header=0, dtype=str, nrows=3)
    st.code(str(list(df_raw.columns)))
    st.dataframe(df_raw, use_container_width=True)
    st.stop()

pf_df    = tickers_df[tickers_df["portif"] == 1].copy()
wl_df    = tickers_df[tickers_df["portif"] != 1].copy()
valid_yf = tuple(str(t) for t in tickers_df["yf_ticker"].dropna() if str(t).strip())

components.html("""
<script>
(function() {
    const key = "watchlist_be_enabled";
    const value = window.localStorage.getItem(key);
    if (value === "1") {
        const url = new URL(window.parent.location.href);
        if (url.searchParams.get("be") !== "1") {
            url.searchParams.set("be", "1");
            window.parent.location.replace(url.toString());
        }
    }
})();
</script>
""", height=0)

if st.query_params.get("be") == "1":
    st.session_state["be_enabled"] = True

# ── CSS global en premier (avant tout élément UI) ─────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Fond & layout ── */
[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"] { background: #0f1117 !important; }
[data-testid="stHeader"] { background: rgba(15,17,23,.85) !important; backdrop-filter: blur(8px); }
.block-container { padding-top: 3rem !important; max-width: 100% !important; }
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* ── Header custom ── */
.wl-topbar {
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #161b2a 0%, #111624 100%);
  border: 1px solid #252d3d;
  border-radius: 14px;
  padding: 14px 24px;
  margin-bottom: 12px;
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
  padding: 0 28px;
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
  margin-bottom: 3px;
}
.wl-stat-val {
  font-size: 1.3rem;
  font-weight: 700;
  color: #e2e8f4;
  font-variant-numeric: tabular-nums;
}
.wl-stat-val.muted { font-size: 1rem; color: #8899bb; }
.wl-stat-val.ok    { color: #22c55e; }
.wl-stat-val.warn  { color: #fbbf24; }

/* ── Boutons ── */
.stButton > button[kind="primary"] {
  background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
  border: none !important; border-radius: 8px !important;
  color: #fff !important; font-weight: 600 !important;
  font-size: .8rem !important; padding: 0 18px !important;
  box-shadow: 0 2px 8px rgba(59,130,246,.4) !important;
  white-space: nowrap !important;
}
.stButton > button[kind="primary"]:hover { opacity: .88 !important; }
.stButton > button {
  background: #1a1f2e !important; border: 1px solid #252d3d !important;
  border-radius: 8px !important; color: #8899bb !important;
  font-size: .8rem !important; font-weight: 500 !important;
  white-space: nowrap !important;
}
.stButton > button:hover { border-color: #3b82f6 !important; color: #93c5fd !important; }

/* ── Download ── */
.stDownloadButton > button {
  background: #1a1f2e !important; border: 1px solid #252d3d !important;
  border-radius: 8px !important; color: #5a6a8a !important; font-size: .75rem !important;
}

/* ── Onglets ── */
.stTabs [data-baseweb="tab-list"] {
  background: #141824; border-radius: 10px; padding: 4px; gap: 2px;
  border: 1px solid #252d3d;
}
.stTabs [data-baseweb="tab"] {
  background: transparent !important; border-radius: 7px !important;
  color: #5a6a8a !important; font-size: .8rem !important;
  font-weight: 500 !important; padding: 6px 18px !important; border: none !important;
}
.stTabs [aria-selected="true"] { background: #252d3d !important; color: #e2e8f4 !important; }

/* ── Recherche ── */
.stTextInput > div > div > input {
  background: #141824 !important; border: 1px solid #252d3d !important;
  border-radius: 8px !important; color: #e2e8f4 !important;
  font-size: .82rem !important; padding: 8px 12px !important;
}
.stTextInput > div > div > input:focus {
  border-color: #3b82f6 !important;
  box-shadow: 0 0 0 3px rgba(59,130,246,.15) !important;
}
label[data-testid="stWidgetLabel"] p {
  font-size: .72rem !important; font-weight: 600 !important;
  color: #5a6a8a !important; text-transform: uppercase; letter-spacing: .07em;
}

/* ── Selectbox ── */
.stSelectbox > div > div {
  background: #141824 !important; border: 1px solid #252d3d !important;
  border-radius: 8px !important; color: #e2e8f4 !important; font-size: .82rem !important;
}

/* ── Misc ── */
hr { border-color: #1e2535 !important; }
.stCaption, .stCaption p { color: #3a4560 !important; font-size: .72rem !important; }
.stWarning {
  background: rgba(251,191,36,.07) !important; border: 1px solid rgba(251,191,36,.3) !important;
  border-radius: 10px !important; color: #fbbf24 !important;
}
.stInfo {
  background: rgba(59,130,246,.07) !important; border: 1px solid rgba(59,130,246,.2) !important;
  border-radius: 10px !important;
}
</style>
""", unsafe_allow_html=True)

# ── Alertes doublons ──────────────────────────────────────────────────────────
dupes = st.session_state.get("ticker_dupes", [])
if dupes:
    tickers_en_double = sorted({d["gf_ticker"] for d in dupes})
    st.warning(f"⚠️ {len(tickers_en_double)} ticker(s) en double : {', '.join(tickers_en_double)}")

# ── Header bar : stats + boutons ──────────────────────────────────────────────
last_ts = st.session_state.get("last_fetch_ts", "—")

# Placeholder pour stats (mise à jour après fetch des prix)
stats_placeholder = st.empty()

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
      <div class="wl-stat-label">Watchlist</div>
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
render_topbar(len(pf_df), len(wl_df), last_ts)

# ── Boutons compacts ──────────────────────────────────────────────────────────
from math import ceil
n = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0

b1, b2 = st.columns([1, 1])
with b1:
    if st.button("Actualiser", use_container_width=True):
        fetch_name_cached.clear(); fetch_prices.clear(); fetch_sparklines.clear()
        st.rerun()
with b2:
    if st.button("Beta & Earnings", use_container_width=True):
        fetch_be_cached.clear()
        st.session_state["be_enabled"] = True
        st.query_params["be"] = "1"
        components.html("""
        <script>
        window.localStorage.setItem("watchlist_be_enabled", "1");
        </script>
        """, height=0)
        st.rerun()

# ── 2. Noms (Yahoo, rapide) ───────────────────────────────────────────────────
with st.spinner("Noms des sociétés…"):
    names = fetch_names(valid_yf)

# ── 3. Beta & Earnings — servi silencieusement depuis le cache 24h ────────────
load_be_now = st.session_state.get("be_enabled", False)
be_data = {t: {"beta": None, "earnings": None} for t in valid_yf}
if load_be_now:
    with st.spinner("Beta & Earnings..."):
        be_data = fetch_be(valid_yf)

# ── 4. Cours (Yahoo) ──────────────────────────────────────────────────────────
with st.spinner("Cours en temps réel…"):
    prices = fetch_prices(valid_yf)

# ── 5. Sparklines (Yahoo, cache 24h) ─────────────────────────────────────────
with st.spinner("Sparklines 52 semaines…"):
    sparklines = fetch_sparklines(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)

# Mise à jour du topbar avec les prix récupérés
render_topbar(len(pf_df), len(wl_df), st.session_state["last_fetch_ts"],
              ok=ok, total=len(valid_yf))

# ── Recherche globale ──────────────────────────────────────────────────────────
# Auto-sélection du texte dans tous les champs texte
components.html("""
<script>
(function() {
    function attachSelectAll() {
        var inputs = window.parent.document.querySelectorAll('input[type="text"]');
        inputs.forEach(function(el) {
            if (!el.dataset.sa) {
                el.addEventListener('focus', function() { this.select(); });
                el.dataset.sa = '1';
            }
        });
    }
    attachSelectAll();
    new MutationObserver(attachSelectAll).observe(
        window.parent.document.body,
        {childList: true, subtree: true}
    );
})();
</script>
""", height=0)

global_search = st.text_input(
    "Recherche",
    placeholder="Ticker ou société…",
    key="global_search",
    label_visibility="collapsed",
)

# Construire les rows des deux onglets une seule fois
rows_pf = build_rows(pf_df, prices, names, be_data, sparklines, False)
rows_wl = build_rows(wl_df, prices, names, be_data, sparklines, True)

# Appliquer la recherche globale
if global_search:
    q = global_search.lower()
    rows_pf = [r for r in rows_pf if q in r["_ticker"].lower() or q in r["_name"].lower()]
    rows_wl = [r for r in rows_wl if q in r["_ticker"].lower() or q in r["_name"].lower()]

if global_search:
    # Vue combinée quand une recherche est active
    combined = rows_pf + rows_wl
    total = len(combined)
    st.markdown(f"<div style='color:#5a6a8a;font-size:.75rem;margin:6px 0 4px'>"
                f"{total} résultat(s) dans Portefeuille + Watchlist</div>",
                unsafe_allow_html=True)
    render_tab(combined, key="search")
else:
    # Vue normale par onglets
    tab1, tab2, tab3 = st.tabs([
        f"Portefeuille ({len(pf_df)})",
        f"Watchlist ({len(wl_df)})",
        "Debug",
    ])
    with tab1:
        render_tab(rows_pf, key="pf")
    with tab2:
        render_tab(rows_wl, key="wl")
    with tab3:
        render_debug(tickers_df, prices, names, be_data)

components.html(
    f"""
    <script>
    setTimeout(function() {{
        window.parent.location.reload();
    }}, {AUTO_REFRESH_SEC * 1000});
    </script>
    """,
    height=0,
)

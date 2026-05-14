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
REFRESH_TTL       = 15 * 60
SHEET_TTL         = 3_600
NAME_TTL          = 7 * 86_400
BE_TTL            = 86_400
BATCH_SIZE        = 50
YF_META_BATCH_SIZE = 10
YF_INFO_BATCH_SIZE = 50
YF_BATCH_PAUSE_SEC = 0.2

# ══════════════════════════════════════════════════════════════════════════════
# Colonnes & layout — identiques entre onglets
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "MAJ", "V", "Pays", "Ticker", "Société", "Qual", "Prix", "Var %", "Upside",
    "Score", "Mixte", "Buy", "Fair", "Trim", "Exit", "Industry", "↗",
]
COL_WIDTHS = {
    "MAJ": "84px", "V": "34px", "Pays": "42px", "Ticker": "58px", "Société": "180px", "Qual": "52px",
    "Date d'achat": "96px", "JRS": "44px",
    "Prix": "70px", "Var %": "70px", "Upside": "66px",
    "Score": "44px", "Mixte": "154px", "Buy": "66px", "Fair": "66px", "Trim": "66px", "Exit": "66px",
    "Industry": "150px",
    "↗": "36px",
}
CENTER = {"MAJ", "V", "Pays", "Date d'achat", "JRS", "Prix", "Var %", "Upside", "Score", "Mixte",
          "Buy", "Fair", "Trim", "Exit", "Qual", "↗"}

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
    "date d'achat": "purchase_date",
    "date d achat": "purchase_date",
    "verif":       "verif",
    "v":           "verif",
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
    "yf_ticker":   "yf_ticker",
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

    # Le nouveau sheet utilise yf_ticker comme ticker principal.
    if df["gf_ticker"].isna().all() and "yf_ticker" in df.columns:
        df["gf_ticker"] = df["yf_ticker"]

    # Si gf_ticker est toujours vide, essai positionnel (col C = index 2)
    if df["gf_ticker"].isna().all() and len(df.columns) > 2:
        candidate = df.iloc[:, 2].dropna().astype(str)
        # Vérifier que ça ressemble à des tickers (pas de valeurs numériques pures)
        looks_like_tickers = candidate.str.upper().str.match(r"^[A-Z0-9:\.\-]+$").sum() > len(candidate) * 0.5
        if looks_like_tickers:
            df["gf_ticker"] = df.iloc[:, 2]

    df["verif_display"] = df["verif"].apply(fmt_verif)
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
            df["last_update"], dayfirst=True, errors="coerce").dt.date
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
# Métadonnées (nom, industry) — parallèle, cache 24h
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_name(t: str) -> tuple[str, str]:
    """Récupère uniquement le nom — rapide, via history_metadata."""
    try:
        tk = yf.Ticker(t)
        tk.history(period="2d", interval="1d")
        meta = getattr(tk, "history_metadata", None) or {}
        name = (meta.get("shortName") or meta.get("longName") or "").strip()
        if not name:
            try:
                info = tk.info or {}
                name = (info.get("shortName") or info.get("longName") or "").strip()
            except Exception:
                pass
        if not name:
            try:
                info = tk.fast_info
                name = (getattr(info, "shortName", None) or "").strip()
            except Exception:
                pass
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

def _fetch_one_industry(t: str) -> tuple[str, str]:
    """Récupère industry, fallback sector, via .info."""
    try:
        tk = yf.Ticker(t)
        try:
            info = tk.info or {}
            industry = (info.get("industry") or info.get("sector") or "").strip()
            return t, industry
        except Exception:
            return t, ""
    except Exception:
        return t, ""

@st.cache_data(ttl=BE_TTL, show_spinner=False)
def fetch_industry_cached(ticker: str) -> str:
    return _fetch_one_industry(ticker)[1]

def fetch_industries(yf_tickers: tuple[str, ...]) -> dict[str, str]:
    """Industry — cache par ticker."""
    import time
    results: dict[str, str] = {}
    tickers = list(yf_tickers)
    for i in range(0, len(tickers), YF_INFO_BATCH_SIZE):
        batch = tickers[i: i + YF_INFO_BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_industry_cached, t): t for t in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    t = futures[future]
                    industry = future.result(timeout=15)
                    if industry:
                        results[t] = industry
                except Exception:
                    pass
        if i + YF_INFO_BATCH_SIZE < len(tickers):
            time.sleep(YF_BATCH_PAUSE_SEC)
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
# Calculs métier
# ══════════════════════════════════════════════════════════════════════════════

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

def safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None

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

def fmt_maj(maj_date) -> str:
    """
    MAJ rouge si la mise à jour a plus de 30 jours.
    """
    if maj_date is None or (isinstance(maj_date, float) and pd.isna(maj_date)):
        return "—"
    try:
        d = maj_date if isinstance(maj_date, date) else pd.to_datetime(maj_date).date()
        s = d.strftime("%d-%m-%Y")
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
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    n = parse_num(s)
    if n is not None:
        return f"{n:g}"
    return s

def html_score_mixte(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        score = float(v)
    except Exception:
        return ""
    value = 100.0 if score >= 85 else 10 + 90 * max(0.0, min(1.0, (score - 35) / (85 - 35)))
    color = "#1B5E20" if score >= 80 else "#43A047" if score >= 70 else "#C49000" if score >= 60 else "#E67E00" if score >= 50 else "#C62828"
    return (
        '<div class="score-spark" title="{:.0f}" role="img" aria-label="Score {:.0f}">'
        '<div class="score-spark-fill" style="width:{:.2f}%;background:{}"></div>'
        '</div>'
    ).format(score, score, value, color)

def fmt_purchase_date(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip():
        return "à vérifier"
    try:
        return pd.to_datetime(v, dayfirst=True, errors="raise").date().strftime("%d-%m-%Y")
    except Exception:
        return str(v).strip()

def fmt_holding_days(v, required: bool = False) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip():
        return "N/A" if required else "—"
    try:
        d = pd.to_datetime(v, dayfirst=True, errors="raise").date()
        days = (date.today() - d).days
        if 150 <= days <= 180:
            return f'<span style="color:#f97316">{days}</span>'
        return str(days)
    except Exception:
        return "N/A" if required else "—"

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

COUNTRY_CODES = {
    ".AS": "NL", ".BR": "BE", ".DE": "DE", ".HK": "HK",
    ".KQ": "KR", ".KS": "KR", ".L": "GB", ".MC": "ES",
    ".PA": "FR", ".SI": "SG", ".ST": "SE", ".T": "JP",
    ".TO": "CA", ".WA": "PL", ".AT": "GR", ".CO": "DK",
    ".MI": "IT", ".SW": "CH",
}

def country_code(ticker: str) -> str:
    t = str(ticker or "").upper().strip()
    for suffix, code in sorted(COUNTRY_CODES.items(), key=lambda item: len(item[0]), reverse=True):
        if t.endswith(suffix):
            return code
    return "US" if t else ""

def html_country_flag(ticker: str) -> str:
    code = country_code(ticker)
    if not code:
        return ""
    return f'<span class="fi fi-{code.lower()} wl-country-flag" title="{code}"></span>'

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict,
               names: dict, industries: dict,
               highlight_radar: bool = False,
               holding_required: bool = False) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t   = r.get("yf_ticker")
        yf_s   = str(yf_t) if pd.notna(yf_t) else ""
        q      = prices.get(yf_s, {})

        price  = q.get("price") or (r.get("spot_sheet") if pd.notna(r.get("spot_sheet")) else None)
        chg    = q.get("chg")
        name   = (r.get("name") or "") if pd.notna(r.get("name")) else ""
        name   = name or names.get(yf_s, "")
        name_u = name.upper() if name else ""

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        ratio   = compute_ratio(price, buy, exit_)
        score   = safe_float(compute_score(ratio, r.get("note")))
        score_sheet = safe_float(r.get("score_sheet"))
        if score is None:
            score = score_sheet
        score_mixte = score_sheet if score_sheet is not None else score
        upside  = compute_upside(price, fair, trim)
        quality = safe_float(r.get("note"))
        industry = industries.get(yf_s, "") or "-"

        gf = str(r["gf_ticker"])
        name_html = name_u if name_u else gf

        # Mise en surbrillance "sous le radar"
        radar   = (highlight_radar and score is not None and score >= 85)
        flagged = bool(r.get("flagged", False))

        rows.append({
            "_score":        score if score is not None else -1.0,
            "_chg":          chg,
            "_maj":          r.get("last_update"),
            "_upside":       upside if upside is not None else -999.0,
            "_quality":      quality if quality is not None else -1.0,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_radar":        radar,
            "_flagged":      flagged,
            # Données brutes pour export XLS
            "_raw": {
                "MAJ": r.get("last_update").strftime("%d-%m-%Y") if pd.notna(r.get("last_update")) and r.get("last_update") else "",
                "V":        r.get("verif_display", ""),
                "Pays":     country_code(yf_s),
                "Ticker":   gf, "Société": name_u,
                "Date d'achat": fmt_purchase_date(r.get("purchase_date")),
                "JRS":      fmt_holding_days(r.get("purchase_date"), holding_required),
                "Prix":     price, "Var %": chg, "Upside %": upside,
                "Score":    round(score) if score is not None else "",
                "Mixte":    score_mixte,
                "Buy":      buy, "Fair":  fair, "Trim":  trim, "Exit":  exit_,
                "Qual":     int(quality) if quality is not None else "",
                "Industry": industry,
            },
            # HTML
            "MAJ":      fmt_maj(r.get("last_update")),
            "V":        r.get("verif_display", ""),
            "Pays":     html_country_flag(yf_s),
            "Ticker":   html_ticker_link(yf_s, gf),
            "Société":  f'<span title="{name_u}">{name_html}</span>',
            "Date d'achat": fmt_purchase_date(r.get("purchase_date")),
            "JRS":      fmt_holding_days(r.get("purchase_date"), holding_required),
            "Qual":     fmt_note(r.get("note")),
            "Prix":     fmt_price(price),
            "Var %":    html_var(chg),
            "Upside":   html_upside(upside),
            "Score":    fmt_score(score),
            "Mixte":    html_score_mixte(score_mixte),
            "Buy":      fmt_price(buy),
            "Fair":     fmt_price(fair),
            "Trim":     fmt_price(trim),
            "Exit":     fmt_price(exit_),
            "Industry": industry,
            "↗":        html_link(r.get("url")),
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# Export XLS
# ══════════════════════════════════════════════════════════════════════════════

def export_xlsx(rows: list[dict]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    cols = ["MAJ", "V", "Pays", "Ticker", "Société", "JRS", "Qual", "Prix", "Var %",
            "Upside %", "Score", "Mixte", "Buy", "Fair", "Trim",
            "Exit", "Industry"]
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

CSS = """<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/lipis/flag-icons@7.3.2/css/flag-icons.min.css">
<style>
.wl-wrap {
  overflow-x: auto;
  max-height: none;
  overflow-y: visible;
  border-radius: 12px;
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
.wl-country-flag {
  display: inline-block;
  width: 19px;
  line-height: 13px;
  border-radius: 2px;
  vertical-align: middle;
}
.score-spark {
  height: 14px;
  width: 100%;
  margin: 0 auto;
  background: #b6c0cb;
  display: block;
  border-radius: 3px;
  overflow: hidden;
}
.score-spark-fill {
  height: 100%;
  border-radius: 3px 0 0 3px;
}
</style>"""

def render_table(rows: list[dict], display_cols: list[str] | None = None) -> None:
    if not rows: st.info("Aucun titre."); return
    cols = display_cols or DISPLAY_COLS
    colgroup = "<colgroup>" + "".join(
        f'<col style="width:{COL_WIDTHS.get(c,"auto")}">' for c in cols
    ) + "</colgroup>"
    th_parts = []
    skip_next = False
    for idx, c in enumerate(cols):
        if skip_next:
            skip_next = False
            continue
        if c == "Score" and idx + 1 < len(cols) and cols[idx + 1] == "Mixte":
            th_parts.append('<th class="c" colspan="2" title="Score">Score</th>')
            skip_next = True
        else:
            th_parts.append(f'<th class="{"c" if c in CENTER else ""}" title="{c}">{c}</th>')
    th = "".join(th_parts)
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
            for c in cols
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

def render_tab(rows: list[dict], key: str, display_cols: list[str] | None = None) -> None:
    sort_choice = st.selectbox("Tri", [
        "Score ↓", "Score ↑", "Ticker A→Z", "Qual ↓",
        "Upside ↓", "Var % ↑", "Var % ↓", "MAJ ↓",
    ], key=f"{key}_t", label_visibility="collapsed")

    sort_map = {
        "Ticker A→Z":     lambda r: r["_ticker"],
        "Score ↓":        lambda r: -r["_score"],
        "Score ↑":        lambda r: r["_score"],
        "Qual ↓":         lambda r: -r["_quality"],
        "Upside ↓":       lambda r: -r["_upside"],
        "Var % ↑":        lambda r: (r["_chg"] is None, -(r["_chg"] or 0)),
        "Var % ↓":        lambda r: (r["_chg"] is None, r["_chg"] or 0),
        "MAJ ↓":          lambda r: r["_maj"] or date.max,
    }
    key_fn = sort_map.get(sort_choice)
    if key_fn:
        rows.sort(key=key_fn, reverse=(sort_choice == "MAJ ↓"))

    render_table(rows, display_cols)

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

def render_debug(tickers_df: pd.DataFrame, prices: dict, names: dict, industries: dict) -> None:
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

    st.subheader("Diagnostic logique MAJ")
    debug_rows = []
    today = date.today()
    for _, row in tickers_df.iterrows():
        yf = str(row.get("yf_ticker", "") or "")
        maj_raw = row.get("last_update")

        maj_date = None
        try:
            if pd.notna(maj_raw) and maj_raw:
                maj_date = maj_raw if isinstance(maj_raw, date) else pd.to_datetime(maj_raw).date()
        except Exception:
            pass

        older_than_30 = (today - maj_date).days > 30 if maj_date is not None else False

        debug_rows.append({
            "gf_ticker": row.get("gf_ticker", ""),
            "yf_ticker": yf,
            "name": row.get("name", ""),
            "MAJ_raw": maj_raw,
            "MAJ_date": maj_date,
            "older_than_30": older_than_30,
            "industry": industries.get(yf, ""),
            "price": prices.get(yf, {}).get("price"),
        })

    st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True, height=500)

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

components.html("""
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
""", height=0)

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

def tickers_for(df: pd.DataFrame) -> tuple[str, ...]:
    return tuple(str(t) for t in df["yf_ticker"].dropna() if str(t).strip())

pf_yf = tickers_for(pf_df)
wl_yf = tickers_for(wl_df)
all_yf = tuple(dict.fromkeys((*pf_yf, *wl_yf)))

def mark_refresh(scope: str) -> None:
    st.session_state["last_action"] = "refresh"
    st.session_state["refresh_scope"] = scope
    fetch_prices.clear()

last_action = st.session_state.pop("last_action", "")
refresh_scope = st.session_state.pop("refresh_scope", "")
active_yf = pf_yf if refresh_scope == "pf" else wl_yf if refresh_scope == "wl" else all_yf

# ── 2. Noms (Yahoo, rapide) ───────────────────────────────────────────────────
data_key = all_yf
same_data_key = st.session_state.get("data_key") == data_key
cached_names = st.session_state.get("names_data", {})
name_scope = active_yf if last_action == "refresh" else all_yf
missing_name_tickers = tuple(t for t in name_scope if not cached_names.get(t))

if not all_yf:
    names = cached_names
elif same_data_key and last_action != "refresh" and "names_data" in st.session_state:
    names = st.session_state["names_data"]
elif last_action == "refresh":
    names = dict(cached_names)
    if missing_name_tickers:
        with st.spinner("Noms des nouveaux tickers…"):
            names.update(fetch_names(missing_name_tickers))
    st.session_state["names_data"] = names
else:
    with st.spinner("Noms des sociétés…"):
        names = fetch_names(all_yf)
    st.session_state["names_data"] = names

# ── 3. Industry (Yahoo) ───────────────────────────────────────────────────────
cached_industries = st.session_state.get("industry_data", {})
missing_industry_tickers = tuple(t for t in all_yf if not cached_industries.get(t))
if not all_yf:
    industries = cached_industries
elif missing_industry_tickers:
    industries = dict(cached_industries)
    with st.spinner("Industries Yahoo…"):
        industries.update(fetch_industries(missing_industry_tickers))
    st.session_state["industry_data"] = industries
elif same_data_key and "industry_data" in st.session_state:
    industries = st.session_state["industry_data"]
else:
    industries = cached_industries

# ── 4. Cours (Yahoo) ──────────────────────────────────────────────────────────
if not all_yf:
    prices = st.session_state.get("prices_data", {})
elif same_data_key and last_action != "refresh" and "prices_data" in st.session_state:
    prices = st.session_state["prices_data"]
else:
    price_scope = active_yf if last_action == "refresh" else all_yf
    prices_spinner = "Actualisation des cours en temps réel…" if last_action == "refresh" else "Cours en temps réel…"
    with st.spinner(prices_spinner):
        fresh_prices = fetch_prices(price_scope)
    prices = dict(st.session_state.get("prices_data", {}))
    prices.update(fresh_prices)
    st.session_state["prices_data"] = prices

st.session_state["data_key"] = data_key

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in all_yf if prices.get(t, {}).get("price") is not None)

# Mise à jour du topbar avec les prix récupérés
render_topbar(len(pf_df), len(wl_df), st.session_state["last_fetch_ts"],
              ok=ok, total=len(all_yf))

# Construire les rows des deux vues une seule fois
rows_pf = build_rows(pf_df, prices, names, industries, False, True)
rows_wl = build_rows(wl_df, prices, names, industries, False, False)

tab1, tab2, tab3 = st.tabs([
    f"Portefeuille ({len(pf_df)})",
    f"Watchlist ({len(wl_df)})",
    "Debug",
])
pf_cols = DISPLAY_COLS[:5] + ["JRS"] + DISPLAY_COLS[5:]
wl_cols = DISPLAY_COLS[:5] + ["JRS"] + DISPLAY_COLS[5:]
with tab1:
    st.button("Actualiser", key="refresh_pf", use_container_width=True, on_click=mark_refresh, args=("pf",))
    render_tab(rows_pf, key="pf", display_cols=pf_cols)
with tab2:
    st.button("Actualiser", key="refresh_wl", use_container_width=True, on_click=mark_refresh, args=("wl",))
    render_tab(rows_wl, key="wl", display_cols=wl_cols)
with tab3:
    render_debug(tickers_df, prices, names, industries)

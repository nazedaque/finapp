from __future__ import annotations

import io
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import openpyxl
import pandas as pd
import streamlit as st
import yfinance as yf

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Watchlist", page_icon="📈", layout="wide",
                   initial_sidebar_state="collapsed")

SHEET_ID      = "1KQ0eolfB-UH-N-jQo2WDxsmVNT3I4IhiTEbdIfcPvbA"
SHEET_NAME    = "Travail"
SHEET_CSV_URL = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
                 f"/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}")
CSV_FALLBACK      = "tickers.csv"
REFRESH_TTL       = 30 * 60
SHEET_TTL         = 3_600
META_TTL          = 86_400
SPARK_TTL         = 86_400
BATCH_SIZE        = 75

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
    "Statut", "Earnings", "🔗",
]
COL_WIDTHS = {
    "MAJ": "92px", "Ticker": "82px", "Société": "210px",
    "Prix": "78px", "Var %": "80px", "Upside": "72px", "Spark": "88px",
    "Score": "52px", "Buy": "74px", "Fair": "74px", "Trim": "74px", "Exit": "74px",
    "Qualité": "58px", "Beta": "56px", "Statut": "90px",
    "Earnings": "98px", "🔗": "44px",
}
CENTER = {"MAJ", "Prix", "Var %", "Upside", "Spark", "Score",
          "Buy", "Fair", "Trim", "Exit", "Qualité", "Beta",
          "Statut", "Earnings", "🔗"}

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


@st.cache_data(ttl=SHEET_TTL, show_spinner=False)
def load_tickers(cache_bust: int = 0) -> tuple[pd.DataFrame, str]:
    """cache_bust est un timestamp passé à l'appel pour forcer le rechargement."""
    import time as _t
    # Paramètre aléatoire pour contourner le cache CDN de Google
    bust = cache_bust or int(_t.time())
    url  = SHEET_CSV_URL + f"&_cb={bust}"
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

@st.cache_data(ttl=META_TTL, show_spinner=False)
def fetch_names(yf_tickers: tuple[str, ...]) -> dict[str, str]:
    import time
    names: dict[str, str] = {}
    tickers = list(yf_tickers)
    for i in range(0, len(tickers), 10):
        batch = tickers[i: i + 10]
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_one_name, t): t for t in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    t, name = future.result(timeout=15)
                    names[t] = name
                except Exception:
                    names[futures[future]] = ""
        if i + 10 < len(tickers):
            time.sleep(0.2)
    return names


def _fetch_one_be(t: str) -> tuple[str, dict]:
    """Récupère beta + earnings — plus lent, via .info et .calendar."""
    result: dict = {"beta": None, "earnings": None}
    try:
        tk = yf.Ticker(t)
        try:
            info = tk.info or {}
            b = info.get("beta")
            if b is not None:
                result["beta"] = float(b)
        except Exception:
            pass
        try:
            cal = tk.calendar
            if cal is not None:
                if isinstance(cal, dict):
                    raw = cal.get("Earnings Date")
                    if raw is not None:
                        dates = raw if isinstance(raw, list) else [raw]
                        parsed = []
                        for d in dates:
                            try:
                                if hasattr(d, "date"): d = d.date()
                                elif isinstance(d, str):
                                    d = datetime.strptime(d[:10], "%Y-%m-%d").date()
                                parsed.append(d)
                            except Exception:
                                pass
                        if parsed:
                            result["earnings"] = min(parsed)
                elif hasattr(cal, "loc"):
                    try:
                        d = cal.loc["Earnings Date"].iloc[0]
                        if hasattr(d, "date"): d = d.date()
                        result["earnings"] = d
                    except Exception:
                        pass
        except Exception:
            pass
    except Exception:
        pass
    return t, result

@st.cache_data(ttl=META_TTL, show_spinner=False)
def fetch_be(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    """Beta + Earnings — déclenchement manuel via bouton."""
    import time
    results: dict[str, dict] = {}
    empty = {"beta": None, "earnings": None}
    tickers = list(yf_tickers)
    for i in range(0, len(tickers), 10):
        batch = tickers[i: i + 10]
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_one_be, t): t for t in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    t, data = future.result(timeout=15)
                    results[t] = data
                except Exception:
                    results[futures[future]] = dict(empty)
        if i + 10 < len(tickers):
            time.sleep(0.2)
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
    results: dict[str, dict] = {}
    for batch in _chunked(list(yf_tickers), BATCH_SIZE):
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
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Sparklines 52 semaines — cache 24h
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=SPARK_TTL, show_spinner=False)
def fetch_sparklines(yf_tickers: tuple[str, ...]) -> dict[str, list[float]]:
    result: dict[str, list[float]] = {}
    for batch in _chunked(list(yf_tickers), BATCH_SIZE):
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
        return '<span style="color:#64748b">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "▲" if chg >= 0 else "▼"
    return f'<span style="color:{c}">{a}&nbsp;{abs(chg):.2f}%</span>'

def html_upside(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '<span style="color:#64748b">—</span>'
    c = "#22c55e" if v >= 0 else "#ef4444"
    a = "▲" if v >= 0 else "▼"
    return f'<span style="color:{c};font-weight:600">{a}&nbsp;{abs(v):.1f}%</span>'

def html_statut(statut) -> str:
    c = STATUT_COLOR.get(statut, "#64748b")
    return f'<span style="color:{c};font-weight:600">{statut or "—"}</span>'

def html_sparkline(closes: list[float]) -> str:
    if not closes or len(closes) < 4: return "—"
    mn, mx = min(closes), max(closes)
    if mx == mn: return "—"
    w, h = 84, 26
    pts = " ".join(
        f"{i / (len(closes) - 1) * w:.1f},{h - (v - mn) / (mx - mn) * h:.1f}"
        for i, v in enumerate(closes)
    )
    color = "#22c55e" if closes[-1] >= closes[0] else "#ef4444"
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'style="display:inline-block;vertical-align:middle">'
            f'<polyline points="{pts}" fill="none" stroke="{color}" '
            f'stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>'
            f'</svg>')

def html_ticker_link(yf_ticker: str, gf_ticker: str) -> str:
    url = f"https://finance.yahoo.com/quote/{yf_ticker}/" if yf_ticker else "#"
    return (f'<a href="{url}" target="_blank" rel="noopener" '
            f'title="Ouvrir sur Yahoo Finance" '
            f'style="color:#93c5fd;font-family:monospace;font-size:.8rem;'
            f'text-decoration:none">{gf_ticker}</a>')

def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith("http"): return ""
    return (f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
            f'style="color:#7dd3fc;font-size:.85rem;text-decoration:none">🔗</a>')

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

        # Mise en surbrillance "sous le radar" (watchlist, statut achat, score élevé)
        radar = (highlight_radar
                 and score is not None and float(score) >= 85)

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
            "🔗":       html_link(r.get("url")),
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
.wl-wrap{overflow-x:auto;max-height:70vh;overflow-y:auto;
  border-radius:8px;border:1px solid #1e293b}
.wl-table{width:100%;border-collapse:collapse;font-size:.82rem;
  color:#e2e8f0;table-layout:fixed}
.wl-table thead tr{position:sticky;top:0;z-index:2}
.wl-table th{background:#0f172a;color:#94a3b8;font-weight:600;
  padding:9px 8px;text-align:left;border-bottom:2px solid #334155;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wl-table th.c{text-align:center}
.wl-table td{padding:5px 8px;border-bottom:1px solid #1a2035;
  vertical-align:middle;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.wl-table td.c{text-align:center}
.wl-table tbody tr:hover td{background:#ffffff0a}
.wl-radar td{background:#0f2920 !important}
.wl-radar:hover td{background:#1a3d2b !important}
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
        cls = ' class="wl-radar"' if r["_radar"] else ""
        tds = "".join(
            f'<td class="{"c" if c in CENTER else ("" if c != "Société" else "")}">'
            f'{r[c]}</td>' for c in DISPLAY_COLS
        )
        trs.append(f"<tr{cls}>{tds}</tr>")
    st.markdown(
        CSS + f'<div class="wl-wrap"><table class="wl-table">'
        f'{colgroup}<thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

def render_tab(df_sub: pd.DataFrame, prices: dict, names: dict,
               be_data: dict, sparklines: dict, key: str,
               highlight_radar: bool = False) -> None:
    rows = build_rows(df_sub, prices, names, be_data, sparklines, highlight_radar)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        search = st.text_input("Recherche", key=f"{key}_s", placeholder="Ticker ou société…")
    with c2:
        sort_choice = st.selectbox("Tri", [
            "Statut + Score", "Ticker A→Z", "Score ↓", "Qualité ↓",
            "Upside ↓", "Var % ↑", "Var % ↓", "MAJ ↓", "Beta ↓",
        ], key=f"{key}_t")
    with c3:
        sf = st.selectbox("Statut",
            ["Tous", "Strong buy", "Buy", "Fair", "Trim", "Exit"], key=f"{key}_f")

    if search:
        q = search.lower()
        rows = [r for r in rows if q in r["_ticker"].lower() or q in r["_name"].lower()]
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

    # Export XLS
    if rows:
        xls_bytes = export_xlsx(rows)
        st.download_button(
            "⬇ Export Excel", data=xls_bytes,
            file_name=f"watchlist_{key}_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key}_xls",
        )

    render_table(rows)

    missing = [r["_ticker"] for r in rows if not r["_price_ok"]]
    if missing:
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# Onglet Debug
# ══════════════════════════════════════════════════════════════════════════════

def render_debug(tickers_df: pd.DataFrame, prices: dict, names: dict) -> None:
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
    st.info("Les earnings sont dans le cache Beta & Earnings (bouton dédié).")

    st.subheader("Mapping complet gf_ticker → yf_ticker")
    st.dataframe(tickers_df[id_cols] if id_cols else tickers_df,
                 use_container_width=True, hide_index=True, height=400)

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Sheet en premier ───────────────────────────────────────────────────────
with st.spinner("Chargement du Google Sheet…"):
    try:
        bust = st.session_state.get("sheet_bust", 0)
        tickers_df, data_source = load_tickers(cache_bust=bust)
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

# ── Métriques & boutons ───────────────────────────────────────────────────────
m1, m2, m3 = st.columns(3)
m1.metric("Portefeuille", len(pf_df))
m2.metric("Watchlist",    len(wl_df))
m3.metric("Dernière MAJ", st.session_state.get("last_fetch_ts", "—"))

rc1, rc2, rc3 = st.columns([1, 1, 4])
with rc1:
    if st.button("Actualiser", type="primary", use_container_width=True):
        import time as _t
        load_tickers.clear()
        fetch_names.clear()
        fetch_prices.clear()
        fetch_sparklines.clear()
        st.session_state["sheet_bust"] = int(_t.time())
        st.rerun()
with rc2:
    if st.button("Beta & Earnings", use_container_width=True):
        fetch_be.clear()
        st.rerun()
with rc3:
    from math import ceil
    n = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0
    st.caption(f"Source : **{data_source}** · {len(valid_yf)} tickers · "
               f"{n} paquets Yahoo · cache {REFRESH_TTL//60} min")

# ── 2. Noms (Yahoo, rapide) ───────────────────────────────────────────────────
with st.spinner("Noms des sociétés…"):
    names = fetch_names(valid_yf)

# ── 3. Beta & Earnings (Yahoo, lent — cache 24h) ──────────────────────────────
with st.spinner("Beta & Earnings…"):
    be_data = fetch_be(valid_yf)

# ── 4. Cours (Yahoo) ──────────────────────────────────────────────────────────
with st.spinner("Cours en temps réel…"):
    prices = fetch_prices(valid_yf)

# ── 5. Sparklines (Yahoo, cache 24h) ─────────────────────────────────────────
with st.spinner("Sparklines 52 semaines…"):
    sparklines = fetch_sparklines(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)
s1, s2, _ = st.columns(3)
s1.metric("Prix récupérés", ok)
s2.metric("Manquants",      len(valid_yf) - ok)

st.divider()

tab1, tab2, tab3 = st.tabs([
    f"Portefeuille ({len(pf_df)})",
    f"Watchlist ({len(wl_df)})",
    "Debug",
])
with tab1:
    render_tab(pf_df, prices, names, be_data, sparklines, key="pf")
with tab2:
    render_tab(wl_df, prices, names, be_data, sparklines, key="wl", highlight_radar=True)
with tab3:
    render_debug(tickers_df, prices, names)

# ── Auto-refresh toutes les 30 minutes ───────────────────────────────────────
import time as _time

AUTO_REFRESH_SEC = 30 * 60

if "next_refresh" not in st.session_state:
    st.session_state["next_refresh"] = _time.time() + AUTO_REFRESH_SEC

remaining = max(0, int(st.session_state["next_refresh"] - _time.time()))
m, s = divmod(remaining, 60)
st.caption(f"Prochain rafraîchissement automatique dans {m}m {s:02d}s")

if remaining == 0:
    fetch_prices.clear()
    st.session_state["next_refresh"] = _time.time() + AUTO_REFRESH_SEC
    st.rerun()
else:
    # Vérifie toutes les 60 secondes sans bloquer l'UI
    _time.sleep(60)
    st.rerun()

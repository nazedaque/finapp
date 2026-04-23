from __future__ import annotations

import io
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from math import ceil

import pandas as pd
import streamlit as st
import yfinance as yf

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Watchlist", page_icon="📈",
                   layout="wide", initial_sidebar_state="collapsed")

SHEET_ID      = "1KQ0eolfB-UH-N-jQo2WDxsmVNT3I4IhiTEbdIfcPvbA"
SHEET_NAME    = "Travail"
SHEET_CSV_URL = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
                 f"/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}")
CSV_FALLBACK   = "tickers.csv"

REFRESH_TTL    = 30 * 60   # cours Yahoo
SHEET_TTL      = 3_600     # liste tickers
META_TTL       = 86_400    # nom/beta/earnings
SPARK_TTL      = 3_600     # sparklines (1h)
BATCH_SIZE     = 75

# ══════════════════════════════════════════════════════════════════════════════
# Exchanges & overrides
# ══════════════════════════════════════════════════════════════════════════════

EXCHANGE_MAP: dict[str, str] = {
    "EPA": ".PA", "ETR": ".DE", "FRA": ".F",  "LON": ".L",
    "AMS": ".AS", "BIT": ".MI", "BME": ".MC", "STO": ".ST",
    "SWX": ".SW", "TYO": ".T",  "TSE": ".TO", "HKG": ".HK",
    "SGX": ".SI", "HEL": ".HE", "VIE": ".VI", "CPH": ".CO",
    "EBR": ".BR", "WSE": ".WA", "CVE": ".V",  "NYSE": "", "NASDAQ": "",
}
MANUAL_OVERRIDES: dict[str, str] = {
    "JST": "JST.DE", "BETS-B": "BETS-B.ST", "MOUR": "MOUR.BR",
    "EPA:HAVAS": "HAVAS.AS", "TSE:DHT.U": "DHT-UN.TO",
    "TSE:CTC.A": "CTC-A.TO", "CPH:VAR": "VAR.OL",
}

# ══════════════════════════════════════════════════════════════════════════════
# Statuts
# ══════════════════════════════════════════════════════════════════════════════

STATUT_ORDER = {"Strong buy": 0, "Buy": 1, "Fair": 2, "Trim": 3, "Exit": 4, "": 9}
STATUT_COLOR = {
    "Strong buy": "#1f8b4c", "Buy": "#6dbf4b", "Fair": "#d4b000",
    "Trim": "#e67e22", "Exit": "#c0392b", "": "#64748b",
}

# ══════════════════════════════════════════════════════════════════════════════
# Colonnes et largeurs (identiques entre onglets)
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "MAJ", "Ticker", "Société", "Spark",
    "Prix", "Var %", "Upside",
    "Score", "Buy", "Fair", "Trim", "Exit",
    "Qualité", "Beta", "Statut", "Earnings", "🔗",
]
COL_WIDTHS = {
    "MAJ":      "92px",  "Ticker":  "105px", "Société": "210px",
    "Spark":    "90px",  "Prix":    "80px",  "Var %":   "80px",
    "Upside":   "75px",  "Score":   "54px",  "Buy":     "75px",
    "Fair":     "75px",  "Trim":    "75px",  "Exit":    "75px",
    "Qualité":  "60px",  "Beta":    "58px",  "Statut":  "90px",
    "Earnings": "100px", "🔗":      "30px",
}
CENTER = {
    "MAJ", "Spark", "Prix", "Var %", "Upside", "Score",
    "Buy", "Fair", "Trim", "Exit", "Qualité", "Beta", "Statut", "Earnings", "🔗",
}

# Score minimum pour le highlight "sous le radar"
RADAR_SCORE_MIN = 75

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def normalize_col(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", str(s))
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").strip().lower()

def gf_to_yf(gf: str, yf_override: str = "") -> str | None:
    """Conversion GF → YF. Priorité : colonne YF_Ticker du sheet, puis overrides, puis mapping."""
    if yf_override and str(yf_override).strip() not in ("", "nan"):
        return str(yf_override).strip()
    gf = str(gf).strip()
    if not gf: return None
    if gf in MANUAL_OVERRIDES: return MANUAL_OVERRIDES[gf]
    if ":" not in gf: return gf
    exchange, symbol = gf.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange)
    return f"{symbol}{suffix}" if suffix is not None else None

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

def stockopedia_url(gf_ticker: str) -> str:
    symbol = gf_ticker.split(":")[-1] if ":" in gf_ticker else gf_ticker
    return f"https://www.stockopedia.com/search/?q={symbol}"

# ══════════════════════════════════════════════════════════════════════════════
# Chargement du sheet
# ══════════════════════════════════════════════════════════════════════════════

SHEET_COL_NORMALIZED = {
    "ticker": "gf_ticker", "societe": "name", "portif": "portif",
    "note": "note", "buy": "buy", "fair": "fair", "trim": "trim",
    "exit": "exit", "url": "url", "spot": "spot_sheet",
    "score mixte": "score_sheet", "last update": "last_update",
    "yf_ticker": "yf_override",   # ← colonne optionnelle override
}
NUMERIC_COLS = ["note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet"]


@st.cache_data(ttl=SHEET_TTL, show_spinner=False)
def load_tickers() -> tuple[pd.DataFrame, str]:
    source = "Google Sheet"
    try:
        df = pd.read_csv(SHEET_CSV_URL, encoding="utf-8", header=0, dtype=str)
    except Exception:
        try:
            df = pd.read_csv(CSV_FALLBACK, header=0, dtype=str); source = "tickers.csv (fallback)"
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    rename_map = {c: SHEET_COL_NORMALIZED[normalize_col(c)]
                  for c in df.columns if normalize_col(c) in SHEET_COL_NORMALIZED}
    df = df.rename(columns=rename_map)
    for col in SHEET_COL_NORMALIZED.values():
        if col not in df.columns: df[col] = pd.NA

    df = df[df["gf_ticker"].notna()].copy()
    df = df[~df["gf_ticker"].astype(str).str.strip().isin(["", "Ticker", "gf_ticker"])].copy()
    df["portif"] = df["portif"].map(
        lambda v: 1 if str(v).strip() in ("1", "TRUE", "True", "true") else 0)
    df["name"] = df["name"].apply(
        lambda v: "" if (pd.isna(v) or str(v).strip().startswith("#")) else str(v).strip())
    for col in NUMERIC_COLS:
        if col in df.columns: df[col] = df[col].apply(parse_num)

    if "last_update" in df.columns:
        df["last_update"] = pd.to_datetime(
            df["last_update"], format="%d/%m/%Y", errors="coerce").dt.date
    else:
        df["last_update"] = None

    df["yf_ticker"] = df.apply(
        lambda r: gf_to_yf(r["gf_ticker"], r.get("yf_override", "")), axis=1)
    return df.reset_index(drop=True), source

# ══════════════════════════════════════════════════════════════════════════════
# Métadonnées : nom, beta, earnings
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_meta(t: str) -> tuple[str, dict]:
    result: dict = {"name": "", "beta": None, "earnings": None}
    try:
        tk = yf.Ticker(t)
        try:
            tk.history(period="2d", interval="1d")
            meta = getattr(tk, "history_metadata", None) or {}
            result["name"] = (meta.get("shortName") or meta.get("longName") or "").strip()
        except Exception:
            pass
        try:
            info = tk.info or {}
            if not result["name"]:
                result["name"] = (info.get("shortName") or info.get("longName") or "").strip()
            b = info.get("beta")
            if b is not None: result["beta"] = float(b)
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
                                elif isinstance(d, str): d = datetime.strptime(d[:10], "%Y-%m-%d").date()
                                parsed.append(d)
                            except Exception:
                                pass
                        if parsed: result["earnings"] = min(parsed)
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
def fetch_meta(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    empty: dict = {"name": "", "beta": None, "earnings": None}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_one_meta, t): t for t in yf_tickers}
        for future in as_completed(futures, timeout=300):
            try:
                t, data = future.result(timeout=20); results[t] = data
            except Exception:
                results[futures[future]] = dict(empty)
    return results

# ══════════════════════════════════════════════════════════════════════════════
# Sparklines (52 semaines, 1j)
# ══════════════════════════════════════════════════════════════════════════════

def _chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i: i + size]


@st.cache_data(ttl=SPARK_TTL, show_spinner=False)
def fetch_sparklines(yf_tickers: tuple[str, ...]) -> dict[str, list[float]]:
    result: dict[str, list[float]] = {}
    for batch in _chunked(list(yf_tickers), BATCH_SIZE):
        try:
            data = yf.download(
                tickers=" ".join(batch), period="1y", interval="1d",
                auto_adjust=True, progress=False, group_by="ticker", threads=True,
            )
            multi = len(batch) > 1
            for t in batch:
                try:
                    s = (data[t]["Close"] if multi else data["Close"]).dropna()
                    result[t] = [float(x) for x in s.values]
                except Exception:
                    result[t] = []
        except Exception:
            for t in batch: result[t] = []
    return result


def make_sparkline(prices: list[float], w: int = 80, h: int = 24) -> str:
    if len(prices) < 2: return ""
    mn, mx = min(prices), max(prices)
    if mx == mn: return ""
    xs = [i * w / (len(prices) - 1) for i in range(len(prices))]
    ys = [h - (p - mn) / (mx - mn) * h for p in prices]
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    c = "#22c55e" if prices[-1] >= prices[0] else "#ef4444"
    return (f'<svg width="{w}" height="{h}" style="display:inline-block;vertical-align:middle">'
            f'<polyline points="{pts}" fill="none" stroke="{c}" '
            f'stroke-width="1.5" stroke-linejoin="round"/></svg>')

# ══════════════════════════════════════════════════════════════════════════════
# Cours Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def _closes(data, ticker, multi):
    try:
        s = data[ticker]["Close"] if multi else data["Close"]
        s = s.dropna().astype(float)
        return s if isinstance(s, pd.Series) else pd.Series(dtype=float)
    except Exception:
        return pd.Series(dtype=float)

def _price_chg(closes):
    if closes.empty: return None, None
    price = float(closes.iloc[-1])
    dates = pd.to_datetime(closes.index).tz_localize(None).normalize()
    prev  = closes[dates < dates[-1]]
    chg   = None
    if not prev.empty:
        p0 = float(prev.iloc[-1])
        if p0: chg = (price - p0) / p0 * 100
    return price, chg


@st.cache_data(ttl=REFRESH_TTL, show_spinner=False)
def fetch_prices(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for batch in _chunked(list(yf_tickers), BATCH_SIZE):
        try:
            data = yf.download(
                tickers=" ".join(batch), period="5d", interval="30m",
                auto_adjust=False, progress=False, group_by="ticker",
                threads=True, prepost=False,
            )
        except Exception:
            for t in batch: results[t] = {"price": None, "chg": None}
            continue
        multi = len(batch) > 1
        for t in batch:
            price, chg = _price_chg(_closes(data, t, multi))
            results[t] = {"price": price, "chg": chg}
    return results

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
    """Upside vers la moyenne de Fair Value et Trim."""
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
    if maj_date is None or (isinstance(maj_date, float) and pd.isna(maj_date)): return "—"
    try:
        d = maj_date if isinstance(maj_date, date) else pd.to_datetime(maj_date).date()
        s = d.strftime("%d-%m-%Y")
        red = (d > earnings_date) if earnings_date is not None else (date.today() - d).days > 30
        return f'<span style="color:#ef4444">{s}</span>' if red else s
    except Exception: return "—"

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
    return f'<span style="color:{c}">{a}&nbsp;{abs(v):.1f}%</span>'

def html_statut(statut) -> str:
    c = STATUT_COLOR.get(statut, "#64748b")
    return f'<span style="color:{c};font-weight:600">{statut or "—"}</span>'

def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith("http"): return ""
    return (f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
            'style="color:#7dd3fc;font-size:1.1rem;text-decoration:none">🔗</a>')

def html_ticker(gf: str) -> str:
    url = stockopedia_url(gf)
    return (f'<a href="{url}" target="_blank" rel="noopener" title="Stockopedia" '
            f'style="color:#93c5fd;font-family:monospace;font-size:.8rem;text-decoration:none">'
            f'{gf}</a>')

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict,
               meta: dict, sparks: dict) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t = r.get("yf_ticker")
        yf_s = str(yf_t) if pd.notna(yf_t) else ""
        q    = prices.get(yf_s, {})
        m    = meta.get(yf_s, {})

        price = q.get("price") or r.get("spot_sheet")
        chg   = q.get("chg")
        name  = r.get("name", "") or m.get("name", "")

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        statut  = compute_statut(price, buy, fair, trim, exit_)
        ratio   = compute_ratio(price, buy, exit_)
        score   = compute_score(ratio, r.get("note")) or r.get("score_sheet")
        upside  = compute_upside(price, fair, trim)
        beta    = m.get("beta")
        earnings = m.get("earnings")

        spark_prices = sparks.get(yf_s, [])
        spark_html   = make_sparkline(spark_prices)

        # Highlight "sous le radar" : Watchlist, statut achat, score élevé
        highlight = (
            r.get("portif", 0) == 0
            and statut in ("Strong buy", "Buy")
            and (score or -1) >= RADAR_SCORE_MIN
        )

        gf = str(r["gf_ticker"])
        name_upper = name.upper() if name else ""
        name_html  = (name_upper if name_upper
                      else f'<span style="color:#475569;font-style:italic">{gf}</span>')

        rows.append({
            "_statut_order": STATUT_ORDER.get(statut, 9),
            "_score":        float(score) if score is not None else -1.0,
            "_chg":          chg,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_statut":       statut,
            "_highlight":    highlight,
            "_maj_date":     r.get("last_update"),
            # colonnes affichées
            "MAJ":      fmt_maj(r.get("last_update"), earnings),
            "Ticker":   html_ticker(gf),
            "Société":  f'<span title="{name_upper}">{name_html}</span>',
            "Spark":    spark_html,
            "Prix":     fmt_price(price),
            "Var %":    html_var(chg),
            "Upside":   html_upside(upside),
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
# Tableau HTML
# ══════════════════════════════════════════════════════════════════════════════

CSS = """<style>
.wl-wrap{overflow-x:auto;max-height:72vh;overflow-y:auto;
  border-radius:8px;border:1px solid #1e293b}
.wl-table{width:100%;border-collapse:collapse;font-size:.82rem;color:#e2e8f0;
  table-layout:fixed}
.wl-table thead tr{position:sticky;top:0;z-index:2}
.wl-table th{background:#0f172a;color:#94a3b8;font-weight:600;
  padding:9px 8px;text-align:left;border-bottom:2px solid #334155;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wl-table th.c{text-align:center}
.wl-table td{padding:6px 8px;border-bottom:1px solid #1a2035;
  vertical-align:middle;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.wl-table td.c{text-align:center}
.wl-table tr.hl td{background:#0d2818}
.wl-table tr.hl:hover td{background:#0f3020}
.wl-table tbody tr:not(.hl):hover td{background:#ffffff08}
</style>"""


def render_table(rows: list[dict]) -> None:
    if not rows: st.info("Aucun titre."); return
    colgroup = "<colgroup>" + "".join(
        f'<col style="width:{COL_WIDTHS.get(c, "auto")}">' for c in DISPLAY_COLS) + "</colgroup>"
    th = "".join(
        f'<th class="{"c" if c in CENTER else ""}" title="{c}">{c}</th>'
        for c in DISPLAY_COLS)
    trs = []
    for r in rows:
        hl_class = " hl" if r["_highlight"] else ""
        tds = "".join(
            f'<td class="{"c" if c in CENTER else ("" if c != "Société" else "")}">{r[c]}</td>'
            for c in DISPLAY_COLS)
        trs.append(f'<tr class="{hl_class.strip()}">{tds}</tr>')
    st.markdown(
        CSS + f'<div class="wl-wrap"><table class="wl-table">'
        f'{colgroup}<thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Export XLS
# ══════════════════════════════════════════════════════════════════════════════

def export_xls(rows: list[dict], sheet_name: str = "Watchlist") -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    import re as _re

    def strip_html(s) -> str:
        return _re.sub(r"<[^>]+>", "", str(s)) if s else ""

    EXPORT_COLS = ["MAJ", "Ticker", "Société", "Prix", "Var %", "Upside",
                   "Score", "Buy", "Fair", "Trim", "Exit", "Qualité", "Beta",
                   "Statut", "Earnings"]

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    header_fill = PatternFill("solid", fgColor="0F172A")
    header_font = Font(bold=True, color="94A3B8", size=10)
    thin = Side(style="thin", color="334155")
    border = Border(bottom=Side(style="thin", color="1E293B"))

    for ci, col in enumerate(EXPORT_COLS, 1):
        cell = ws.cell(row=1, column=ci, value=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
        ws.column_dimensions[cell.column_letter].width = 14

    for ri, r in enumerate(rows, 2):
        hl = r.get("_highlight", False)
        row_fill = PatternFill("solid", fgColor="0D2818") if hl else None
        for ci, col in enumerate(EXPORT_COLS, 1):
            raw = r.get(col, "")
            val = strip_html(raw) if isinstance(raw, str) else raw
            # Replace — with empty
            if val == "—": val = ""
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.alignment = Alignment(horizontal="center" if col in CENTER else "left")
            cell.border = border
            if row_fill: cell.fill = row_fill

    # Freeze header row
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

SORT_OPTIONS = [
    "Statut + Score", "Ticker A→Z", "Score ↓", "Qualité ↓",
    "Upside ↓", "Var % ↑", "Var % ↓", "MAJ ↑ (plus ancien)", "MAJ ↓ (plus récent)",
]


def render_tab(df_sub: pd.DataFrame, prices: dict, meta: dict,
               sparks: dict, key: str) -> None:
    rows = build_rows(df_sub, prices, meta, sparks)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        search = st.text_input("Recherche", key=f"{key}_s", placeholder="Ticker ou société…")
    with c2:
        sort_choice = st.selectbox("Tri", SORT_OPTIONS, key=f"{key}_t")
    with c3:
        sf = st.selectbox("Statut", ["Tous", "Strong buy", "Buy", "Fair", "Trim", "Exit"],
                          key=f"{key}_f")

    if search:
        q = search.lower()
        rows = [r for r in rows if q in r["_ticker"].lower() or q in r["_name"].lower()]
    if sf != "Tous":
        rows = [r for r in rows if r["_statut"] == sf]

    if sort_choice == "Statut + Score":
        rows.sort(key=lambda r: (r["_statut_order"], -r["_score"]))
    elif sort_choice == "Ticker A→Z":
        rows.sort(key=lambda r: r["_ticker"])
    elif sort_choice in ("Score ↓", "Qualité ↓"):
        rows.sort(key=lambda r: -r["_score"])
    elif sort_choice == "Upside ↓":
        rows.sort(key=lambda r: (r["Upside"] == "—" or r["Upside"] == "", r["Upside"]))
    elif sort_choice == "Var % ↑":
        rows.sort(key=lambda r: (r["_chg"] is None, -(r["_chg"] or 0)))
    elif sort_choice == "Var % ↓":
        rows.sort(key=lambda r: (r["_chg"] is None, r["_chg"] or 0))
    elif sort_choice == "MAJ ↑ (plus ancien)":
        rows.sort(key=lambda r: (r["_maj_date"] is None, r["_maj_date"] or date.min))
    elif sort_choice == "MAJ ↓ (plus récent)":
        rows.sort(key=lambda r: (r["_maj_date"] is None, r["_maj_date"] or date.min), reverse=True)

    render_table(rows)

    # Résumé "sous le radar"
    radar = [r for r in rows if r["_highlight"]]
    if radar:
        st.caption(f"🟢 {len(radar)} titre(s) sous le radar (Score ≥ {RADAR_SCORE_MIN}, statut achat)")

    # Export XLS
    xls_bytes = export_xls(rows, sheet_name=key.upper())
    st.download_button(
        label="Télécharger XLS",
        data=xls_bytes,
        file_name=f"watchlist_{key}_{date.today():%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"{key}_xls",
    )

    missing = [r["_ticker"] for r in rows if not r["_price_ok"]]
    if missing:
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# Onglet Debug
# ══════════════════════════════════════════════════════════════════════════════

def render_debug(tickers_df: pd.DataFrame, prices: dict, meta: dict) -> None:
    st.subheader("Diagnostics")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Mapping GF → Yahoo impossible**")
        no_map = tickers_df[tickers_df["yf_ticker"].isna()][["gf_ticker"]].copy()
        st.dataframe(no_map, use_container_width=True, hide_index=True) if not no_map.empty \
            else st.success("Aucun problème")

        st.markdown("**Tickers sans cours Yahoo**")
        no_price = tickers_df[tickers_df["yf_ticker"].apply(
            lambda t: prices.get(str(t) if pd.notna(t) else "", {}).get("price") is None
        )][["gf_ticker", "yf_ticker"]].copy()
        st.dataframe(no_price, use_container_width=True, hide_index=True) if not no_price.empty \
            else st.success("Aucun problème")

    with col2:
        st.markdown("**Tickers sans nom**")
        no_name = tickers_df[tickers_df.apply(
            lambda r: not r["name"] and not meta.get(str(r["yf_ticker"]) if pd.notna(r["yf_ticker"]) else "", {}).get("name"),
            axis=1
        )][["gf_ticker", "yf_ticker"]].copy()
        st.dataframe(no_name, use_container_width=True, hide_index=True) if not no_name.empty \
            else st.success("Aucun problème")

        st.markdown("**Tickers sans earnings**")
        no_earn = tickers_df[tickers_df["yf_ticker"].apply(
            lambda t: meta.get(str(t) if pd.notna(t) else "", {}).get("earnings") is None
        )][["gf_ticker", "yf_ticker"]].copy()
        st.dataframe(no_earn, use_container_width=True, hide_index=True) if not no_earn.empty \
            else st.success("Aucun problème")

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

with st.spinner("Chargement de la liste de titres…"):
    try:
        tickers_df, data_source = load_tickers()
    except Exception as exc:
        st.error(str(exc)); st.stop()

pf_df    = tickers_df[tickers_df["portif"] == 1].copy()
wl_df    = tickers_df[tickers_df["portif"] != 1].copy()
valid_yf = tuple(str(t) for t in tickers_df["yf_ticker"].dropna() if str(t).strip())

# Métriques
m1, m2, m3 = st.columns(3)
m1.metric("Portefeuille", len(pf_df))
m2.metric("Watchlist", len(wl_df))
m3.metric("Dernière MAJ", st.session_state.get("last_fetch_ts", "—"))

# Bouton Actualiser
rc1, rc2 = st.columns([1, 4])
with rc1:
    if st.button("Actualiser", type="primary", use_container_width=True):
        fetch_prices.clear(); load_tickers.clear()
        fetch_meta.clear();   fetch_sparklines.clear()
        st.rerun()
with rc2:
    n = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0
    st.caption(f"Source : **{data_source}** · {len(valid_yf)} tickers · {n} paquets · cache {REFRESH_TTL//60} min")

# Données
with st.spinner("Métadonnées (nom / beta / earnings)…"):
    meta = fetch_meta(valid_yf)
with st.spinner("Sparklines 52 semaines…"):
    sparks = fetch_sparklines(valid_yf)
with st.spinner(f"Cours ({len(valid_yf)} titres)…"):
    prices = fetch_prices(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)
s1, s2, _ = st.columns(3)
s1.metric("Prix récupérés", ok)
s2.metric("Manquants", len(valid_yf) - ok)

st.divider()

tab1, tab2, tab3 = st.tabs([
    f"Portefeuille ({len(pf_df)})",
    f"Watchlist ({len(wl_df)})",
    "🔧 Debug",
])
with tab1:
    render_tab(pf_df, prices, meta, sparks, key="pf")
with tab2:
    render_tab(wl_df, prices, meta, sparks, key="wl")
with tab3:
    render_debug(tickers_df, prices, meta)

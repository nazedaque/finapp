from __future__ import annotations

import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import pandas as pd
import streamlit as st
import yfinance as yf

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Watchlist",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

SHEET_ID      = "1KQ0eolfB-UH-N-jQo2WDxsmVNT3I4IhiTEbdIfcPvbA"
SHEET_NAME    = "Travail"
SHEET_CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
)
CSV_FALLBACK      = "tickers.csv"
REFRESH_TTL       = 30 * 60
SHEET_TTL         = 3_600
META_TTL          = 86_400   # 24h pour noms, beta, earnings
BATCH_SIZE        = 75
DOWNLOAD_PERIOD   = "5d"
DOWNLOAD_INTERVAL = "30m"

# ══════════════════════════════════════════════════════════════════════════════
# Exchanges & overrides
# ══════════════════════════════════════════════════════════════════════════════

EXCHANGE_MAP: dict[str, str] = {
    "EPA": ".PA", "ETR": ".DE", "FRA": ".F",
    "LON": ".L",  "AMS": ".AS", "BIT": ".MI",
    "BME": ".MC", "STO": ".ST", "SWX": ".SW",
    "TYO": ".T",  "TSE": ".TO", "HKG": ".HK",
    "SGX": ".SI", "HEL": ".HE", "VIE": ".VI",
    "CPH": ".CO", "EBR": ".BR", "WSE": ".WA",
    "CVE": ".V",  "NYSE": "",   "NASDAQ": "",
}
MANUAL_OVERRIDES: dict[str, str] = {
    "JST":       "JST.DE",
    "BETS-B":    "BETS-B.ST",
    "MOUR":      "MOUR.BR",
    "EPA:HAVAS": "HAVAS.AS",
    "TSE:DHT.U": "DHT-UN.TO",
    "TSE:CTC.A": "CTC-A.TO",
    "CPH:VAR":   "VAR.OL",
}

# ══════════════════════════════════════════════════════════════════════════════
# Statuts
# ══════════════════════════════════════════════════════════════════════════════

STATUT_ORDER = {"Strong buy": 0, "Buy": 1, "Fair": 2, "Trim": 3, "Exit": 4, "": 9}
STATUT_COLOR = {
    "Strong buy": "#1f8b4c", "Buy": "#6dbf4b", "Fair": "#d4b000",
    "Trim": "#e67e22",       "Exit": "#c0392b", "": "#64748b",
}

# ══════════════════════════════════════════════════════════════════════════════
# Layout : colonnes et largeurs (identiques entre onglets)
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "MAJ",
    "Ticker", "Société", "Prix", "Var %",
    "Score", "Buy", "Fair", "Trim", "Exit",
    "Qualité", "Beta", "Statut", "Earnings", "🔗",
]

COL_WIDTHS = {
    "MAJ":      "38px",
    "Ticker":   "100px",
    "Société":  "220px",
    "Prix":     "82px",
    "Var %":    "82px",
    "Score":    "55px",
    "Buy":      "78px",
    "Fair":     "78px",
    "Trim":     "78px",
    "Exit":     "78px",
    "Qualité":  "62px",
    "Beta":     "60px",
    "Statut":   "92px",
    "Earnings": "100px",
    "🔗":       "32px",
}

CENTER = {"MAJ", "Prix", "Var %", "Score", "Buy", "Fair", "Trim", "Exit",
          "Qualité", "Beta", "Statut", "Earnings", "🔗"}

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def normalize_col(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", str(s))
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").strip().lower()


def gf_to_yf(gf: str) -> str | None:
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
    if re.match(r"^\d{1,3}(,\d{3})+$", s):
        return float(s.replace(",", ""))
    if re.match(r"^\d{1,3}(,\d{3})+,\d{1,2}$", s):
        parts = s.split(",")
        return float("".join(parts[:-1]) + "." + parts[-1])
    if "," in s:
        return float(s.replace(".", "").replace(",", "."))
    if re.match(r"^\d{1,3}(\.\d{3})+$", s):
        return float(s.replace(".", ""))
    try: return float(s)
    except ValueError: return None

# ══════════════════════════════════════════════════════════════════════════════
# Chargement du sheet
# ══════════════════════════════════════════════════════════════════════════════

SHEET_COL_NORMALIZED = {
    "ticker": "gf_ticker", "societe": "name", "portif": "portif",
    "note": "note", "buy": "buy", "fair": "fair", "trim": "trim",
    "exit": "exit", "url": "url", "spot": "spot_sheet",
    "score mixte": "score_sheet", "last update": "last_update",
}
NUMERIC_COLS = ["note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet"]


@st.cache_data(ttl=SHEET_TTL, show_spinner=False)
def load_tickers() -> tuple[pd.DataFrame, str]:
    source = "Google Sheet"
    try:
        df = pd.read_csv(SHEET_CSV_URL, encoding="utf-8", header=0, dtype=str)
    except Exception:
        try:
            df = pd.read_csv(CSV_FALLBACK, header=0, dtype=str)
            source = "tickers.csv (fallback)"
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

    # Parsing de la date de dernière MAJ (format JJ/MM/AAAA depuis le sheet)
    if "last_update" in df.columns:
        df["last_update"] = pd.to_datetime(
            df["last_update"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    else:
        df["last_update"] = None

    df["yf_ticker"] = df["gf_ticker"].astype(str).apply(gf_to_yf)
    return df.reset_index(drop=True), source

# ══════════════════════════════════════════════════════════════════════════════
# Métadonnées Yahoo : nom, beta, earnings (en parallèle, cache 24h)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_meta(t: str) -> tuple[str, dict]:
    result: dict = {"name": "", "beta": None, "earnings": None, "last_earnings": None}
    try:
        ticker_obj = yf.Ticker(t)
        # Nom via endpoint chart (fiable pour toutes places)
        try:
            ticker_obj.history(period="2d", interval="1d")
            meta = getattr(ticker_obj, "history_metadata", None) or {}
            result["name"] = (meta.get("shortName") or meta.get("longName") or "").strip()
        except Exception:
            pass
        # Beta + Earnings via .info
        try:
            info = ticker_obj.info or {}
            if not result["name"]:
                result["name"] = (info.get("shortName") or info.get("longName") or "").strip()
            beta = info.get("beta")
            if beta is not None:
                result["beta"] = float(beta)
            # Prochains earnings
            ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
            if ts and isinstance(ts, (int, float)) and ts > 0:
                d = datetime.utcfromtimestamp(ts).date()
                if d >= date.today():
                    result["earnings"] = d
        except Exception:
            pass
        # Derniers earnings passés (via earnings_dates)
        try:
            ed = ticker_obj.earnings_dates
            if ed is not None and not ed.empty and "Reported EPS" in ed.columns:
                past = ed[ed["Reported EPS"].notna()]
                if not past.empty:
                    last_ts = past.index[0]
                    # L'index peut être tz-aware
                    if hasattr(last_ts, "date"):
                        result["last_earnings"] = last_ts.date()
        except Exception:
            pass
    except Exception:
        pass
    return t, result


@st.cache_data(ttl=META_TTL, show_spinner=False)
def fetch_ticker_metadata(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    metadata: dict[str, dict] = {}
    empty = {"name": "", "beta": None, "earnings": None}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_one_meta, t): t for t in yf_tickers}
        for future in as_completed(futures, timeout=180):
            try:
                t, data = future.result(timeout=15)
                metadata[t] = data
            except Exception:
                metadata[futures[future]] = dict(empty)
    return metadata

# ══════════════════════════════════════════════════════════════════════════════
# Cours Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def _chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i: i + size]


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
                tickers=" ".join(batch), period=DOWNLOAD_PERIOD,
                interval=DOWNLOAD_INTERVAL, auto_adjust=False,
                progress=False, group_by="ticker", threads=True, prepost=False,
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

def fmt_earnings(d: date | None) -> str:
    if d is None: return "—"
    today = date.today()
    if d < today: return "—"
    s = d.strftime("%d-%m-%Y")
    if (d - today).days <= 7:
        return f'<span style="color:#ef4444;font-weight:700">{s}</span>'
    return s

def html_var(chg) -> str:
    if chg is None or (isinstance(chg, float) and pd.isna(chg)):
        return '<span style="color:#64748b">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "▲" if chg >= 0 else "▼"
    return f'<span style="color:{c}">{a}&nbsp;{abs(chg):.2f}%</span>'

def html_statut(statut) -> str:
    c = STATUT_COLOR.get(statut, "#64748b")
    return f'<span style="color:{c};font-weight:600">{statut or "—"}</span>'

def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith("http"): return ""
    return (f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
            'style="color:#7dd3fc;font-size:1.1rem;text-decoration:none">🔗</a>')

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict, metadata: dict) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t   = r.get("yf_ticker")
        q      = prices.get(str(yf_t), {}) if pd.notna(yf_t) else {}
        meta   = metadata.get(str(yf_t), {}) if pd.notna(yf_t) else {}

        price  = q.get("price") or r.get("spot_sheet")
        chg    = q.get("chg")

        # Nom : sheet → Yahoo metadata
        name = r.get("name", "") or meta.get("name", "")
        name_upper = name.upper() if name else ""

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        statut = compute_statut(price, buy, fair, trim, exit_)
        ratio  = compute_ratio(price, buy, exit_)
        score  = compute_score(ratio, r.get("note")) or r.get("score_sheet")

        beta     = meta.get("beta")
        earnings = meta.get("earnings")
        last_earnings = meta.get("last_earnings")

        # Colonne MAJ : comparaison date de mise à jour vs derniers earnings
        last_update = r.get("last_update")  # date or None
        if last_update and last_earnings:
            maj_html = "✔️" if last_update >= last_earnings else "⚠️"
        else:
            maj_html = ""

        gf = str(r["gf_ticker"])
        name_html = (name_upper if name_upper
                     else f'<span style="color:#475569;font-style:italic">{gf}</span>')

        rows.append({
            "_statut_order": STATUT_ORDER.get(statut, 9),
            "_score":        float(score) if score is not None else -1.0,
            "_chg":          chg,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_statut":       statut,
            "MAJ":           maj_html,
            "Ticker":        f'<span style="color:#93c5fd;font-size:.8rem;font-family:monospace">{gf}</span>',
            "Société":       f'<span title="{name_upper}">{name_html}</span>',
            "Prix":          fmt_price(price),
            "Var %":         html_var(chg),
            "Score":         fmt_score(score),
            "Buy":           fmt_price(buy),
            "Fair":          fmt_price(fair),
            "Trim":          fmt_price(trim),
            "Exit":          fmt_price(exit_),
            "Qualité":       fmt_note(r.get("note")),
            "Beta":          fmt_beta(beta),
            "Statut":        html_statut(statut),
            "Earnings":      fmt_earnings(earnings),
            "🔗":            html_link(r.get("url")),
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# Tableau HTML avec largeurs fixes identiques entre onglets
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
.wl-table td.soc{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wl-table tbody tr:hover td{background:#ffffff08}
</style>"""


def render_table(rows: list[dict]) -> None:
    if not rows:
        st.info("Aucun titre.")
        return

    colgroup = "<colgroup>" + "".join(
        f'<col style="width:{COL_WIDTHS.get(c, "auto")}">'
        for c in DISPLAY_COLS
    ) + "</colgroup>"

    th = "".join(
        f'<th class="{"c" if c in CENTER else ""}" title="{c}">{c}</th>'
        for c in DISPLAY_COLS
    )
    trs = []
    for r in rows:
        tds = "".join(
            f'<td class="{"c" if c in CENTER else ("soc" if c == "Société" else "")}">'
            f'{r[c]}</td>'
            for c in DISPLAY_COLS
        )
        trs.append(f"<tr>{tds}</tr>")

    st.markdown(
        CSS
        + f'<div class="wl-wrap"><table class="wl-table">'
        f'{colgroup}<thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

def render_tab(df_sub: pd.DataFrame, prices: dict, metadata: dict, key: str) -> None:
    rows = build_rows(df_sub, prices, metadata)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        search = st.text_input("Recherche", key=f"{key}_s", placeholder="Ticker ou société…")
    with c2:
        sort_choice = st.selectbox(
            "Tri",
            ["Statut + Score", "Ticker A→Z", "Score ↓", "Qualité ↓", "Var % ↑", "Var % ↓"],
            key=f"{key}_t",
        )
    with c3:
        sf = st.selectbox(
            "Statut", ["Tous", "Strong buy", "Buy", "Fair", "Trim", "Exit"],
            key=f"{key}_f",
        )

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
    elif sort_choice == "Var % ↑":
        rows.sort(key=lambda r: (r["_chg"] is None, -(r["_chg"] or 0)))
    elif sort_choice == "Var % ↓":
        rows.sort(key=lambda r: (r["_chg"] is None, r["_chg"] or 0))

    render_table(rows)

    missing = [r["_ticker"] for r in rows if not r["_price_ok"]]
    if missing:
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

with st.spinner("Chargement de la liste de titres…"):
    try:
        tickers_df, data_source = load_tickers()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

pf_df    = tickers_df[tickers_df["portif"] == 1].copy()
wl_df    = tickers_df[tickers_df["portif"] != 1].copy()
valid_yf = tuple(str(t) for t in tickers_df["yf_ticker"].dropna() if str(t).strip())

# Métriques (sans Total, sans titre)
m1, m2, m3 = st.columns(3)
m1.metric("Portefeuille", len(pf_df))
m2.metric("Watchlist", len(wl_df))
m3.metric("Dernière MAJ", st.session_state.get("last_fetch_ts", "—"))

# Bouton Actualiser
rc1, rc2 = st.columns([1, 4])
with rc1:
    if st.button("Actualiser", type="primary", use_container_width=True):
        fetch_prices.clear()
        load_tickers.clear()
        fetch_ticker_metadata.clear()
        st.rerun()
with rc2:
    from math import ceil
    n = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0
    st.caption(f"Source : **{data_source}** · {len(valid_yf)} tickers · {n} paquets Yahoo · cache {REFRESH_TTL//60} min")

# Métadonnées (nom, beta, earnings) — cache 24h
with st.spinner(f"Chargement des métadonnées ({len(valid_yf)} titres)…"):
    metadata = fetch_ticker_metadata(valid_yf)

# Cours Yahoo
with st.spinner(f"Récupération de {len(valid_yf)} cours…"):
    prices = fetch_prices(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)
s1, s2, _ = st.columns(3)
s1.metric("Prix récupérés", ok)
s2.metric("Manquants", len(valid_yf) - ok)

st.divider()

tab1, tab2 = st.tabs([f"Portefeuille ({len(pf_df)})", f"Watchlist ({len(wl_df)})"])
with tab1:
    render_tab(pf_df, prices, metadata, key="pf")
with tab2:
    render_tab(wl_df, prices, metadata, key="wl")

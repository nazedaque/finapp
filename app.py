from __future__ import annotations

import io
import unicodedata
from datetime import datetime, timezone
from math import ceil

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
REFRESH_TTL       = 30 * 60   # cache cours Yahoo (secondes)
SHEET_TTL         = 3_600     # cache sheet (secondes)
BATCH_SIZE        = 75
DOWNLOAD_PERIOD   = "5d"
DOWNLOAD_INTERVAL = "30m"

# ══════════════════════════════════════════════════════════════════════════════
# Mapping exchanges
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
    "JST":         "JST.DE",
    "BETS-B":      "BETS-B.ST",
    "MOUR":        "MOUR.BR",
    "EPA:HAVAS":   "HAVAS.AS",    # Havas N.V. → Amsterdam
    "TSE:DHT.U":   "DHT-UN.TO",  # DRI Healthcare Trust (UN, pas U)
    "TSE:CTC.A":   "CTC-A.TO",   # Canadian Tire classe A
    "CPH:VAR":     "VAR.OL",     # Vår Energi → Oslo
}

# Tickers connus comme introuvables sur Yahoo → on utilisera le prix Google
YAHOO_UNKNOWN = {"VIL.PA"}

# ══════════════════════════════════════════════════════════════════════════════
# Statuts
# ══════════════════════════════════════════════════════════════════════════════

STATUT_ORDER = {"Strong buy": 0, "Buy": 1, "Fair": 2, "Trim": 3, "Exit": 4, "": 9}
STATUT_COLOR = {
    "Strong buy": "#1f8b4c", "Buy": "#6dbf4b", "Fair": "#d4b000",
    "Trim": "#e67e22",       "Exit": "#c0392b", "": "#64748b",
}

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def normalize_col(s: str) -> str:
    return strip_accents(str(s)).strip().lower()


def gf_to_yf(gf: str) -> str | None:
    gf = str(gf).strip()
    if not gf:
        return None
    if gf in MANUAL_OVERRIDES:
        return MANUAL_OVERRIDES[gf]
    if ":" not in gf:
        return gf
    exchange, symbol = gf.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange)
    if suffix is None:
        return None
    return f"{symbol}{suffix}"


def parse_fr_number(v) -> float | None:
    """Convertit '20,36' ou '1.234,56' en float."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s in ("#REF!", "#N/A", "#VALUE!", "—", ""):
        return None
    # enlève espaces et points de milliers, remplace virgule décimale
    s = s.replace("\xa0", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

# ══════════════════════════════════════════════════════════════════════════════
# Chargement du sheet
# ══════════════════════════════════════════════════════════════════════════════

# Mapping : nom normalisé (sans accents, lowercase) → nom interne
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
    "spot":        "spot_sheet",       # prix Google Finance (fallback)
    "score mixte": "score_sheet",
}

NUMERIC_INTERNAL = ["note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet"]


@st.cache_data(ttl=SHEET_TTL, show_spinner=False)
def load_tickers() -> tuple[pd.DataFrame, str]:
    source = "Google Sheet"
    try:
        # Lire en UTF-8 — gviz retourne de l'UTF-8
        # On ne met PAS decimal=',' ici : on parse manuellement après
        df = pd.read_csv(SHEET_CSV_URL, encoding="utf-8", header=0, dtype=str)
    except Exception:
        try:
            df = pd.read_csv(CSV_FALLBACK, header=0, dtype=str)
            source = "tickers.csv (fallback)"
        except Exception as exc:
            raise RuntimeError(f"Impossible de charger les données : {exc}") from exc

    # Renommage robuste : on normalise les noms de colonnes
    rename_map = {}
    for raw_col in df.columns:
        norm = normalize_col(raw_col)
        if norm in SHEET_COL_NORMALIZED:
            rename_map[raw_col] = SHEET_COL_NORMALIZED[norm]
    df = df.rename(columns=rename_map)

    # Colonnes manquantes
    for col in list(SHEET_COL_NORMALIZED.values()):
        if col not in df.columns:
            df[col] = pd.NA

    # Filtre lignes vides
    df = df[df["gf_ticker"].notna()].copy()
    df = df[~df["gf_ticker"].astype(str).str.strip().isin(["", "Ticker"])].copy()

    # Nettoyage portif
    df["portif"] = df["portif"].map(
        lambda v: 1 if str(v).strip() in ("1", "TRUE", "True", "true") else 0
    )

    # Noms : nettoyer les #REF!
    df["name"] = df["name"].apply(
        lambda v: "" if (pd.isna(v) or str(v).strip().startswith("#")) else str(v).strip()
    )

    # Colonnes numériques : parser les nombres FR (virgule décimale)
    for col in NUMERIC_INTERNAL:
        if col in df.columns:
            df[col] = df[col].apply(parse_fr_number)

    # Ticker Yahoo
    df["yf_ticker"] = df["gf_ticker"].astype(str).apply(gf_to_yf)

    return df.reset_index(drop=True), source

# ══════════════════════════════════════════════════════════════════════════════
# Récupération des noms manquants via yfinance
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=SHEET_TTL * 6, show_spinner=False)
def fetch_missing_names(yf_tickers_without_name: tuple[str, ...]) -> dict[str, str]:
    names = {}
    for t in yf_tickers_without_name:
        try:
            info = yf.Ticker(t).fast_info
            names[t] = getattr(info, "display_name", "") or ""
        except Exception:
            names[t] = ""
    return names

# ══════════════════════════════════════════════════════════════════════════════
# Récupération des cours Yahoo
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
    if closes.empty:
        return None, None
    price = float(closes.iloc[-1])
    dates = pd.to_datetime(closes.index).tz_localize(None).normalize()
    prev  = closes[dates < dates[-1]]
    chg   = None
    if not prev.empty:
        p0 = float(prev.iloc[-1])
        if p0:
            chg = (price - p0) / p0 * 100
    return price, chg


@st.cache_data(ttl=REFRESH_TTL, show_spinner=False)
def fetch_prices(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for batch in _chunked(list(yf_tickers), BATCH_SIZE):
        try:
            data = yf.download(
                tickers=" ".join(batch),
                period=DOWNLOAD_PERIOD,
                interval=DOWNLOAD_INTERVAL,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=True,
                prepost=False,
            )
        except Exception:
            for t in batch:
                results[t] = {"price": None, "chg": None}
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
    vals = [price, buy, fair, trim, exit_]
    if any(v is None or (isinstance(v, float) and pd.isna(v)) for v in vals):
        return ""
    p, b, f, k, e = (float(v) for v in vals)
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
    except Exception:
        return None


def compute_score(ratio, note) -> float | None:
    try:
        return round((0.6 * float(ratio) + 0.4 * float(note) / 100) * 100, 1)
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════════════════
# Formatage
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{float(v):,.2f}"

def fmt_note(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return str(int(float(v)))

def fmt_score(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return f"{float(v):.1f}"

def html_var(chg) -> str:
    if chg is None or (isinstance(chg, float) and pd.isna(chg)):
        return '<span style="color:#64748b">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "▲" if chg >= 0 else "▼"
    return f'<span style="color:{c}">{a}&nbsp;{abs(chg):.2f}%</span>'

def html_bar(ratio, statut) -> str:
    if ratio is None: return ""
    color = STATUT_COLOR.get(statut, "#64748b")
    pct   = ratio * 100
    return (
        '<div style="width:72px;height:9px;background:#1e293b;border-radius:5px;'
        'overflow:hidden;display:inline-block">'
        f'<div style="width:{pct:.1f}%;height:100%;background:{color};'
        'border-radius:5px"></div></div>'
    )

def html_statut(statut) -> str:
    c = STATUT_COLOR.get(statut, "#64748b")
    return f'<span style="color:{c};font-weight:600">{statut or "—"}</span>'

def html_link(url) -> str:
    if not url or (isinstance(url, float) and pd.isna(url)): return ""
    u = str(url).strip()
    if not u.startswith("http"): return ""
    return (
        f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
        'style="color:#7dd3fc;font-size:1.1rem;text-decoration:none">🔗</a>'
    )

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict, names_fallback: dict) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t = r.get("yf_ticker")
        q    = prices.get(str(yf_t), {}) if pd.notna(yf_t) else {}

        # Prix : Yahoo en priorité, puis Spot du sheet
        price = q.get("price")
        chg   = q.get("chg")
        if price is None:
            price = r.get("spot_sheet")  # fallback Google Finance

        # Nom : sheet en priorité, puis yfinance
        name = r.get("name", "")
        if not name and pd.notna(yf_t):
            name = names_fallback.get(str(yf_t), "")

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        statut = compute_statut(price, buy, fair, trim, exit_)
        ratio  = compute_ratio(price, buy, exit_)
        score  = compute_score(ratio, r.get("note"))

        rows.append({
            "_statut_order": STATUT_ORDER.get(statut, 9),
            "_score":        score if score is not None else -1.0,
            "_chg":          chg,
            "_price_ok":     price is not None,
            "_ticker":       str(r["gf_ticker"]),
            "_name":         name,
            "_statut":       statut,
            "Ticker":  f'<code style="color:#93c5fd;font-size:.8rem">{r["gf_ticker"]}</code>',
            "Société":       name or f'<span style="color:#475569">{r["gf_ticker"]}</span>',
            "Prix":          fmt_price(price),
            "Var %":         html_var(chg),
            "Barre":         html_bar(ratio, statut),
            "Qualité":       fmt_note(r.get("note")),
            "Buy":           fmt_price(buy),
            "Fair":          fmt_price(fair),
            "Trim":          fmt_price(trim),
            "Exit":          fmt_price(exit_),
            "Score":         fmt_score(score),
            "Statut":        html_statut(statut),
            "🔗":            html_link(r.get("url")),
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# Tableau HTML
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "Ticker", "Société", "Prix", "Var %", "Barre",
    "Qualité", "Buy", "Fair", "Trim", "Exit",
    "Score", "Statut", "🔗",
]
CENTER = {"Qualité", "Score", "Barre", "🔗", "Statut", "Prix", "Var %"}

CSS = """<style>
.wl-wrap{overflow-x:auto;max-height:72vh;overflow-y:auto;
  border-radius:8px;border:1px solid #1e293b}
.wl-table{width:100%;border-collapse:collapse;font-size:.82rem;color:#e2e8f0}
.wl-table thead tr{position:sticky;top:0;z-index:2}
.wl-table th{background:#0f172a;color:#94a3b8;font-weight:600;
  padding:9px 11px;text-align:left;border-bottom:2px solid #334155;
  white-space:nowrap}
.wl-table th.c{text-align:center}
.wl-table td{padding:6px 11px;border-bottom:1px solid #1a2035;
  vertical-align:middle;white-space:nowrap}
.wl-table td.c{text-align:center}
.wl-table tbody tr:hover td{background:#ffffff08}
</style>"""


def render_table(rows: list[dict]) -> None:
    if not rows:
        st.info("Aucun titre.")
        return
    th  = "".join(
        f'<th class="{"c" if c in CENTER else ""}">{c}</th>'
        for c in DISPLAY_COLS
    )
    trs = []
    for r in rows:
        tds = "".join(
            f'<td class="{"c" if c in CENTER else ""}">{r[c]}</td>'
            for c in DISPLAY_COLS
        )
        trs.append(f"<tr>{tds}</tr>")
    st.markdown(
        CSS
        + f'<div class="wl-wrap"><table class="wl-table">'
        f'<thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>',
        unsafe_allow_html=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# Rendu d'un onglet
# ══════════════════════════════════════════════════════════════════════════════

def render_tab(df_sub: pd.DataFrame, prices: dict, names_fb: dict, key: str) -> None:
    rows = build_rows(df_sub, prices, names_fb)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        search = st.text_input(
            "Recherche", key=f"{key}_s",
            placeholder="🔍 Ticker ou société…", label_visibility="collapsed",
        )
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
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours Yahoo ni Google"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

st.title("📈 Watchlist Boursière")
st.caption("Cours : Yahoo Finance (principal) + Google Finance via sheet (fallback).")

# ── Chargement sheet ──────────────────────────────────────────────────────────
with st.spinner("Chargement de la liste de titres…"):
    try:
        tickers_df, data_source = load_tickers()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

pf_df    = tickers_df[tickers_df["portif"] == 1].copy()
wl_df    = tickers_df[tickers_df["portif"] != 1].copy()
valid_yf = tuple(str(t) for t in tickers_df["yf_ticker"].dropna() if str(t).strip())

# ── Noms manquants via yfinance ───────────────────────────────────────────────
no_name_yf = tuple(
    str(r["yf_ticker"])
    for _, r in tickers_df.iterrows()
    if not r["name"] and pd.notna(r.get("yf_ticker"))
)
if no_name_yf:
    with st.spinner(f"Récupération de {len(no_name_yf)} noms manquants…"):
        names_fallback = fetch_missing_names(no_name_yf)
else:
    names_fallback = {}

# ── Métriques ─────────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total", len(tickers_df))
m2.metric("Portefeuille", len(pf_df))
m3.metric("Watchlist", len(wl_df))
m4.metric("Dernière MAJ", st.session_state.get("last_fetch_ts", "—"))

# ── Bouton Actualiser (vide LES DEUX caches) ──────────────────────────────────
rc1, rc2 = st.columns([1, 4])
with rc1:
    if st.button("🔄 Actualiser", type="primary", use_container_width=True):
        fetch_prices.clear()
        load_tickers.clear()          # ← vide aussi le cache du sheet
        fetch_missing_names.clear()
        st.rerun()
with rc2:
    n_batches = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0
    st.caption(
        f"Source : **{data_source}** · {len(valid_yf)} tickers · "
        f"{n_batches} paquets Yahoo · cache cours {REFRESH_TTL // 60} min"
    )

# ── Cours Yahoo ───────────────────────────────────────────────────────────────
with st.spinner(f"Récupération de {len(valid_yf)} cours Yahoo…"):
    prices = fetch_prices(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)
ko = len(valid_yf) - ok
s1, s2, _ = st.columns(3)
s1.metric("✅ Prix Yahoo récupérés", ok)
s2.metric("⚠️ Fallback Google / manquants", ko)

st.divider()

# ── Onglets ───────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs([f"💼 Portefeuille ({len(pf_df)})", f"👁 Watchlist ({len(wl_df)})"])
with tab1:
    render_tab(pf_df, prices, names_fallback, key="pf")
with tab2:
    render_tab(wl_df, prices, names_fallback, key="wl")

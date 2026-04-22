from __future__ import annotations

import re
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
REFRESH_TTL       = 30 * 60
SHEET_TTL         = 3_600
NAMES_TTL         = 86_400   # noms en cache 24h
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
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def normalize_col(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", str(s))
    ascii_ = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    return ascii_.strip().lower()


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
    return f"{symbol}{suffix}" if suffix is not None else None


def parse_num(v) -> float | None:
    """
    Parse robuste des nombres exportés par Google Sheets (locale FR).
    Gère : virgule décimale '20,36', séparateur de milliers '2,300',
    et nombres purs '2300', '900'.
    """
    if v is None:
        return None
    s = str(v).strip().replace("\xa0", "").replace(" ", "")
    if not s or s in ("#REF!", "#N/A", "#VALUE!", "#ERROR!", "—", ""):
        return None
    # Entier avec séparateur de milliers (virgule) : '2,300' ou '1,234,567'
    if re.match(r"^\d{1,3}(,\d{3})+$", s):
        return float(s.replace(",", ""))
    # Milliers + décimale : '1,234,56'
    if re.match(r"^\d{1,3}(,\d{3})+,\d{1,2}$", s):
        parts = s.split(",")
        return float("".join(parts[:-1]) + "." + parts[-1])
    # Décimale virgule standard : '20,36'
    if "," in s:
        return float(s.replace(".", "").replace(",", "."))
    # Milliers avec point : '1.234'
    if re.match(r"^\d{1,3}(\.\d{3})+$", s):
        return float(s.replace(".", ""))
    try:
        return float(s)
    except ValueError:
        return None

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
            raise RuntimeError(f"Impossible de charger les données : {exc}") from exc

    # Renommage robuste (insensible aux accents et à la casse)
    rename_map = {}
    for col in df.columns:
        norm = normalize_col(col)
        if norm in SHEET_COL_NORMALIZED:
            rename_map[col] = SHEET_COL_NORMALIZED[norm]
    df = df.rename(columns=rename_map)

    for col in list(SHEET_COL_NORMALIZED.values()):
        if col not in df.columns:
            df[col] = pd.NA

    # Supprimer lignes vides ou header dupliqué
    df = df[df["gf_ticker"].notna()].copy()
    df = df[~df["gf_ticker"].astype(str).str.strip().isin(["", "Ticker", "gf_ticker"])].copy()

    # portif : 1 = portefeuille, 0 = watchlist
    df["portif"] = df["portif"].map(
        lambda v: 1 if str(v).strip() in ("1", "TRUE", "True", "true") else 0
    )

    # Noms : nettoyer les erreurs de formule
    df["name"] = df["name"].apply(
        lambda v: "" if (pd.isna(v) or str(v).strip().startswith("#")) else str(v).strip()
    )

    # Colonnes numériques
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_num)

    df["yf_ticker"] = df["gf_ticker"].astype(str).apply(gf_to_yf)
    return df.reset_index(drop=True), source

# ══════════════════════════════════════════════════════════════════════════════
# Noms manquants via yfinance
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=NAMES_TTL, show_spinner=False)
def fetch_missing_names(yf_tickers: tuple[str, ...]) -> dict[str, str]:
    names: dict[str, str] = {}
    for t in yf_tickers:
        try:
            info = yf.Ticker(t).info
            name = info.get("shortName") or info.get("longName") or ""
            names[t] = str(name).strip()
        except Exception:
            names[t] = ""
    return names

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
    if any(v is None or (isinstance(v, float) and pd.isna(v))
           for v in [price, buy, fair, trim, exit_]):
        return ""
    p, b, f, k, e = float(price), float(buy), float(fair), float(trim), float(exit_)
    if p <= b: return "Strong buy"
    if p <= f: return "Buy"
    if p <= k: return "Fair"
    if p <= e: return "Trim"
    return "Exit"


def compute_ratio(price, buy, exit_) -> float | None:
    try:
        p, b, e = float(price), float(buy), float(exit_)
        if e <= b:
            return None
        return max(0.0, min(1.0, (e - p) / (e - b)))
    except Exception:
        return None


def compute_score(ratio, note) -> float | None:
    try:
        return (0.6 * float(ratio) + 0.4 * float(note) / 100) * 100
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════════════════
# Formatage
# ══════════════════════════════════════════════════════════════════════════════

def fmt_price(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):,.2f}"

def fmt_note(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return str(int(float(v)))

def fmt_score(v) -> str:
    """Score arrondi à l'entier le plus proche."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return str(round(float(v)))

def html_var(chg) -> str:
    if chg is None or (isinstance(chg, float) and pd.isna(chg)):
        return '<span style="color:#64748b">—</span>'
    c = "#22c55e" if chg >= 0 else "#ef4444"
    a = "▲" if chg >= 0 else "▼"
    return f'<span style="color:{c}">{a}&nbsp;{abs(chg):.2f}%</span>'

def html_bar(ratio, statut) -> str:
    if ratio is None:
        return ""
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
    if not url or (isinstance(url, float) and pd.isna(url)):
        return ""
    u = str(url).strip()
    if not u.startswith("http"):
        return ""
    return (
        f'<a href="{u}" target="_blank" rel="noopener" title="Analyse ChatGPT" '
        'style="color:#7dd3fc;font-size:1.1rem;text-decoration:none">🔗</a>'
    )

# ══════════════════════════════════════════════════════════════════════════════
# Construction des lignes
# ══════════════════════════════════════════════════════════════════════════════

def build_rows(df_sub: pd.DataFrame, prices: dict, names_fb: dict) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t = r.get("yf_ticker")
        q    = prices.get(str(yf_t), {}) if pd.notna(yf_t) else {}

        # Prix : Yahoo d'abord, sinon Spot du sheet
        price = q.get("price")
        chg   = q.get("chg")
        if price is None:
            price = r.get("spot_sheet")

        # Nom : sheet → yfinance → gf_ticker
        name = r.get("name", "")
        if not name and pd.notna(yf_t):
            name = names_fb.get(str(yf_t), "")
        if not name:
            name = ""   # affichera le gf_ticker en italique gris

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        statut = compute_statut(price, buy, fair, trim, exit_)
        ratio  = compute_ratio(price, buy, exit_)
        score  = compute_score(ratio, r.get("note"))

        # Fallback : score calculé par le sheet si on ne peut pas le calculer
        if score is None:
            score = r.get("score_sheet")

        gf = str(r["gf_ticker"])
        name_html = name if name else f'<span style="color:#475569;font-style:italic">{gf}</span>'

        rows.append({
            "_statut_order": STATUT_ORDER.get(statut, 9),
            "_score":        float(score) if score is not None else -1.0,
            "_chg":          chg,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_statut":       statut,
            # Colonnes affichées
            "Ticker":    f'<code style="color:#93c5fd;font-size:.8rem">{gf}</code>',
            "Société":   name_html,
            "Prix":      fmt_price(price),
            "Var %":     html_var(chg),
            "Barre":     html_bar(ratio, statut),
            "Score":     fmt_score(score),      # ← Score en premier
            "Buy":       fmt_price(buy),
            "Fair":      fmt_price(fair),
            "Trim":      fmt_price(trim),
            "Exit":      fmt_price(exit_),
            "Qualité":   fmt_note(r.get("note")),  # ← Qualité après les prix
            "Statut":    html_statut(statut),
            "🔗":        html_link(r.get("url")),
        })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# Tableau HTML
# ══════════════════════════════════════════════════════════════════════════════

DISPLAY_COLS = [
    "Ticker", "Société", "Prix", "Var %", "Barre",
    "Score",                              # ← Score avant les prix
    "Buy", "Fair", "Trim", "Exit",
    "Qualité",                            # ← Qualité après les prix
    "Statut", "🔗",
]

# Colonnes centrées
CENTER = {"Score", "Qualité", "Barre", "🔗", "Statut", "Prix", "Var %",
          "Buy", "Fair", "Trim", "Exit"}

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
            placeholder="Ticker ou société…", label_visibility="collapsed",
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
        with st.expander(f"⚠️ {len(missing)} titre(s) sans cours"):
            st.write(", ".join(missing))

# ══════════════════════════════════════════════════════════════════════════════
# APP PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════════

st.title("Watchlist")   # ← pas d'emoji dans le titre

with st.spinner("Chargement de la liste de titres…"):
    try:
        tickers_df, data_source = load_tickers()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

pf_df    = tickers_df[tickers_df["portif"] == 1].copy()
wl_df    = tickers_df[tickers_df["portif"] != 1].copy()
valid_yf = tuple(str(t) for t in tickers_df["yf_ticker"].dropna() if str(t).strip())

# Métriques
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total", len(tickers_df))
m2.metric("Portefeuille", len(pf_df))
m3.metric("Watchlist", len(wl_df))
m4.metric("Dernière MAJ", st.session_state.get("last_fetch_ts", "—"))

# Bouton Actualiser — vide les 3 caches
rc1, rc2 = st.columns([1, 4])
with rc1:
    if st.button("Actualiser", type="primary", use_container_width=True):
        fetch_prices.clear()
        load_tickers.clear()
        fetch_missing_names.clear()
        st.rerun()
with rc2:
    n_batches = ceil(len(valid_yf) / BATCH_SIZE) if valid_yf else 0
    st.caption(
        f"Source : **{data_source}** · {len(valid_yf)} tickers · "
        f"{n_batches} paquets Yahoo · cache {REFRESH_TTL // 60} min"
    )

# Noms manquants (uniquement pour les tickers sans nom dans le sheet)
no_name_yf = tuple(
    str(r["yf_ticker"])
    for _, r in tickers_df.iterrows()
    if not r["name"] and pd.notna(r.get("yf_ticker"))
)
if no_name_yf:
    with st.spinner(f"Récupération de {len(no_name_yf)} noms manquants…"):
        names_fb = fetch_missing_names(no_name_yf)
else:
    names_fb = {}

# Cours Yahoo
with st.spinner(f"Récupération de {len(valid_yf)} cours…"):
    prices = fetch_prices(valid_yf)

st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

ok = sum(1 for t in valid_yf if prices.get(t, {}).get("price") is not None)
ko = len(valid_yf) - ok
s1, s2, _ = st.columns(3)
s1.metric("Prix Yahoo récupérés", ok)
s2.metric("Fallback Google / manquants", ko)

st.divider()

tab1, tab2 = st.tabs([f"Portefeuille ({len(pf_df)})", f"Watchlist ({len(wl_df)})"])
with tab1:
    render_tab(pf_df, prices, names_fb, key="pf")
with tab2:
    render_tab(wl_df, prices, names_fb, key="wl")

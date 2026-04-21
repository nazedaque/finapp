import streamlit as st
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import pytz

# ─── Configuration ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Ma Watchlist Boursière",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

REFRESH_INTERVAL = 20 * 60  # 20 minutes en secondes

# ─── Conversion Google Finance → yfinance ─────────────────────────────────────
EXCHANGE_MAP = {
    "EPA":    ".PA",   # Euronext Paris
    "ETR":    ".DE",   # XETRA / Allemagne
    "FRA":    ".F",    # Frankfurt FSE
    "LON":    ".L",    # London
    "AMS":    ".AS",   # Amsterdam
    "BIT":    ".MI",   # Milan
    "BME":    ".MC",   # Madrid
    "STO":    ".ST",   # Stockholm
    "SWX":    ".SW",   # Suisse
    "TYO":    ".T",    # Tokyo
    "TSE":    ".TO",   # Toronto
    "HKG":    ".HK",   # Hong Kong
    "SGX":    ".SI",   # Singapour
    "HEL":    ".HE",   # Helsinki
    "VIE":    ".VI",   # Vienne
    "CPH":    ".CO",   # Copenhague
    "EBR":    ".BR",   # Bruxelles
    "WSE":    ".WA",   # Varsovie
    "CVE":    ".V",    # TSX Venture
    "NYSE":   "",      # US
    "NASDAQ": "",      # US
}

# Exceptions manuelles (tickers sans préfixe mais non-US)
MANUAL_OVERRIDES = {
    "JST":    "JST.DE",
    "BETS-B": "BETS-B.ST",
}

def gf_to_yf(gf_ticker: str) -> str:
    """Convertit un ticker Google Finance en ticker yfinance."""
    if gf_ticker in MANUAL_OVERRIDES:
        return MANUAL_OVERRIDES[gf_ticker]
    if ":" not in gf_ticker:
        return gf_ticker  # US ticker
    exchange, code = gf_ticker.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange, "")
    return f"{code}{suffix}"

# ─── Chargement des tickers ───────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_tickers():
    df = pd.read_csv("tickers.csv", dtype={"note": "float64"})
    df["yf_ticker"] = df["gf_ticker"].apply(gf_to_yf)
    return df

# ─── Récupération des cours ───────────────────────────────────────────────────
def fetch_prices(yf_tickers: list) -> dict:
    """Récupère prix et variation % via yfinance en un seul appel batch."""
    results = {}
    try:
        tickers_str = " ".join(yf_tickers)
        data = yf.download(
            tickers_str,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        for t in yf_tickers:
            try:
                if len(yf_tickers) == 1:
                    closes = data["Close"].dropna()
                else:
                    closes = data[t]["Close"].dropna()
                if len(closes) >= 2:
                    price = float(closes.iloc[-1])
                    prev  = float(closes.iloc[-2])
                    chg   = (price - prev) / prev * 100
                    results[t] = {"price": price, "chg": chg}
                elif len(closes) == 1:
                    results[t] = {"price": float(closes.iloc[-1]), "chg": None}
            except Exception:
                results[t] = {"price": None, "chg": None}
    except Exception as e:
        st.warning(f"Erreur yfinance : {e}")
    return results

# ─── Helpers d'affichage ──────────────────────────────────────────────────────
def fmt_price(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.2f}"

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    arrow = "▲" if v >= 0 else "▼"
    return f"{arrow} {abs(v):.2f}%"

def status_label(price, buy, fair, trim, exit_):
    """Retourne un emoji de statut selon la position du prix."""
    try:
        p, b, f, k, e = float(price), float(buy), float(fair), float(trim), float(exit_)
        if p <= b:  return "🟢 Fort achat"
        if p <= f:  return "🟡 Achat"
        if p <= k:  return "🟠 Conserver"
        if p <= e:  return "🔴 Alléger"
        return "⛔ Sortir"
    except Exception:
        return ""

def color_chg(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return "color: #22c55e" if v >= 0 else "color: #ef4444"

def build_table(df_sub: pd.DataFrame, prices: dict) -> pd.DataFrame:
    rows = []
    for _, r in df_sub.iterrows():
        p = prices.get(r["yf_ticker"], {})
        price = p.get("price")
        chg   = p.get("chg")
        rows.append({
            "Ticker":   r["gf_ticker"],
            "Société":  r["name"],
            "Prix":     fmt_price(price),
            "Var %":    fmt_pct(chg),
            "_chg":     chg,
            "Note":     int(r["note"]) if pd.notna(r.get("note")) else "",
            "Buy":      fmt_price(r.get("buy")),
            "Fair":     fmt_price(r.get("fair")),
            "Trim":     fmt_price(r.get("trim")),
            "Exit":     fmt_price(r.get("exit")),
            "Statut":   status_label(price, r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit"))
                        if price else "",
        })
    return pd.DataFrame(rows)

# ─── CSS personnalisé ─────────────────────────────────────────────────────────
st.markdown("""
<style>
  [data-testid="stAppViewContainer"] { background: #0f1117; }
  .main-title { font-size: 1.6rem; font-weight: 700; color: #f1f5f9; margin-bottom: 0; }
  .sub-title  { font-size: 0.85rem; color: #94a3b8; margin-bottom: 1.2rem; }
  .metric-box { background: #1e2130; border-radius: 10px; padding: 12px 18px; text-align: center; }
  .metric-label { font-size: 0.75rem; color: #64748b; text-transform: uppercase; letter-spacing: .05em; }
  .metric-val   { font-size: 1.4rem; font-weight: 700; color: #f1f5f9; }
  div[data-testid="stDataFrame"] table { font-size: 0.82rem; }
  div[data-testid="stTab"] button { font-size: 0.9rem; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ─── Auto-refresh ─────────────────────────────────────────────────────────────
if "last_fetch" not in st.session_state:
    st.session_state.last_fetch = 0
    st.session_state.prices = {}

now = time.time()
needs_refresh = (now - st.session_state.last_fetch) >= REFRESH_INTERVAL

# ─── En-tête ──────────────────────────────────────────────────────────────────
st.markdown('<p class="main-title">📈 Ma Watchlist Boursière</p>', unsafe_allow_html=True)

tickers_df = load_tickers()
pf_df = tickers_df[tickers_df["portif"] == 1].copy()
wl_df = tickers_df[tickers_df["portif"] == 0].copy()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""<div class="metric-box">
        <div class="metric-label">Titres total</div>
        <div class="metric-val">{len(tickers_df)}</div></div>""", unsafe_allow_html=True)
with col2:
    st.markdown(f"""<div class="metric-box">
        <div class="metric-label">Portefeuille</div>
        <div class="metric-val">{len(pf_df)}</div></div>""", unsafe_allow_html=True)
with col3:
    st.markdown(f"""<div class="metric-box">
        <div class="metric-label">Watchlist</div>
        <div class="metric-val">{len(wl_df)}</div></div>""", unsafe_allow_html=True)
with col4:
    last_str = datetime.fromtimestamp(st.session_state.last_fetch).strftime("%H:%M:%S") \
               if st.session_state.last_fetch else "—"
    st.markdown(f"""<div class="metric-box">
        <div class="metric-label">Dernière MAJ</div>
        <div class="metric-val" style="font-size:1rem">{last_str}</div></div>""",
        unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─── Bouton de refresh manuel ─────────────────────────────────────────────────
col_btn, col_msg = st.columns([1, 5])
with col_btn:
    manual_refresh = st.button("🔄 Actualiser maintenant")
with col_msg:
    if st.session_state.last_fetch:
        elapsed = int(now - st.session_state.last_fetch)
        remaining = max(0, REFRESH_INTERVAL - elapsed)
        st.caption(f"Prochain rafraîchissement auto dans {remaining//60}m {remaining%60:02d}s")

# ─── Récupération des cours ───────────────────────────────────────────────────
if needs_refresh or manual_refresh or not st.session_state.prices:
    all_yf = tickers_df["yf_ticker"].tolist()
    with st.spinner(f"Récupération des cours ({len(all_yf)} titres)…"):
        st.session_state.prices = fetch_prices(all_yf)
        st.session_state.last_fetch = time.time()
    st.rerun()

prices = st.session_state.prices

# ─── Onglets ──────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs([f"💼 Portefeuille ({len(pf_df)})", f"👁 Watchlist ({len(wl_df)})"])

def display_tab(df_sub):
    table = build_table(df_sub, prices)
    
    # Séparation hausse / baisse
    up   = table[table["_chg"].apply(lambda x: isinstance(x, float) and x >= 0)]
    down = table[table["_chg"].apply(lambda x: isinstance(x, float) and x < 0)]
    na   = table[table["_chg"].apply(lambda x: x is None or not isinstance(x, float))]
    
    display_cols = ["Ticker", "Société", "Prix", "Var %", "Note", "Buy", "Fair", "Trim", "Exit", "Statut"]
    
    # Top hausses / baisses du jour
    if not up.empty or not down.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**🟢 Top hausses du jour**")
            top_up = up.nlargest(5, "_chg")[display_cols]
            st.dataframe(top_up, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**🔴 Top baisses du jour**")
            top_dn = down.nsmallest(5, "_chg")[display_cols]
            st.dataframe(top_dn, use_container_width=True, hide_index=True)
        st.markdown("---")
    
    # Filtre de recherche
    search = st.text_input("🔍 Rechercher un titre…", key=f"search_{id(df_sub)}")
    if search:
        mask = (
            table["Ticker"].str.contains(search, case=False) |
            table["Société"].str.contains(search, case=False)
        )
        table = table[mask]
    
    # Tri par statut / note
    sort_col = st.selectbox("Trier par", ["Ticker", "Note ↓", "Var % ↓", "Var % ↑", "Statut"],
                             key=f"sort_{id(df_sub)}")
    if sort_col == "Note ↓":
        table = table.sort_values("Note", ascending=False)
    elif sort_col == "Var % ↓":
        table = table.sort_values("_chg", ascending=True, na_position="last")
    elif sort_col == "Var % ↑":
        table = table.sort_values("_chg", ascending=False, na_position="last")
    
    st.dataframe(
        table[display_cols],
        use_container_width=True,
        hide_index=True,
        height=600,
    )
    
    # Titres non récupérés
    not_found = [r["gf_ticker"] for _, r in df_sub.iterrows()
                 if prices.get(r["yf_ticker"], {}).get("price") is None]
    if not_found:
        with st.expander(f"⚠️ {len(not_found)} ticker(s) non récupéré(s)"):
            st.write(", ".join(not_found))

with tab1:
    display_tab(pf_df)

with tab2:
    display_tab(wl_df)

# ─── Auto-refresh via rerun ───────────────────────────────────────────────────
time.sleep(1)
st.rerun()

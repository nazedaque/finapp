from __future__ import annotations

from datetime import datetime, timezone
from math import ceil
from typing import Iterable

import pandas as pd
import streamlit as st
import yfinance as yf

st.set_page_config(
    page_title="Ma Watchlist Boursière",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

REFRESH_TTL_SECONDS = 30 * 60
DOWNLOAD_PERIOD = "5d"
DOWNLOAD_INTERVAL = "30m"
BATCH_SIZE = 75

EXCHANGE_MAP = {
    "EPA": ".PA",
    "ETR": ".DE",
    "FRA": ".F",
    "LON": ".L",
    "AMS": ".AS",
    "BIT": ".MI",
    "BME": ".MC",
    "STO": ".ST",
    "SWX": ".SW",
    "TYO": ".T",
    "TSE": ".TO",
    "HKG": ".HK",
    "SGX": ".SI",
    "HEL": ".HE",
    "VIE": ".VI",
    "CPH": ".CO",
    "EBR": ".BR",
    "WSE": ".WA",
    "CVE": ".V",
    "NYSE": "",
    "NASDAQ": "",
}

MANUAL_OVERRIDES = {
    "JST": "JST.DE",
    "BETS-B": "BETS-B.ST",
}

REQUIRED_COLUMNS = ["gf_ticker", "portif", "name"]
OPTIONAL_NUMERIC_COLUMNS = ["note", "buy", "fair", "trim", "exit"]
OPTIONAL_COLUMNS = OPTIONAL_NUMERIC_COLUMNS


class TickerMappingError(ValueError):
    pass



def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]



def gf_to_yf(gf_ticker: str) -> str:
    raw = str(gf_ticker).strip()
    if not raw:
        raise TickerMappingError("Ticker vide")

    if raw in MANUAL_OVERRIDES:
        return MANUAL_OVERRIDES[raw]

    if ":" not in raw:
        return raw

    exchange, symbol = raw.split(":", 1)
    suffix = EXCHANGE_MAP.get(exchange)
    if suffix is None:
        raise TickerMappingError(f"Place non gérée: {exchange}")
    if not symbol:
        raise TickerMappingError("Code ticker vide")
    return f"{symbol}{suffix}"


@st.cache_data(ttl=3600, show_spinner=False)
def load_tickers(path: str = "tickers.csv") -> tuple[pd.DataFrame, list[str]]:
    df = pd.read_csv(path)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans tickers.csv: {', '.join(missing)}")

    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    mapping_errors: list[str] = []
    yf_tickers: list[str | None] = []

    for value in df["gf_ticker"].astype(str):
        try:
            yf_tickers.append(gf_to_yf(value))
        except TickerMappingError as exc:
            yf_tickers.append(None)
            mapping_errors.append(f"{value}: {exc}")

    df = df.copy()
    df["yf_ticker"] = yf_tickers
    df["portif"] = pd.to_numeric(df["portif"], errors="coerce").fillna(0).astype(int)

    for col in OPTIONAL_NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, mapping_errors



def _extract_close_series(data: pd.DataFrame, ticker: str, multi: bool) -> pd.Series:
    if multi:
        if ticker not in data.columns.get_level_values(0):
            return pd.Series(dtype="float64")
        series = data[ticker]["Close"]
    else:
        if "Close" not in data.columns:
            return pd.Series(dtype="float64")
        series = data["Close"]

    series = series.dropna()
    if isinstance(series, pd.DataFrame):
        return pd.Series(dtype="float64")
    return series.astype("float64")



def _last_price_and_previous_close(closes: pd.Series) -> tuple[float | None, float | None]:
    if closes.empty:
        return None, None

    last_price = float(closes.iloc[-1])
    dates = pd.to_datetime(closes.index).tz_localize(None).normalize()
    last_date = dates[-1]
    previous_session = closes[dates < last_date]
    previous_close = float(previous_session.iloc[-1]) if not previous_session.empty else None
    return last_price, previous_close


@st.cache_data(ttl=REFRESH_TTL_SECONDS, show_spinner=False)
def fetch_prices(yf_tickers: list[str]) -> tuple[dict[str, dict[str, float | None]], list[str], str]:
    results: dict[str, dict[str, float | None]] = {}
    failures: list[str] = []

    tickers = [ticker for ticker in yf_tickers if ticker]
    if not tickers:
        fetched_at = datetime.now(timezone.utc).isoformat()
        return results, failures, fetched_at

    for batch in chunked(tickers, BATCH_SIZE):
        batch_str = " ".join(batch)
        try:
            data = yf.download(
                tickers=batch_str,
                period=DOWNLOAD_PERIOD,
                interval=DOWNLOAD_INTERVAL,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=True,
                prepost=False,
            )
        except Exception as exc:
            failures.extend([f"{ticker}: {exc}" for ticker in batch])
            continue

        if data.empty:
            failures.extend([f"{ticker}: aucune donnée renvoyée" for ticker in batch])
            continue

        multi = len(batch) > 1
        for ticker in batch:
            try:
                closes = _extract_close_series(data, ticker, multi)
                price, prev_close = _last_price_and_previous_close(closes)
                if price is None:
                    failures.append(f"{ticker}: aucune clôture exploitable")
                    results[ticker] = {"price": None, "chg": None}
                    continue

                chg = None
                if prev_close not in (None, 0):
                    chg = (price - prev_close) / prev_close * 100

                results[ticker] = {"price": price, "chg": chg}
            except Exception as exc:
                failures.append(f"{ticker}: {exc}")
                results[ticker] = {"price": None, "chg": None}

    fetched_at = datetime.now(timezone.utc).isoformat()
    return results, failures, fetched_at



def fmt_price(value: float | None) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{value:,.2f}"



def fmt_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{value:+.2f}%"



def status_label(price: float | None, buy: float | None, fair: float | None, trim: float | None, exit_: float | None) -> str:
    if price is None or pd.isna(price):
        return ""
    if any(pd.isna(v) for v in [buy, fair, trim, exit_]):
        return ""

    p = float(price)
    if p <= float(buy):
        return "Strong buy"
    if p <= float(fair):
        return "Buy"
    if p <= float(trim):
        return "Fair"
    if p <= float(exit_):
        return "Trim"
    return "Exit"



def make_table(df_sub: pd.DataFrame, prices: dict[str, dict[str, float | None]]) -> pd.DataFrame:
    rows = []
    for _, row in df_sub.iterrows():
        quote = prices.get(row["yf_ticker"], {}) if pd.notna(row["yf_ticker"]) else {}
        price = quote.get("price")
        chg = quote.get("chg")
        rows.append(
            {
                "Ticker": row["gf_ticker"],
                "Société": row["name"],
                "Prix": fmt_price(price),
                "Var %": fmt_pct(chg),
                "_chg": chg,
                "Note": int(row["note"]) if pd.notna(row["note"]) else "",
                "Buy": fmt_price(row["buy"]),
                "Fair": fmt_price(row["fair"]),
                "Trim": fmt_price(row["trim"]),
                "Exit": fmt_price(row["exit"]),
                "Statut": status_label(price, row["buy"], row["fair"], row["trim"], row["exit"]),
            }
        )
    return pd.DataFrame(rows)



def render_table(df_sub: pd.DataFrame, prices: dict[str, dict[str, float | None]], search_key: str, sort_key: str) -> None:
    table = make_table(df_sub, prices)
    if table.empty:
        st.info("Aucun titre.")
        return

    left, right = st.columns([2, 1])
    with left:
        search = st.text_input("Recherche", key=search_key, placeholder="Ticker ou société")
    with right:
        sort_choice = st.selectbox(
            "Tri",
            ["Ticker", "Note ↓", "Var % ↑", "Var % ↓", "Statut"],
            key=sort_key,
        )

    if search:
        mask = (
            table["Ticker"].astype(str).str.contains(search, case=False, na=False)
            | table["Société"].astype(str).str.contains(search, case=False, na=False)
        )
        table = table[mask]

    if sort_choice == "Note ↓":
        table = table.sort_values(by="Note", ascending=False, na_position="last")
    elif sort_choice == "Var % ↑":
        table = table.sort_values(by="_chg", ascending=False, na_position="last")
    elif sort_choice == "Var % ↓":
        table = table.sort_values(by="_chg", ascending=True, na_position="last")
    elif sort_choice == "Statut":
        table = table.sort_values(by=["Statut", "Ticker"], ascending=[True, True], na_position="last")
    else:
        table = table.sort_values(by="Ticker", ascending=True)

    display_cols = ["Ticker", "Société", "Prix", "Var %", "Note", "Buy", "Fair", "Trim", "Exit", "Statut"]
    st.dataframe(table[display_cols], use_container_width=True, hide_index=True, height=650)



def render_summary(total_count: int, portfolio_count: int, watchlist_count: int, fetched_at: str | None) -> None:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Titres", total_count)
    with col2:
        st.metric("Portefeuille", portfolio_count)
    with col3:
        st.metric("Watchlist", watchlist_count)
    with col4:
        if fetched_at:
            ts = pd.Timestamp(fetched_at).tz_convert("Europe/Luxembourg")
            label = ts.strftime("%Y-%m-%d %H:%M")
        else:
            label = "inconnu"
        st.metric("Dernière MAJ", label)



def render_failures(mapping_errors: list[str], fetch_failures: list[str], tickers_df: pd.DataFrame, prices: dict[str, dict[str, float | None]]) -> None:
    unresolved = []
    for _, row in tickers_df.iterrows():
        yf_ticker = row["yf_ticker"]
        if pd.isna(yf_ticker):
            continue
        if prices.get(yf_ticker, {}).get("price") is None:
            unresolved.append(f"{row['gf_ticker']} -> {yf_ticker}")

    total_issues = len(mapping_errors) + len(fetch_failures) + len(unresolved)
    if total_issues == 0:
        return

    with st.expander(f"Problèmes détectés ({total_issues})"):
        if mapping_errors:
            st.write("Mapping ticker non résolu :")
            st.code("\n".join(mapping_errors))
        if fetch_failures:
            st.write("Echecs de récupération :")
            st.code("\n".join(fetch_failures[:200]))
        if unresolved:
            st.write("Tickers sans prix exploitable :")
            st.code("\n".join(unresolved[:200]))


st.title("Ma Watchlist Boursière")
st.caption(
    "Source : Yahoo Finance via yfinance. Données gratuites, non officielles, suffisantes pour une watchlist perso, pas pour un usage critique."
)

try:
    tickers_df, mapping_errors = load_tickers()
except Exception as exc:
    st.error(str(exc))
    st.stop()

valid_tickers = [ticker for ticker in tickers_df["yf_ticker"].dropna().astype(str).tolist() if ticker]
portfolio_df = tickers_df[tickers_df["portif"] == 1].copy()
watchlist_df = tickers_df[tickers_df["portif"] != 1].copy()

render_summary(
    total_count=len(tickers_df),
    portfolio_count=len(portfolio_df),
    watchlist_count=len(watchlist_df),
    fetched_at=st.session_state.get("last_fetch_iso"),
)

left, right = st.columns([1, 3])
with left:
    refresh_clicked = st.button("Actualiser maintenant", type="primary", use_container_width=True)
with right:
    st.caption(
        f"Cache prix : {REFRESH_TTL_SECONDS // 60} min. Batch size : {BATCH_SIZE}. Intervalle Yahoo : {DOWNLOAD_INTERVAL}."
    )

if refresh_clicked:
    fetch_prices.clear()

with st.spinner(f"Récupération des cours pour {len(valid_tickers)} titres..."):
    prices, fetch_failures, fetched_at = fetch_prices(valid_tickers)

st.session_state["last_fetch_iso"] = fetched_at

resolved_count = sum(1 for ticker in valid_tickers if prices.get(ticker, {}).get("price") is not None)
unresolved_count = len(valid_tickers) - resolved_count
batch_count = ceil(len(valid_tickers) / BATCH_SIZE) if valid_tickers else 0

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Prix récupérés", resolved_count)
with col2:
    st.metric("Prix manquants", unresolved_count)
with col3:
    st.metric("Paquets Yahoo", batch_count)

portfolio_tab, watchlist_tab = st.tabs(
    [f"Portefeuille ({len(portfolio_df)})", f"Watchlist ({len(watchlist_df)})"]
)

with portfolio_tab:
    render_table(portfolio_df, prices, search_key="search_pf", sort_key="sort_pf")

with watchlist_tab:
    render_table(watchlist_df, prices, search_key="search_wl", sort_key="sort_wl")

render_failures(mapping_errors, fetch_failures, tickers_df, prices)

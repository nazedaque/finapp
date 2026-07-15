from __future__ import annotations

import json
import html
import hmac
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
import streamlit.components.v1 as components
import yfinance as yf

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Finapp SOL", page_icon=None, layout="wide",
                   initial_sidebar_state="collapsed")

APP_TITLE         = "Finapp SOL"
SHEET_ID          = "1P6f-aDWgS6a9qstyazQlITGv6NBraU9yG3uN1Fu8R1o"
SHEET_NAME        = "Registre"
REFRESH_TTL       = 15 * 60
BATCH_SIZE        = 50
YF_META_BATCH_SIZE = 10
YF_BATCH_PAUSE_SEC = 0.2
HTTP_RETRIES      = 3


def _secret(path: tuple[str, ...], default=None):
    try:
        value = st.secrets
        for key in path:
            value = value[key]
        return value
    except (FileNotFoundError, KeyError, TypeError):
        return default


def access_guard() -> None:
    """Bloque tout chargement du Sheet avant validation du code privé."""
    expected = str(_secret(("app", "access_code"), "")).strip()
    invalid = not expected or expected.lower().startswith("replace") or len(expected) < 4

    if invalid:
        st.title(APP_TITLE)
        st.info("L'application privée n'est pas encore configurée.")
        st.caption(
            "Ajoutez un code d'accès d'au moins 4 caractères et la connexion Google "
            "dans les secrets Streamlit. Aucune donnée n'est chargée dans cet état."
        )
        st.stop()

    if st.session_state.get("access_granted"):
        return

    st.title(APP_TITLE)
    st.caption("Accès privé — le Sheet n'est chargé qu'après authentification.")
    with st.form("access_form", clear_on_submit=True):
        candidate = st.text_input(
            "Code d'accès", type="password", autocomplete="current-password"
        )
        submitted = st.form_submit_button(
            "Ouvrir finapp", type="primary", use_container_width=True
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
    "Score", "Buy", "Fair", "Trim", "Exit", "Commentaires",
]
COL_WIDTHS = {
    "MAJ": "46px", "Audit": "42px", "JRS": "38px", "Pays": "36px",
    "Ticker": "59px", "Société": "145px", "Qual": "44px",
    "Prix": "45px", "Var %": "55px", "Upside": "51px",
    "Score": "84px",
    "Buy": "51px", "Fair": "51px", "Trim": "51px", "Exit": "51px", "Commentaires": "177px",
}
CENTER = {"MAJ", "Audit", "JRS", "Pays", "Prix", "Var %", "Upside", "Score",
          "Buy", "Fair", "Trim", "Exit", "Qual"}
GROUP_STARTS = {"Prix", "Score", "Buy", "Commentaires"}
HEADER_CENTER = CENTER | {"Commentaires"}
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
    "Commentaires": "text",
}

# ══════════════════════════════════════════════════════════════════════════════
# Utilitaires
# ══════════════════════════════════════════════════════════════════════════════

def parse_num(v) -> float | None:
    if v is None: return None
    s = (str(v).strip().replace("\u202f", "").replace("\xa0", "")
         .replace(" ", "").replace("%", ""))
    if not s or s in ("#REF!", "#N/A", "#VALUE!", "#ERROR!", "—", ""): return None
    if re.match(r"^\d{1,3}(,\d{3})+$", s): return float(s.replace(",", ""))
    if re.match(r"^\d{1,3}(,\d{3})+,\d{1,2}$", s):
        parts = s.split(","); return float("".join(parts[:-1]) + "." + parts[-1])
    if "," in s: return float(s.replace(".", "").replace(",", "."))
    if re.match(r"^\d{1,3}(\.\d{3})+$", s): return float(s.replace(".", ""))
    try: return float(s)
    except ValueError: return None


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
    "action suivante": "next_action",
    "last update": "last_update",
    "yf ticker":   "yf_ticker",
    "yf_ticker":   "yf_ticker",
}
NUMERIC_COLS = [
    "note", "buy", "fair", "trim", "exit", "spot_sheet", "score_sheet",
    "upside_fair_sheet", "upside_trim_sheet",
]


def _normalize_col(s: str) -> str:
    """Normalisation agressive : supprime BOM, accents, espaces, casse."""
    s = str(s).replace("\ufeff", "").replace("\u202f", "").replace("\xa0", "")
    nfkd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()


def _read_private_sheet(ttl: str | int = "5m") -> pd.DataFrame:
    """Lit l'onglet Registre via la connexion Google privée de Streamlit."""
    from streamlit_gsheets import GSheetsConnection

    connection = st.connection("gsheets", type=GSheetsConnection)
    return connection.read(worksheet=SHEET_NAME, ttl=ttl)


def load_tickers(force_refresh: bool = False) -> tuple[pd.DataFrame, str]:
    """Charge et normalise SOL input sans rendre le Sheet public."""
    try:
        df = _read_private_sheet(ttl=0 if force_refresh else "5m")
    except Exception as exc:
        raise RuntimeError(
            "Impossible de lire SOL input / Registre avec la connexion Google privée. "
            "Vérifiez les secrets Streamlit et le partage en lecture avec le compte de service."
        ) from exc

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
    df["verif_display"] = df["verif"].apply(fmt_verif)
    df["flagged"] = False

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
        lambda v: 1
        if parse_num(v) == 1 or str(v).strip().upper() in ("OUI", "TRUE", "VRAI")
        else 0
    )
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
    if "purchase_date" in df.columns:
        df["purchase_date"] = pd.to_datetime(
            df["purchase_date"], dayfirst=True, errors="coerce").dt.date

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
    df["gf_ticker"] = df["gf_ticker"].astype(str).str.strip()
    df["yf_ticker"] = df["yf_ticker"].astype(str).str.strip().str.upper()

    # Détection des doublons
    dupes = df[df["gf_ticker"].duplicated(keep=False)][["gf_ticker", "yf_ticker"]].copy()
    st.session_state["ticker_dupes"] = dupes.to_dict("records") if not dupes.empty else []

    return df.reset_index(drop=True), "SOL input / Registre (privé)"

# ══════════════════════════════════════════════════════════════════════════════
# Métadonnées (noms) — parallèle, cache 7j
# ══════════════════════════════════════════════════════════════════════════════

def iter_completed(futures: dict, timeout: int = 60):
    """Renvoie les futures terminées sans faire échouer tout le batch en cas de timeout."""
    try:
        yield from as_completed(futures, timeout=timeout)
    except TimeoutError:
        return

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

def fetch_name_cached(ticker: str) -> str:
    return _fetch_one_name(ticker)[1]

def fetch_names(yf_tickers: tuple[str, ...]) -> dict[str, str]:
    names: dict[str, str] = {}
    tickers = list(yf_tickers)
    # Requetes unitaires Yahoo : petits batches + courte pause pour limiter la pression.
    for i in range(0, len(tickers), YF_META_BATCH_SIZE):
        batch = tickers[i: i + YF_META_BATCH_SIZE]
        executor = ThreadPoolExecutor(max_workers=8)
        try:
            futures = {executor.submit(fetch_name_cached, t): t for t in batch}
            for future in iter_completed(futures):
                try:
                    t = futures[future]
                    names[t] = future.result(timeout=15)
                except Exception:
                    names[futures[future]] = ""
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        if i + YF_META_BATCH_SIZE < len(tickers):
            time.sleep(YF_BATCH_PAUSE_SEC)
    return names

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
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None

def _fetch_chart_quote(ticker: str) -> tuple[str, dict]:
    symbol = str(ticker or "").strip().upper()
    empty = {"price": None, "chg": None, "name": "", "error": ""}
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
            return symbol, {"price": price, "chg": chg, "name": name, "error": ""}
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
    value = float(v)
    return f"{value:,.0f}" if value > 1_000 else f"{value:,.2f}"


def fmt_target(v, hide_decimals: bool = False) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    value = float(v)
    return f"{value:,.0f}" if hide_decimals else f"{value:,.2f}"

def fmt_note(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    return str(int(float(v)))

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


def html_audit(v, underwritten: bool) -> tuple[str, int]:
    value = fmt_verif(v)
    normalized = _normalize_col(value)
    audited = normalized in {
        "pass", "pass avec reserves", "correction materielle", "corrige apres audit",
    }
    if audited:
        color, label, rank = "#22c55e", "Underwrité et audité", 2
        if value:
            label += f" — {value}"
    elif underwritten:
        color, label, rank = "#facc15", "Underwrité mais non audité", 1
    else:
        color, label, rank = "#f97316", "Screené uniquement", 0
    light = (
        f'<span class="audit-light" title="{html.escape(label, quote=True)}" '
        f'role="img" aria-label="{html.escape(label, quote=True)}" '
        f'style="color:{color};background:{color}"></span>'
    )
    return light, rank


def html_score_bar(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        score = max(0.0, min(100.0, float(v)))
    except Exception:
        return "—"
    hidden = 100.0 - score
    return (
        '<div class="score-bar" title="Score global du Sheet : {:.0f}/100" '
        'role="img" aria-label="Score global {:.0f} sur 100">'
        '<div class="score-bar-fill" style="clip-path:inset(0 {:.2f}% 0 0)"></div>'
        '<span>{:.0f}</span>'
        '</div>'
    ).format(score, score, hidden, score)

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
            f'style="color:#93c5fd;font-family:"JetBrains Mono",monospace;'
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
COUNTRY_CODES = {
    ".AS": "NL", ".BR": "BE", ".DE": "DE", ".HK": "HK",
    ".KQ": "KR", ".KS": "KR", ".L": "GB", ".MC": "ES",
    ".OL": "NO", ".PA": "FR", ".SI": "SG", ".ST": "SE", ".T": "JP",
    ".TO": "CA", ".WA": "PL", ".AT": "GR", ".CO": "DK",
    ".MI": "IT", ".SW": "CH",
}
COUNTRY_SUFFIXES = tuple(sorted(COUNTRY_CODES.items(), key=lambda item: len(item[0]), reverse=True))


def country_code(ticker: str) -> str:
    t = str(ticker or "").upper().strip()
    for suffix, code in COUNTRY_SUFFIXES:
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
               names: dict,
               holding_required: bool = False) -> list[dict]:
    rows = []
    for _, r in df_sub.iterrows():
        yf_t = r.get("yf_ticker")
        yf_s = str(yf_t).strip().upper() if pd.notna(yf_t) else ""
        q = prices.get(yf_s, {})

        price = q.get("price")
        if price is None and pd.notna(r.get("spot_sheet")):
            price = r.get("spot_sheet")
        chg = q.get("chg")
        name = (r.get("name") or "") if pd.notna(r.get("name")) else ""
        name = str(name or names.get(yf_s, ""))
        name_u = name.upper() if name else ""

        buy, fair, trim, exit_ = r.get("buy"), r.get("fair"), r.get("trim"), r.get("exit")
        target_values = tuple(safe_float(value) for value in (buy, fair, trim, exit_))
        hide_target_decimals = any(
            value is not None and value > 1_000 for value in target_values
        )
        score = safe_float(r.get("score_sheet"))
        upside = compute_upside(price, fair, trim)
        quality = safe_float(r.get("note"))
        prompt_version = (
            "" if pd.isna(r.get("prompt_version")) else str(r.get("prompt_version")).strip()
        )
        underwritten = bool(prompt_version) or sum(value is not None for value in target_values) >= 3
        audit_html, audit_rank = html_audit(r.get("verif"), underwritten)
        days = holding_days(r.get("purchase_date"))
        comments = "" if pd.isna(r.get("comments")) else str(r.get("comments"))

        gf = str(r["gf_ticker"])
        name_html = name_u if name_u else gf
        flagged = bool(r.get("flagged", False))

        rows.append({
            "_score":        score,
            "_chg":          chg,
            "_maj":          r.get("last_update"),
            "_upside":       upside,
            "_quality":      quality,
            "_price_ok":     price is not None,
            "_ticker":       gf,
            "_name":         name,
            "_flagged":      flagged,
            "_sort": {
                "MAJ": r.get("last_update").toordinal() if isinstance(r.get("last_update"), date) else None,
                "Audit": audit_rank,
                "JRS": days,
                "Pays": country_code(yf_s),
                "Ticker": gf,
                "Société": name_u,
                "Qual": quality,
                "Prix": price,
                "Upside": upside,
                "Var %": chg,
                "Score": score,
                "Commentaires": comments,
            },
            "MAJ":      fmt_maj(r.get("last_update")),
            "Audit":    audit_html,
            "JRS":      fmt_holding_days(r.get("purchase_date"), holding_required),
            "Pays":     html_country_flag(yf_s),
            "Ticker":   html_ticker_link(yf_s, gf),
            "Société":  f'<span title="{html.escape(name_u, quote=True)}">{html.escape(name_html)}</span>',
            "Qual":     fmt_note(r.get("note")),
            "Prix":     fmt_price(price),
            "Var %":    html_var(chg),
            "Upside":   html_upside(upside),
            "Score":    html_score_bar(score),
            "Buy":      fmt_target(buy, hide_target_decimals),
            "Fair":     fmt_target(fair, hide_target_decimals),
            "Trim":     fmt_target(trim, hide_target_decimals),
            "Exit":     fmt_target(exit_, hide_target_decimals),
            "Commentaires": html.escape(comments),
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
  padding: 6px 8px;
  border-bottom: 1px solid #1a2030;
  vertical-align: middle;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}
.wl-table td.c { text-align: center; }
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
.score-bar {
  position: relative;
  height: 18px;
  width: 100%;
  margin: 0 auto;
  background: #252d3d;
  display: block;
  border: 1px solid rgba(148,163,184,.22);
  border-radius: 999px;
  overflow: hidden;
}
.score-bar-fill {
  position: absolute;
  inset: 0;
  background: linear-gradient(90deg, #f4cccc 0%, #ffefb2 50%, #c6e5c6 100%);
  border-radius: inherit;
}
.score-bar span {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #f8fafc;
  font-size: .68rem;
  font-weight: 800;
  line-height: 1;
  text-shadow: 0 1px 2px rgba(0,0,0,.95);
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
        st.info("Aucun titre.")
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
            f' aria-sort="{initial_sort}" tabindex="0" role="button"'
            if sortable else ""
        )
        title = f"{label} — cliquer pour trier" if sortable else label

        th_parts.append(
            f'<th class="{classes}" title="{title}"{sort_attrs}>{label}</th>'
        )

    trs = []
    for row in rows:
        row_class = "wl-flagged" if row["_flagged"] else ""
        td_parts = []
        for column in cols:
            classes = " ".join(filter(None, (
                "c" if column in CENTER else "",
                "group-start" if column in GROUP_STARTS else "",
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
      direction = activeColumn === column ? -direction : 1;
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
    components.html(script, height=0)

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

if cached_tickers_df is not None and not force_sheet_refresh:
    tickers_df = cached_tickers_df.copy(deep=True)
    data_source = st.session_state.get("data_source", "Google Sheet")
else:
    with st.spinner("Chargement du Google Sheet…"):
        try:
            tickers_df, data_source = load_tickers(force_refresh=force_sheet_refresh)
            st.session_state["tickers_df"] = tickers_df.copy(deep=True)
            st.session_state["data_source"] = data_source
        except Exception as exc:
            if cached_tickers_df is None:
                st.error(str(exc))
                st.stop()
            st.warning(f"Google Sheet indisponible : données précédentes conservées ({exc}).")
            tickers_df = cached_tickers_df.copy(deep=True)
            data_source = st.session_state.get("data_source", "Cache de session")
if tickers_df.empty:
    st.error("L'onglet Registre ne contient aucun titre exploitable.")
    st.stop()

ASIA_SUFFIXES = (".T", ".KQ", ".KS", ".SI", ".HK")

def is_asia_ticker(ticker: str) -> bool:
    return str(ticker or "").upper().strip().endswith(ASIA_SUFFIXES)

pf_df = tickers_df[tickers_df["portif"] == 1].copy()
watchlist_all_df = tickers_df[tickers_df["portif"] != 1].copy()
asia_mask = watchlist_all_df["yf_ticker"].apply(is_asia_ticker)
asia_df = watchlist_all_df[asia_mask].copy()
wl_df = watchlist_all_df[~asia_mask].copy()

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

def mark_refresh() -> None:
    """Actualise le Sheet et tous les cours depuis un point unique."""
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
render_topbar(len(pf_df), len(watchlist_all_df), last_ts)

def tickers_for(df: pd.DataFrame) -> tuple[str, ...]:
    normalized = (str(t).strip().upper() for t in df["yf_ticker"].dropna())
    return tuple(dict.fromkeys(t for t in normalized if t))

def table_cols_with_holding_days() -> list[str]:
    """Colonnes synthétiques adaptées au Registre SOL."""
    return DISPLAY_COLS

pf_yf = tickers_for(pf_df)
wl_yf = tickers_for(wl_df)
asia_yf = tickers_for(asia_df)
all_yf = tuple(dict.fromkeys((*pf_yf, *wl_yf, *asia_yf)))

last_action = st.session_state.pop("last_action", "")
active_yf = all_yf

# ── 2. Cours et noms Yahoo ────────────────────────────────────────────────────
data_key = all_yf
same_data_key = st.session_state.get("data_key") == data_key
cached_prices = dict(st.session_state.get("prices_data", {}))
fresh_prices: dict[str, dict] = {}

if not all_yf:
    prices = cached_prices
elif same_data_key and last_action != "refresh" and cached_prices:
    prices = cached_prices
else:
    price_scope = active_yf if last_action == "refresh" else all_yf
    prices_spinner = "Actualisation des cours en temps réel…" if last_action == "refresh" else "Cours en temps réel…"
    refresh_nonce = st.session_state.get("refresh_nonce", 0) if last_action == "refresh" else 0
    with st.spinner(prices_spinner):
        fresh_prices = fetch_prices(price_scope, refresh_nonce)
    prices = cached_prices
    prices.update(fresh_prices)
    prices = {ticker: prices[ticker] for ticker in all_yf if ticker in prices}
    st.session_state["prices_data"] = prices
    st.session_state["last_fetch_ts"] = datetime.now(timezone.utc).strftime("%H:%M UTC")

# Le endpoint chart fournit généralement le nom avec le prix. yfinance.info
# n'est utilisé que pour les nouveaux tickers dont le nom reste manquant.
names = dict(st.session_state.get("names_data", {}))
for ticker, quote in fresh_prices.items():
    if not names.get(ticker) and quote.get("name"):
        names[ticker] = quote["name"]

sheet_named_tickers = {
    str(row["yf_ticker"]).strip().upper()
    for _, row in tickers_df.iterrows()
    if pd.notna(row.get("name")) and str(row.get("name")).strip()
}
name_scope = active_yf if last_action == "refresh" else all_yf
should_resolve_names = last_action == "refresh" or not same_data_key
missing_name_tickers = tuple(
    ticker for ticker in name_scope
    if ticker not in sheet_named_tickers and not names.get(ticker)
)
if should_resolve_names and missing_name_tickers:
    with st.spinner("Noms des nouveaux tickers…"):
        names.update(fetch_names(missing_name_tickers))

names = {ticker: names[ticker] for ticker in all_yf if ticker in names}
st.session_state["names_data"] = names
st.session_state["data_key"] = data_key

last_ts = st.session_state.get("last_fetch_ts", "—")

ok = sum(1 for t in all_yf if prices.get(t, {}).get("price") is not None)

# Mise à jour du topbar avec les prix récupérés
render_topbar(len(pf_df), len(watchlist_all_df), last_ts, ok=ok, total=len(all_yf))

# Construire les rows des vues une seule fois
rows_pf = build_rows(pf_df, prices, names, True)
rows_wl = build_rows(wl_df, prices, names, False)
rows_asia = build_rows(asia_df, prices, names, False)

tab1, tab2, tab3 = st.tabs([
    f"Portefeuille ({len(pf_df)})",
    f"Analysés ({len(wl_df)})",
    f"Asie ({len(asia_df)})",
])
main_cols = table_cols_with_holding_days()
with tab1:
    render_tab(rows_pf, key="pf", display_cols=main_cols)
with tab2:
    render_tab(rows_wl, key="wl", display_cols=main_cols)
with tab3:
    render_tab(rows_asia, key="asia", display_cols=main_cols)


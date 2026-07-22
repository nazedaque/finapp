from __future__ import annotations

import math
import re
import unicodedata
from datetime import date, datetime

import pandas as pd


ERROR_NUMBER_VALUES = {
    "",
    "#ref!",
    "#n/a",
    "#value!",
    "#error!",
    "—",
    "nan",
    "none",
    "<na>",
}

SHEET_ERROR_VALUES = {
    "#ref!",
    "#n/a",
    "#value!",
    "#error!",
    "#div/0!",
    "#name?",
    "#num!",
    "#null!",
    "#spill!",
    "#calc!",
}

SCREENING_COLUMN_ALIASES = {
    "ticker": "gf_ticker",
    "entreprise": "name",
    "cours": "spot_sheet",
    "devise": "currency",
    "qualite provisoire": "note",
    "buy provisoire": "buy",
    "fair provisoire": "fair",
    "trim provisoire": "trim",
    "exit provisoire": "exit",
    "verdict": "verdict",
    "confiance": "confidence",
    "point decisif": "decisive_point",
    "date screening": "screening_date",
    "version prompt": "prompt_version",
    "statut": "status",
    "yf ticker": "yf_ticker",
    "yf_ticker": "yf_ticker",
}
SCREENING_NUMERIC_COLUMNS = ("spot_sheet", "note", "buy", "fair", "trim", "exit")

COUNTRY_CODES = {
    ".AS": "NL", ".AT": "GR", ".AX": "AU", ".BO": "IN",
    ".BR": "BE", ".CO": "DK", ".DE": "DE", ".HE": "FI", ".HK": "HK",
    ".IL": "GB", ".KQ": "KR", ".KS": "KR", ".L": "GB", ".MC": "ES",
    ".MI": "IT", ".NS": "IN", ".OL": "NO", ".PA": "FR",
    ".SI": "SG", ".SS": "CN", ".ST": "SE", ".SW": "CH",
    ".SZ": "CN", ".T": "JP", ".TO": "CA", ".TW": "TW",
    ".TWO": "TW", ".V": "CA", ".VI": "AT", ".VS": "LT",
    ".WA": "PL", ".WS": "PL",
}
COUNTRY_SUFFIXES = tuple(
    sorted(COUNTRY_CODES.items(), key=lambda item: len(item[0]), reverse=True)
)


def configure_gsheets_timeout(connection, timeout: tuple[int, int]) -> bool:
    """Configure le délai réseau du client gspread enveloppé par Streamlit."""
    try:
        raw_client = getattr(connection.client, "_client", None)
        set_timeout = getattr(raw_client, "set_timeout", None)
        if not callable(set_timeout):
            return False
        set_timeout(timeout)
        return True
    except Exception:
        return False


def parse_sheet_date(value):
    """Normalise une date texte, Python ou un numéro de série Google Sheets."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        try:
            serial = float(value)
            if 1 <= serial <= 100_000:
                return (pd.Timestamp("1899-12-30") + pd.to_timedelta(serial, unit="D")).date()
        except (TypeError, ValueError, OverflowError):
            return None

    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T].*)?", text):
        parsed = pd.to_datetime(text, format="ISO8601", errors="coerce")
    else:
        parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def finite_float(value) -> float | None:
    """Convertit une valeur en nombre fini, sinon renvoie None."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_number(value) -> float | None:
    """Interprète les formats numériques français et anglais usuels."""
    if value is None:
        return None
    if not isinstance(value, str):
        return finite_float(value)

    raw = (
        value.strip()
        .replace("\u202f", "")
        .replace("\xa0", "")
        .replace(" ", "")
        .replace("%", "")
        .replace("'", "")
        .replace("’", "")
    )
    if raw.casefold() in ERROR_NUMBER_VALUES:
        return None

    if "," in raw and "." in raw:
        decimal_separator = "," if raw.rfind(",") > raw.rfind(".") else "."
        thousands_separator = "." if decimal_separator == "," else ","
        normalized = raw.replace(thousands_separator, "").replace(decimal_separator, ".")
        return finite_float(normalized)

    if "," in raw:
        if re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+", raw):
            return finite_float(raw.replace(",", ""))
        if raw.count(",") > 1:
            sign = raw[0] if raw[:1] in {"+", "-"} else ""
            unsigned = raw[1:] if sign else raw
            parts = unsigned.split(",")
            valid_grouped_decimal = (
                len(parts[-1]) in {1, 2}
                and 1 <= len(parts[0]) <= 3
                and all(part.isdigit() and len(part) == 3 for part in parts[1:-1])
                and parts[-1].isdigit()
            )
            if not valid_grouped_decimal:
                return None
            raw = sign + "".join(parts[:-1]) + "." + parts[-1]
        else:
            raw = raw.replace(",", ".")

    elif re.fullmatch(r"[+-]?\d{1,3}(?:\.\d{3})+", raw):
        raw = raw.replace(".", "")

    return finite_float(raw)


def normalize_column_name(value: str) -> str:
    """Supprime BOM, accents, espaces superflus et différences de casse."""
    normalized = str(value).replace("\ufeff", "").replace("\u202f", "").replace("\xa0", "")
    decomposed = unicodedata.normalize("NFD", normalized)
    without_accents = "".join(
        char for char in decomposed if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"\s+", " ", without_accents).strip().lower()


def is_sheet_error(value) -> bool:
    """Indique si une cellule contient une erreur de formule Google Sheets/Excel."""
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        return False
    return str(value).strip().casefold() in SHEET_ERROR_VALUES


def clean_sheet_text(value) -> str:
    """Nettoie une cellule texte et masque les erreurs de formule brutes."""
    if value is None or is_sheet_error(value):
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "<na>"} else text


def find_sheet_errors(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Recense les erreurs de formule sans dépendre de noms de colonnes uniques."""
    ticker_position = next(
        (
            index
            for index, column in enumerate(frame.columns)
            if normalize_column_name(column) in {"ticker", "yf ticker", "yf_ticker"}
        ),
        None,
    )
    errors: list[dict[str, object]] = []
    for row_position in range(len(frame)):
        row = frame.iloc[row_position]
        ticker = (
            clean_sheet_text(row.iloc[ticker_position])
            if ticker_position is not None
            else ""
        )
        for column_position, value in enumerate(row.tolist()):
            if is_sheet_error(value):
                errors.append({
                    "row": row_position + 2,
                    "ticker": ticker,
                    "column": str(frame.columns[column_position]),
                    "error": str(value).strip(),
                })
    return errors


def _missing_series_values(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip().str.casefold()
    return series.isna() | text.isin({"", "nan", "none", "<na>"})


def coalesce_alias_columns(
    frame: pd.DataFrame,
    aliases: dict[str, str],
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]]]:
    """Fusionne les en-têtes synonymes en privilégiant la première valeur renseignée."""
    groups: dict[str, list[tuple[int, str]]] = {}
    recognized_positions: set[int] = set()
    for position, source in enumerate(frame.columns):
        target = aliases.get(normalize_column_name(source))
        if target:
            groups.setdefault(target, []).append((position, str(source)))
            recognized_positions.add(position)

    untouched_positions = [
        position for position in range(len(frame.columns))
        if position not in recognized_positions
    ]
    result = frame.iloc[:, untouched_positions].copy()
    collisions: dict[str, tuple[str, ...]] = {}
    for target, entries in groups.items():
        combined = frame.iloc[:, entries[0][0]].copy()
        for position, _source in entries[1:]:
            combined = combined.mask(
                _missing_series_values(combined),
                frame.iloc[:, position],
            )
        result[target] = combined
        if len(entries) > 1:
            collisions[target] = tuple(source for _position, source in entries)

    return result, collisions


def normalize_register_frame(
    frame: pd.DataFrame,
    aliases: dict[str, str],
    numeric_columns: tuple[str, ...] | list[str],
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]]]:
    """Normalise le Registre et conserve les lignes aux valorisations incomplètes."""
    result, collisions = coalesce_alias_columns(frame, aliases)
    required_columns = tuple(dict.fromkeys((
        *aliases.values(),
        "gf_ticker",
        "yf_ticker",
        "portif",
        "last_update",
        *numeric_columns,
    )))
    for column in required_columns:
        if column not in result.columns:
            result[column] = pd.NA

    numeric_set = set(numeric_columns)
    date_columns = {"last_update"}
    non_text_columns = numeric_set | date_columns | {"portif"}
    for column in required_columns:
        if column not in non_text_columns:
            result[column] = result[column].map(clean_sheet_text)

    gf_ticker = result["gf_ticker"].map(clean_sheet_text)
    yf_ticker = result["yf_ticker"].map(clean_sheet_text)
    result["gf_ticker"] = gf_ticker.mask(gf_ticker.eq(""), yf_ticker)
    result["yf_ticker"] = yf_ticker.mask(yf_ticker.eq(""), result["gf_ticker"])
    result["gf_ticker"] = result["gf_ticker"].str.upper()
    result["yf_ticker"] = result["yf_ticker"].str.upper()

    invalid_tickers = {"", "TICKER", "GF_TICKER", "NAN", "NONE", "<NA>"}
    result = result[~result["gf_ticker"].isin(invalid_tickers)].copy()
    result["portif"] = result["portif"].map(normalize_portif)
    for column in numeric_columns:
        result[column] = result[column].map(parse_number)
    result["last_update"] = pd.to_datetime(
        result["last_update"], dayfirst=True, errors="coerce"
    ).dt.date
    return result.reset_index(drop=True), collisions


def normalize_screening_frame(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, tuple[str, ...]]]:
    """Normalise l'onglet Screening sans le mélanger au Registre."""
    result, collisions = coalesce_alias_columns(frame, SCREENING_COLUMN_ALIASES)
    for column in dict.fromkeys(SCREENING_COLUMN_ALIASES.values()):
        if column not in result.columns:
            result[column] = pd.NA

    text_columns = (
        "gf_ticker", "yf_ticker", "name", "currency", "verdict", "confidence",
        "decisive_point", "prompt_version", "status",
    )
    for column in text_columns:
        result[column] = result[column].map(clean_sheet_text)

    result["gf_ticker"] = result["gf_ticker"].mask(
        result["gf_ticker"].eq(""), result["yf_ticker"]
    )
    result["yf_ticker"] = result["yf_ticker"].mask(
        result["yf_ticker"].eq(""), result["gf_ticker"]
    )
    result["gf_ticker"] = result["gf_ticker"].str.upper()
    result["yf_ticker"] = result["yf_ticker"].str.upper()
    invalid_tickers = {"", "TICKER", "GF_TICKER", "NAN", "NONE", "<NA>"}
    result = result[~result["gf_ticker"].isin(invalid_tickers)].copy()

    for column in SCREENING_NUMERIC_COLUMNS:
        result[column] = result[column].map(parse_number)

    result["screening_date"] = pd.to_datetime(
        result["screening_date"], dayfirst=True, errors="coerce"
    ).dt.date

    return result.reset_index(drop=True), collisions


def stale_quote_tickers(
    tickers: tuple[str, ...],
    attempt_times: dict[str, float],
    now: float,
    ttl: float,
) -> tuple[str, ...]:
    """Renvoie les cotations jamais tentées ou dont le dernier essai a expiré."""
    return tuple(
        ticker
        for ticker in tickers
        if (
            finite_float(attempt_times.get(ticker)) is None
            or now - finite_float(attempt_times.get(ticker)) >= ttl
        )
    )


def safe_date_ordinal(value) -> int | None:
    """Convertit une date en ordinal sans confondre pandas.NaT avec une date valide."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    try:
        if hasattr(value, "toordinal"):
            return int(value.toordinal())
        parsed = pd.to_datetime(value, errors="coerce")
        return None if pd.isna(parsed) else int(parsed.toordinal())
    except (TypeError, ValueError, OverflowError):
        return None


def merge_quote_cache(
    cached: dict[str, dict],
    fresh: dict[str, dict],
    tickers: tuple[str, ...],
) -> dict[str, dict]:
    """Fusionne les cours sans écraser une valeur valide par un échec Yahoo."""
    cached_index = {str(key).upper(): dict(value) for key, value in cached.items()}
    fresh_index = {str(key).upper(): dict(value) for key, value in fresh.items()}
    merged: dict[str, dict] = {}

    for ticker in tickers:
        key = str(ticker).upper()
        old = cached_index.get(key)
        new = fresh_index.get(key)
        if new is None:
            if old is not None:
                merged[key] = old
            continue

        new_price = finite_float(new.get("price"))
        if new_price is not None:
            merged[key] = {
                **new,
                "price": new_price,
                "chg": finite_float(new.get("chg")),
                "name": clean_sheet_text(new.get("name"))
                or clean_sheet_text((old or {}).get("name")),
                "error": clean_sheet_text(new.get("error")),
                "_stale": False,
            }
            continue

        old_price = finite_float((old or {}).get("price"))
        if old is not None and old_price is not None:
            merged[key] = {
                **old,
                "price": old_price,
                "chg": finite_float(old.get("chg")),
                "error": clean_sheet_text(new.get("error")),
                "_stale": True,
            }
        else:
            merged[key] = {
                **new,
                "price": None,
                "chg": finite_float(new.get("chg")),
                "name": clean_sheet_text(new.get("name")),
                "error": clean_sheet_text(new.get("error")),
                "_stale": False,
            }
    return merged


def normalize_portif(value) -> int:
    """Normalise les représentations usuelles d'une appartenance au portefeuille."""
    if isinstance(value, bool):
        return int(value)

    number = finite_float(value)
    if number is not None:
        return 1 if number == 1.0 else 0

    normalized = str(value).strip().casefold()
    return 1 if normalized in {"true", "vrai", "yes", "oui"} else 0


def is_suspended_underwriting(row) -> bool:
    """Repère un underwriting suspendu sans score ni zones exploitables."""
    if not clean_sheet_text(row.get("prompt_version")):
        return False
    if normalize_column_name(row.get("next_action")) != "suspendre":
        return False
    analytic_fields = ("note", "score_sheet", "buy", "fair", "trim", "exit")
    return all(finite_float(row.get(field)) is None for field in analytic_fields)


def country_code(ticker: str) -> str:
    normalized = str(ticker or "").upper().strip()
    for suffix, code in COUNTRY_SUFFIXES:
        if normalized.endswith(suffix):
            return code
    return "US" if normalized else ""


def compute_ratio(price, buy, exit_) -> float | None:
    """Position normalisée du cours entre Buy et Exit."""
    p = finite_float(price)
    b = finite_float(buy)
    e = finite_float(exit_)
    if p is None or b is None or e is None or e <= b:
        return None
    return max(0.0, min(1.0, (e - p) / (e - b)))


def compute_score(ratio, note) -> float | None:
    """Score combiné, uniquement lorsque ses deux entrées sont valides."""
    ratio_value = finite_float(ratio)
    note_value = finite_float(note)
    if ratio_value is None or note_value is None:
        return None
    return (0.6 * ratio_value + 0.4 * note_value / 100) * 100

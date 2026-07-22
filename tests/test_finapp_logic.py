import math
import unittest
import warnings
from datetime import date

import pandas as pd

from finapp_logic import (
    clean_sheet_text,
    coalesce_alias_columns,
    configure_gsheets_timeout,
    compute_ratio,
    compute_score,
    country_code,
    find_sheet_errors,
    is_blocking_audit_status,
    is_suspended_underwriting,
    merge_quote_cache,
    normalize_register_frame,
    normalize_screening_frame,
    normalize_portif,
    parse_number,
    parse_sheet_date,
    safe_date_ordinal,
    stale_quote_tickers,
)


class GSheetsTimeoutTests(unittest.TestCase):
    def test_wrapped_gspread_client_receives_timeout(self):
        class RawClient:
            def __init__(self):
                self.timeout = None

            def set_timeout(self, timeout):
                self.timeout = timeout

        class WrappedClient:
            def __init__(self):
                self._client = RawClient()

        class Connection:
            def __init__(self):
                self.client = WrappedClient()

        connection = Connection()
        self.assertTrue(configure_gsheets_timeout(connection, (5, 15)))
        self.assertEqual(connection.client._client.timeout, (5, 15))

    def test_missing_internal_client_is_a_safe_noop(self):
        class Connection:
            client = object()

        self.assertFalse(configure_gsheets_timeout(Connection(), (5, 15)))


class PortfolioNormalizationTests(unittest.TestCase):
    def test_numeric_one_variants_are_in_portfolio(self):
        for value in (1, 1.0, "1", "1.0", True):
            with self.subTest(value=value):
                self.assertEqual(normalize_portif(value), 1)

    def test_boolean_text_variants_are_in_portfolio(self):
        for value in ("TRUE", "true", "VRAI", "oui", "YES"):
            with self.subTest(value=value):
                self.assertEqual(normalize_portif(value), 1)

    def test_missing_or_zero_values_are_outside_portfolio(self):
        for value in (None, 0, 0.0, "", "0", False, float("nan")):
            with self.subTest(value=value):
                self.assertEqual(normalize_portif(value), 0)


class ScoreSafetyTests(unittest.TestCase):
    def test_ratio_rejects_missing_or_non_finite_inputs(self):
        invalid_cases = (
            (75, float("nan"), 100),
            (75, 50, float("nan")),
            (float("nan"), 50, 100),
            (75, float("inf"), 100),
            (75, 50, None),
        )
        for price, buy, exit_ in invalid_cases:
            with self.subTest(price=price, buy=buy, exit=exit_):
                self.assertIsNone(compute_ratio(price, buy, exit_))

    def test_score_rejects_non_finite_inputs(self):
        self.assertIsNone(compute_score(float("nan"), 80))
        self.assertIsNone(compute_score(0.5, float("inf")))

    def test_complete_live_inputs_use_the_sheet_formula(self):
        score = compute_score(compute_ratio(75, 50, 100), 80)
        self.assertTrue(math.isclose(score, 62.0))


class NumberParsingTests(unittest.TestCase):
    def test_french_and_english_grouped_decimals(self):
        cases = {
            "1 234,56": 1234.56,
            "1.234,56": 1234.56,
            "1,234.56": 1234.56,
            "-1,234.56": -1234.56,
            "-1,234": -1234.0,
            "1,234,56": 1234.56,
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertTrue(math.isclose(parse_number(raw), expected))

    def test_invalid_or_non_finite_values_are_missing(self):
        for value in (None, "", "nan", "inf", float("nan"), float("inf"), "#N/A"):
            with self.subTest(value=value):
                self.assertIsNone(parse_number(value))


class AliasCoalescingTests(unittest.TestCase):
    def test_synonymous_headers_are_coalesced_without_duplicate_columns(self):
        frame = pd.DataFrame({
            "Ticker": ["AIR.PA", "MC.PA", "AM.PA"],
            "Entreprise": ["Airbus", None, ""],
            "Société": ["Airbus SE", "LVMH", "Dassault Aviation"],
        })
        aliases = {
            "ticker": "gf_ticker",
            "entreprise": "name",
            "societe": "name",
        }

        result, collisions = coalesce_alias_columns(frame, aliases)

        self.assertEqual(result["name"].tolist(), ["Airbus", "LVMH", "Dassault Aviation"])
        self.assertEqual(result.columns.tolist().count("name"), 1)
        self.assertNotIn("Entreprise", result.columns)
        self.assertNotIn("Société", result.columns)
        self.assertEqual(collisions, {"name": ("Entreprise", "Société")})

    def test_duplicate_exact_headers_are_coalesced_by_position(self):
        frame = pd.DataFrame(
            [["MC.PA", "", "LVMH"], ["", "AIR.PA", "Airbus"]],
            columns=["Ticker", "Ticker", "Entreprise"],
        )

        result, collisions = coalesce_alias_columns(
            frame,
            {"ticker": "gf_ticker", "entreprise": "name"},
        )

        self.assertEqual(result["gf_ticker"].tolist(), ["MC.PA", "AIR.PA"])
        self.assertEqual(result["name"].tolist(), ["LVMH", "Airbus"])
        self.assertEqual(collisions, {"gf_ticker": ("Ticker", "Ticker")})


class SheetReliabilityTests(unittest.TestCase):
    ALIASES = {
        "portif": "portif",
        "ticker": "gf_ticker",
        "entreprise": "name",
        "cours": "spot_sheet",
        "qualite /100": "note",
        "buy": "buy",
        "fair": "fair",
        "trim": "trim",
        "exit": "exit",
        "score global /100": "score_sheet",
        "date analyse": "last_update",
        "action suivante": "next_action",
        "yf ticker": "yf_ticker",
    }
    NUMERIC = ("spot_sheet", "note", "buy", "fair", "trim", "exit", "score_sheet")

    def test_formula_errors_are_reported_then_cleaned_from_text(self):
        frame = pd.DataFrame({
            "Portif": [1],
            "Ticker": [" lacr.pa "],
            "Entreprise": ["LACROIX"],
            "Action suivante": ["#REF!"],
            "Date analyse": ["17/07/2026"],
        })

        errors = find_sheet_errors(frame)
        result, _ = normalize_register_frame(frame, self.ALIASES, self.NUMERIC)

        self.assertEqual(errors, [{
            "row": 2,
            "ticker": "lacr.pa",
            "column": "Action suivante",
            "error": "#REF!",
        }])
        self.assertEqual(result.loc[0, "gf_ticker"], "LACR.PA")
        self.assertEqual(result.loc[0, "next_action"], "")

    def test_incomplete_valuation_row_is_retained(self):
        frame = pd.DataFrame({
            "Portif": [0],
            "Ticker": ["rmv.l"],
            "Entreprise": ["Rightmove"],
            "Cours": [""],
            "Qualité /100": [""],
            "Buy": [""],
            "Fair": [""],
            "Trim": [""],
            "Exit": [""],
            "Score global /100": [""],
            "Date analyse": [""],
        })

        result, _ = normalize_register_frame(frame, self.ALIASES, self.NUMERIC)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "gf_ticker"], "RMV.L")
        self.assertEqual(result.loc[0, "yf_ticker"], "RMV.L")
        for column in self.NUMERIC:
            self.assertIsNone(result.loc[0, column])

    def test_yahoo_ticker_can_rescue_a_missing_display_ticker(self):
        frame = pd.DataFrame({
            "Portif": [0],
            "Ticker": [""],
            "YF ticker": ["aapl"],
            "Entreprise": ["Apple"],
        })

        result, _ = normalize_register_frame(frame, self.ALIASES, self.NUMERIC)

        self.assertEqual(result.loc[0, "gf_ticker"], "AAPL")
        self.assertEqual(result.loc[0, "yf_ticker"], "AAPL")

    def test_sheet_error_cleaner_preserves_normal_hash_text(self):
        self.assertEqual(clean_sheet_text("#REF!"), "")
        self.assertEqual(clean_sheet_text("Priorité #1"), "Priorité #1")

    def test_missing_pandas_date_has_no_sort_ordinal(self):
        self.assertIsNone(safe_date_ordinal(pd.NaT))
        self.assertIsNone(safe_date_ordinal(None))
        self.assertIsNone(safe_date_ordinal("date invalide"))
        self.assertEqual(safe_date_ordinal(date(2026, 7, 17)), date(2026, 7, 17).toordinal())


class ScreeningNormalizationTests(unittest.TestCase):
    def test_real_screening_headers_are_normalized_independently(self):
        frame = pd.DataFrame({
            "Ticker": ["MC.PA"],
            "Entreprise": ["LVMH"],
            "Cours": ["1,234.56"],
            "Devise": ["EUR"],
            "Qualité provisoire": ["82,5"],
            "Buy provisoire": ["600,00"],
            "Fair provisoire": ["750,00"],
            "Trim provisoire": ["850,00"],
            "Exit provisoire": ["950,00"],
            "Verdict": ["À approfondir"],
            "Confiance": ["Moyenne"],
            "Point décisif": ["Marge"],
            "Date screening": ["17/07/2026"],
            "Version prompt": ["v3"],
            "Statut": ["Actif"],
        })

        result, collisions = normalize_screening_frame(frame)

        self.assertEqual(collisions, {})
        self.assertEqual(result.loc[0, "gf_ticker"], "MC.PA")
        self.assertEqual(result.loc[0, "yf_ticker"], "MC.PA")
        self.assertEqual(result.loc[0, "name"], "LVMH")
        self.assertTrue(math.isclose(result.loc[0, "spot_sheet"], 1234.56))
        self.assertTrue(math.isclose(result.loc[0, "note"], 82.5))
        self.assertEqual(result.loc[0, "verdict"], "À approfondir")
        self.assertEqual(result.loc[0, "status"], "Actif")
        self.assertEqual(str(result.loc[0, "screening_date"]), "2026-07-17")

    def test_incomplete_screening_targets_are_kept_as_missing(self):
        frame = pd.DataFrame({
            "Ticker": ["btrw.l"],
            "Entreprise": ["Barratt Redrow"],
            "Cours": ["492,40"],
            "Qualité provisoire": [""],
            "Buy provisoire": [""],
            "Fair provisoire": [""],
            "Trim provisoire": [""],
            "Exit provisoire": [""],
            "Verdict": ["#N/A"],
            "Date screening": ["17/07/2026"],
            "Statut": ["À traiter"],
        })

        result, _ = normalize_screening_frame(frame)

        self.assertEqual(result.loc[0, "gf_ticker"], "BTRW.L")
        self.assertEqual(result.loc[0, "verdict"], "")
        for column in ("note", "buy", "fair", "trim", "exit"):
            self.assertIsNone(result.loc[0, column])


class QuoteCacheReliabilityTests(unittest.TestCase):
    def test_expired_and_never_attempted_quotes_are_selected(self):
        tickers = ("AAPL", "MC.PA", "AIR.PA")
        attempts = {"AAPL": 950.0, "MC.PA": 100.0}

        self.assertEqual(
            stale_quote_tickers(tickers, attempts, now=1000.0, ttl=300.0),
            ("MC.PA", "AIR.PA"),
        )

    def test_failed_refresh_keeps_last_valid_quote_and_marks_it_stale(self):
        cached = {
            "AAPL": {"price": 210.0, "chg": 1.5, "name": "Apple", "error": ""}
        }
        fresh = {
            "AAPL": {"price": None, "chg": None, "name": "", "error": "timeout"}
        }

        result = merge_quote_cache(cached, fresh, ("AAPL",))

        self.assertEqual(result["AAPL"]["price"], 210.0)
        self.assertEqual(result["AAPL"]["chg"], 1.5)
        self.assertEqual(result["AAPL"]["error"], "timeout")
        self.assertTrue(result["AAPL"]["_stale"])

    def test_successful_refresh_replaces_stale_quote(self):
        cached = {"AAPL": {"price": 210.0, "chg": 1.5, "_stale": True}}
        fresh = {"AAPL": {"price": 212.0, "chg": 0.8, "name": "Apple", "error": ""}}

        result = merge_quote_cache(cached, fresh, ("AAPL",))

        self.assertEqual(result["AAPL"]["price"], 212.0)
        self.assertEqual(result["AAPL"]["chg"], 0.8)
        self.assertFalse(result["AAPL"]["_stale"])

    def test_non_finite_fresh_quote_cannot_overwrite_cache(self):
        cached = {"AAPL": {"price": 210.0, "chg": 1.5}}
        fresh = {"AAPL": {"price": float("inf"), "chg": float("nan")}}

        result = merge_quote_cache(cached, fresh, ("AAPL",))

        self.assertEqual(result["AAPL"]["price"], 210.0)
        self.assertTrue(result["AAPL"]["_stale"])


class GeographicClassificationTests(unittest.TestCase):
    def test_supported_exchange_suffixes_have_the_correct_country(self):
        expected = {
            "RELIANCE.NS": "IN",
            "RELIANCE.BO": "IN",
            "2330.TW": "TW",
            "6488.TWO": "TW",
            "000001.SS": "CN",
            "399001.SZ": "CN",
            "BHP.AX": "AU",
            "NOKIA.HE": "FI",
            "HSBK.IL": "GB",
            "TOI.V": "CA",
            "STR.VI": "AT",
            "RSU1L.VS": "LT",
            "GPP.WA": "PL",
            "4MS.WS": "PL",
            "AAPL": "US",
        }
        for ticker, code in expected.items():
            with self.subTest(ticker=ticker):
                self.assertEqual(country_code(ticker), code)


class WatchlistEligibilityTests(unittest.TestCase):
    def test_vetoed_underwriting_is_suspended(self):
        row = {
            "prompt_version": "SOL.8b-X",
            "next_action": "Suspendre",
            "note": None,
            "score_sheet": None,
            "buy": None,
            "fair": None,
            "trim": None,
            "exit": None,
        }

        self.assertTrue(is_suspended_underwriting(row))

    def test_valid_underwriting_is_not_suspended(self):
        row = {
            "prompt_version": "SOL.8b-X",
            "next_action": "Lancer l’audit",
            "note": 67,
            "score_sheet": 64,
            "buy": 36,
            "fair": 49,
            "trim": 57,
            "exit": 66,
        }

        self.assertFalse(is_suspended_underwriting(row))

    def test_unconfirmed_or_failed_audit_blocks_active_decision(self):
        for status in (
            "CORRECTION À CONFIRMER",
            "VALIDATION FAIL",
            "NON AUDITABLE",
        ):
            with self.subTest(status=status):
                self.assertTrue(is_blocking_audit_status(status))

    def test_completed_audit_statuses_remain_operational(self):
        for status in (
            "PASS",
            "PASS AVEC RÉSERVES",
            "CORRIGÉ APRÈS AUDIT",
            "CORRECTION MATÉRIELLE",
            "VALIDATION PASS",
        ):
            with self.subTest(status=status):
                self.assertFalse(is_blocking_audit_status(status))


class SheetDateParsingTests(unittest.TestCase):
    def test_iso_dates_do_not_emit_dayfirst_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            parsed = parse_sheet_date("2026-07-17")

        self.assertEqual(parsed, date(2026, 7, 17))
        self.assertFalse(any("dayfirst" in str(item.message) for item in caught))

    def test_french_dates_keep_day_first_semantics(self):
        self.assertEqual(parse_sheet_date("17/07/2026"), date(2026, 7, 17))

if __name__ == "__main__":
    unittest.main()

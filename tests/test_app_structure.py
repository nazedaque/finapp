import ast
import html
from html.parser import HTMLParser
from pathlib import Path
import re
import unittest
import urllib

import finapp_logic
import pandas as pd


class _SingleTagParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))


class AppStructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (Path(__file__).parents[1] / "app.py").read_text(encoding="utf-8")

    def test_only_current_business_tabs_are_declared(self):
        self.assertIn("tab1, tab2, tab3 = st.tabs(", self.source)
        self.assertIn('f"Portefeuille ({len(pf_df)})"', self.source)
        self.assertIn('f"Watchlist ({len(wl_df)})"', self.source)
        self.assertIn('f"Screenés ({len(to_analyze_df)})"', self.source)

    def test_every_finapp_logic_import_exists(self):
        tree = ast.parse(self.source)
        imported_names = [
            alias.name
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == "finapp_logic"
            for alias in node.names
        ]
        missing = [name for name in imported_names if not hasattr(finapp_logic, name)]
        self.assertEqual(missing, [])

    def test_only_non_auditable_blocks_active_values(self):
        self.assertIn(
            '_normalize_col(r.get("_audit_status")) == "non auditable"',
            self.source,
        )
        self.assertNotIn("correction a confirmer", self.source.casefold())
        self.assertNotIn("validation fail", self.source.casefold())

    def test_screener_uses_the_separate_screening_sheet(self):
        self.assertIn('SCREENING_SHEET_NAME = "Screening"', self.source)
        self.assertIn("def load_screening_candidates(", self.source)
        self.assertIn("_normalize_screening_candidates(raw_df, registry_tickers)", self.source)
        self.assertIn("rows_to_analyze = build_rows(to_analyze_df", self.source)

    def test_quote_cache_has_expiry_and_failed_refresh_fallback(self):
        self.assertIn("stale_quote_tickers(", self.source)
        self.assertIn("merge_quote_cache(cached_prices, fresh_prices, all_yf)", self.source)
        self.assertIn('st.session_state["quote_attempt_times"]', self.source)

    def test_google_sheets_requests_have_a_bounded_timeout(self):
        self.assertIn("GSHEETS_HTTP_TIMEOUT = (5, 15)", self.source)
        self.assertIn(
            "configure_gsheets_timeout(connection, GSHEETS_HTTP_TIMEOUT)",
            self.source,
        )
        self.assertIn(
            'LOGGER.exception("Échec du chargement Google Sheets : Registre")',
            self.source,
        )
        self.assertIn("anciennes valeurs conservées", self.source)

    def test_sheet_formula_errors_are_visible_but_not_rendered_as_values(self):
        self.assertIn('st.session_state["sheet_errors"] = find_sheet_errors(df)', self.source)
        self.assertIn("warn_sheet_errors(", self.source)

    def test_price_and_score_use_yahoo_without_sheet_fallback(self):
        self.assertNotIn('price = r.get("spot_sheet")', self.source)
        self.assertNotIn('score = safe_float(r.get("score_sheet"))', self.source)
        self.assertIn(
            "score = compute_score(compute_ratio(price, buy, exit_), quality)",
            self.source,
        )
        self.assertIn("Score global calculé avec le cours Yahoo", self.source)

    def test_live_score_colors_match_the_sheet_gradient_stops(self):
        tree = ast.parse(self.source)
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "score_gradient_color"
        )
        namespace = {
            "safe_float": lambda value: None if value is None else float(value),
        }
        module = ast.Module(body=[function], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), "app.py", "exec"), namespace)

        color = namespace["score_gradient_color"]
        self.assertEqual(color(20), "#ff0000")
        self.assertEqual(color(30), "#ff0000")
        self.assertEqual(color(50), "#ffd966")
        self.assertEqual(color(80), "#6aa84f")
        self.assertEqual(color(95), "#6aa84f")
        self.assertIsNone(color(None))

    def test_legacy_asia_routing_is_absent(self):
        self.assertNotIn("Asie", self.source)
        self.assertNotIn("asia_", self.source)
        self.assertNotIn('refresh_scope="asia"', self.source)

    def test_refresh_is_global_and_preserves_the_active_tab(self):
        self.assertIn('key="refresh_all"', self.source)
        self.assertIn("def remember_active_tab()", self.source)
        self.assertIn('st.session_state["active_tab_slug"]', self.source)
        self.assertIn("default=default_tab", self.source)

    def test_streamlit_158_iframe_api_replaces_deprecated_components(self):
        self.assertNotIn("streamlit.components.v1", self.source)
        self.assertNotIn("components.html", self.source)
        self.assertGreaterEqual(self.source.count("st.iframe("), 2)
        self.assertIn("tab_index=-1", self.source)

    def test_empty_states_and_mobile_stats_are_contextual(self):
        self.assertIn("Aucun titre en portefeuille.", self.source)
        self.assertIn("Aucun titre dans la watchlist.", self.source)
        self.assertIn("Aucun screening à afficher.", self.source)
        self.assertIn("min-width: max-content", self.source)
        self.assertIn("overflow-x: auto", self.source)

    def test_ticker_link_has_one_complete_valid_style_attribute(self):
        tree = ast.parse(self.source)
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "html_ticker_link"
        )
        namespace = {"html": html, "urllib": urllib}
        module = ast.Module(body=[function], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), "app.py", "exec"), namespace)

        markup = namespace["html_ticker_link"]("A&B.PA", "<A&B>")
        parser = _SingleTagParser()
        parser.feed(markup)

        self.assertEqual(len(parser.tags), 1)
        tag, attrs = parser.tags[0]
        self.assertEqual(tag, "a")
        self.assertEqual(attrs["href"], "https://finance.yahoo.com/quote/A%26B.PA/")
        self.assertEqual(attrs["rel"], "noopener")
        self.assertIn("font-family:'JetBrains Mono',monospace", attrs["style"])
        self.assertIn("letter-spacing:.02em", attrs["style"])
        self.assertNotIn("jetbrains", attrs)
        self.assertIn("&lt;A&amp;B&gt;", markup)

    def test_workflow_links_use_strict_codex_deep_links(self):
        tree = ast.parse(self.source)
        selected = []
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "CODEX_THREAD_LINK_RE"
                for target in node.targets
            ):
                selected.append(node)
            if isinstance(node, ast.FunctionDef) and node.name in {
                "normalize_codex_thread_link",
                "html_workflow_badge",
            }:
                selected.append(node)

        namespace = {"html": html, "pd": pd, "re": re}
        module = ast.Module(body=selected, type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), "app.py", "exec"), namespace)

        valid = "codex://threads/019f6fab-de3c-7503-af3b-4234b6adb10d"
        self.assertEqual(namespace["normalize_codex_thread_link"](valid), valid)
        self.assertEqual(namespace["normalize_codex_thread_link"]("https://example.com"), "")

        markup = namespace["html_workflow_badge"]("U", "green", "Ouvrir", valid)
        parser = _SingleTagParser()
        parser.feed(markup)
        self.assertEqual(parser.tags[0][0], "a")
        self.assertEqual(parser.tags[0][1]["href"], valid)
        self.assertIn("workflow-light--green", markup)

        invalid_markup = namespace["html_workflow_badge"](
            "A", "green", "Lien invalide", "javascript:alert(1)"
        )
        self.assertNotIn("<a ", invalid_markup)
        self.assertIn("workflow-link--disabled", invalid_markup)

    def test_links_column_replaces_the_legacy_audit_light(self):
        self.assertIn('"MAJ", "Liens", "JRS"', self.source)
        self.assertIn('"lien underwriting": "underwriting_link"', self.source)
        self.assertIn('"lien audit": "audit_link"', self.source)
        self.assertNotIn(".audit-light", self.source)

    def test_latest_audit_row_supplies_status_and_link_together(self):
        tree = ast.parse(self.source)
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_normalize_audit_data"
        )

        class _StreamlitStub:
            session_state = {}

        namespace = {
            "pd": pd,
            "st": _StreamlitStub(),
            "coalesce_alias_columns": finapp_logic.coalesce_alias_columns,
            "clean_sheet_text": finapp_logic.clean_sheet_text,
            "AUDIT_COL_NORMALIZED": {
                "ticker": "gf_ticker",
                "statut audit": "audit_status",
                "lien audit": "audit_link",
            },
        }
        module = ast.Module(body=[function], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), "app.py", "exec"), namespace)

        old_link = "codex://threads/00000000-0000-0000-0000-000000000001"
        new_link = "codex://threads/00000000-0000-0000-0000-000000000002"
        frame = pd.DataFrame({
            "Ticker": ["ABC", "ABC"],
            "Statut audit": ["PASS", "CORRECTION MATÉRIELLE"],
            "Lien audit": [old_link, new_link],
        })
        statuses, links = namespace["_normalize_audit_data"](frame)
        self.assertEqual(statuses["ABC"], "CORRECTION MATÉRIELLE")
        self.assertEqual(links["ABC"], new_link)


if __name__ == "__main__":
    unittest.main()

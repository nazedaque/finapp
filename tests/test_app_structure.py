import ast
import html
from html.parser import HTMLParser
from pathlib import Path
import unittest
import urllib


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

    def test_screener_uses_the_separate_screening_sheet(self):
        self.assertIn('SCREENING_SHEET_NAME = "Screening"', self.source)
        self.assertIn("def load_screening_candidates(", self.source)
        self.assertIn("_normalize_screening_candidates(raw_df, registry_tickers)", self.source)
        self.assertIn("rows_to_analyze = build_rows(to_analyze_df", self.source)

    def test_quote_cache_has_expiry_and_failed_refresh_fallback(self):
        self.assertIn("stale_quote_tickers(", self.source)
        self.assertIn("merge_quote_cache(cached_prices, fresh_prices, all_yf)", self.source)
        self.assertIn('st.session_state["quote_attempt_times"]', self.source)
        self.assertIn("anciennes valeurs conservées", self.source)

    def test_sheet_formula_errors_are_visible_but_not_rendered_as_values(self):
        self.assertIn('st.session_state["sheet_errors"] = find_sheet_errors(df)', self.source)
        self.assertIn("warn_sheet_errors(", self.source)

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


if __name__ == "__main__":
    unittest.main()

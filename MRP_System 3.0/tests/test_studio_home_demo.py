"""The minimal homepage preview keeps actual workspace and dashboard wiring."""
from html.parser import HTMLParser
from pathlib import Path
import unittest

STATIC = Path(__file__).resolve().parents[2] / 'Webpage' / 'static'


class Elements(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = []

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


class StudioHomepageDemoTests(unittest.TestCase):
    def setUp(self):
        self.html = (STATIC / 'studio-home-demo.html').read_text(encoding='utf-8')
        parser = Elements()
        parser.feed(self.html)
        self.elements = parser.elements

    def test_real_module_destinations_without_homepage_kpi_link(self):
        links = [attrs.get('href') for tag, attrs in self.elements if tag == 'a']
        for route in ['/', '/planning', '/quotation_lookup', '/purchase_orders', '/item_info']:
            self.assertIn(route, links)
        self.assertNotIn('/planning/kpi', links)
        self.assertNotIn('#', links)
        header = self.html.split('<header class="topbar">', 1)[1].split('</header>', 1)[0]
        for route in ['/quotation_lookup', '/item_info', '/purchase_orders']:
            self.assertIn('href="' + route + '"', header)
        self.assertNotIn('workspace-title', self.html)

    def test_minimal_workspace_and_attention_without_marketing(self):
        self.assertIn('Needs attention', self.html)
        self.assertIn('Open planning', self.html)
        self.assertEqual(self.html.count('<main '), 1)
        self.assertEqual(self.html.count('</main>'), 1)
        self.assertNotIn('wordmark-frame', self.html)
        self.assertNotIn('hero', self.html)
        forms = [attrs for tag, attrs in self.elements if tag == 'form']
        self.assertEqual(len(forms), 1)
        self.assertEqual(forms[0]['action'], '/global_search')
        self.assertEqual(forms[0]['method'], 'get')
        css = (STATIC / 'studio-home-demo.css').read_text(encoding='utf-8')
        self.assertIn(':focus-visible', css)
        self.assertIn('prefers-reduced-motion', css)
        self.assertIn('overflow-x: clip', css)

    def test_greeting_and_existing_snoopy_search_are_reused(self):
        self.assertIn('id="time-greeting"', self.html)
        self.assertIn('/static/snoopy-figure-transparent.png', self.html)
        self.assertIn('/static/home-search.js', self.html)
        self.assertIn('id="snoopy-search-toggle"', self.html)
        self.assertIn('aria-expanded="false"', self.html)
        self.assertIn('id="snoopy-search-bubble"', self.html)
        script = (STATIC / 'studio-home-demo.js').read_text(encoding='utf-8')
        for greeting in ['Good morning', 'Good afternoon', 'Good evening']:
            self.assertIn(greeting, script)

    def test_read_only_alert_loading_has_error_and_accessibility_states(self):
        script = (STATIC / 'studio-home-demo.js').read_text(encoding='utf-8')
        self.assertIn("fetch('/',", script)
        self.assertIn('home-alerts-title', script)
        self.assertIn('textContent', script)
        self.assertNotIn('innerHTML', script)
        self.assertIn('Alerts could not be loaded.', script)
        self.assertIn("setAttribute('aria-busy', 'false')", script)
        self.assertIn('aria-live="polite"', self.html)
        self.assertIn('<noscript>', self.html)


if __name__ == '__main__':
    unittest.main()

"""Check the consolidated UI without importing the database-backed server."""
import ast
from pathlib import Path
import re
import runpy
import unittest

from jinja2 import Environment


WEB_ROOT = Path(__file__).resolve().parents[2] / "Webpage"


class WebUIAssetsTests(unittest.TestCase):
    def test_all_templates_parse_and_have_existing_static_assets(self):
        templates = {
            name: value
            for name, value in runpy.run_path(str(WEB_ROOT / "ui.py")).items()
            if name.endswith("_TPL")
        }
        self.assertEqual(len(templates), 7)
        self.assertNotIn('INVENTORY_TPL', templates)
        for name, template in templates.items():
            with self.subTest(template=name):
                Environment().parse(template)
                assets = re.findall(r'/static/([^\s"\'?<>]+)', template)
                assets += re.findall(r"filename=['\"]([^'\"]+)['\"]", template)
                for asset in assets:
                    self.assertTrue((WEB_ROOT / "static" / asset).is_file(), asset)

    def test_server_uses_one_ui_module(self):
        server = ast.parse((WEB_ROOT / "server.py").read_text(encoding="utf-8"))
        imports = [node for node in ast.walk(server) if isinstance(node, ast.ImportFrom)]
        self.assertFalse(any(node.module in {"quote_ui", "peripheral_status_ui"} for node in imports))
        imported = {alias.name for node in imports if node.module == "ui" for alias in node.names}
        self.assertIn("QUOTE_TPL", imported)
        self.assertIn("PERIPHERAL_STATUS_TPL", imported)
        self.assertFalse((WEB_ROOT / "quote_ui.py").exists())
        self.assertFalse((WEB_ROOT / "peripheral_status_ui.py").exists())

    def test_production_planning_removed(self):
        server = (WEB_ROOT / "server.py").read_text(encoding="utf-8")
        ui = (WEB_ROOT / "ui.py").read_text(encoding="utf-8")
        self.assertNotIn("production_planning", server)
        self.assertNotIn("production_planning", ui)
        self.assertNotIn("PRODUCTION_TPL", ui)
        self.assertNotIn("production_overrides", server)
        self.assertNotIn("FINAL_SO", server)
        self.assertIn("Sales Order Not Assigned LT", ui)

    def test_one_transparent_asset_per_snoopy_figure(self):
        assets = sorted(path.name for path in (WEB_ROOT / "static").glob("snoopy*"))
        self.assertEqual(assets, ["snoopy-dancing-transparent.png", "snoopy-figure-transparent.png"])
        for name in assets:
            header = (WEB_ROOT / "static" / name).read_bytes()[:26]
            self.assertEqual(header[:8], b"\x89PNG\r\n\x1a\n")
            self.assertIn(header[25], (4, 6), "PNG must have an alpha channel")


if __name__ == "__main__":
    unittest.main()

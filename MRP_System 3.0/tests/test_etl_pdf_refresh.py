"""Verify watcher invocation without running the watcher or touching the DB."""
import ast
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock


SOURCE = Path(__file__).resolve().parents[1] / 'mrp_system' / 'cli' / 'etl.py'


class ETLPDFRefreshTests(unittest.TestCase):
    def test_refresh_command_and_failure_propagation(self):
        tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
        helper = next(node for node in tree.body
                      if isinstance(node, ast.FunctionDef) and node.name == '_refresh_pdf_orders')
        runner = Mock()
        runner.CalledProcessError = subprocess.CalledProcessError
        context = {'Path': Path, 'sys': sys, 'subprocess': runner, 'logging': Mock()}
        exec(compile(ast.Module(body=[helper], type_ignores=[]), str(SOURCE), 'exec'), context)
        context['_refresh_pdf_orders']()
        watcher = Path(r'C:\Users\Admin\Desktop\File_Watcher\file_watcher.py')
        runner.run.assert_called_once_with(
            [sys.executable, str(watcher), '--only-pdf'], cwd=watcher.parent, check=True,
            capture_output=True, text=True, errors='replace')
        context['logging'].error.assert_not_called()
        runner.run.side_effect = subprocess.CalledProcessError(
            1, 'watcher', output='scan details', stderr='failure details')
        with self.assertRaises(subprocess.CalledProcessError):
            context['_refresh_pdf_orders']()
        context['logging'].error.assert_called_once_with(
            'PDF scan failed (exit %s).\n%s\n%s', 1, 'scan details', 'failure details')

    def test_refresh_precedes_pdf_read_and_reorder(self):
        tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
        main = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == 'main')
        calls = {node.func.id: node.lineno for node in ast.walk(main)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        self.assertLess(calls['_refresh_pdf_orders'], calls['fetch_pdf_orders_df_from_DB'])
        self.assertLess(calls['fetch_pdf_orders_df_from_DB'], calls['build_structured_df'])


if __name__ == '__main__':
    unittest.main()

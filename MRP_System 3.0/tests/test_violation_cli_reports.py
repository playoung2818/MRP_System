"""Exercise CLI summaries without importing ETL's input/database configuration."""
import ast
from pathlib import Path
import warnings

import pandas as pd

from mrp_system.runtime.cli_reports import format_cli_table


def cli_functions(tmp_path):
    source = Path(__file__).resolve().parents[1] / 'mrp_system' / 'cli' / 'etl.py'
    names = {'_print_violation_overview', '_normalize_violation_report', '_print_violation_diff'}
    constants = {'VIOLATION_REPORT_COLUMNS', 'VIOLATION_DIFF_KEY_COLUMNS'}
    nodes = [node for node in ast.parse(source.read_text(encoding='utf-8-sig')).body
             if (isinstance(node, ast.FunctionDef) and node.name in names)
             or (isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                 and target.id in constants for target in node.targets))]
    context = {'pd': pd, 'format_cli_table': format_cli_table, 'REPORT_DIR': tmp_path,
               'VIOLATION_SNAPSHOT_PATH': tmp_path / '.last_violation_report.csv'}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<violation-cli-test>', 'exec'), context)
    return context


def violation(so, qty):
    return {'Date': '2026-10-16', 'Item': 'PART', 'Item_raw': 'PART',
            'Projected_NAV': qty, 'Name': 'Customer', 'QB Num': so}


def summary_values(output):
    lines = output.splitlines()
    index = next(i for i, line in enumerate(lines) if line.startswith('| Current'))
    return [cell.strip() for cell in lines[index + 2].split('|')[1:-1]]


def test_overview_is_a_table(tmp_path, capsys):
    functions = cli_functions(tmp_path)
    current = pd.DataFrame([violation('SO-Z', -10), violation('SO-A', -1)])
    original = current.copy(deep=True)
    functions['_print_violation_overview'](current)
    output = capsys.readouterr().out
    assert 'INVENTORY VIOLATIONS:' in output
    assert '| Metric' in output
    assert '| Violation rows      | 2' in output
    assert '| Affected SOs        | 2' in output
    assert '| Worst projected qty | -10' in output
    assert '2026-10-16' in output
    pd.testing.assert_frame_equal(current, original)


def test_empty_overview_is_a_zero_count_table(tmp_path, capsys):
    functions = cli_functions(tmp_path)
    functions['_print_violation_overview'](pd.DataFrame(columns=functions['VIOLATION_REPORT_COLUMNS']))
    output = capsys.readouterr().out
    assert '| Violation rows      | 0' in output
    assert '| Affected SOs        | 0' in output
    assert 'nan' not in output


def test_unsorted_duplicate_keys_have_no_performance_warnings(tmp_path, capsys):
    functions = cli_functions(tmp_path)
    current = pd.DataFrame([violation('SO-Z', -10), violation('SO-A', -1), violation('SO-Z', -20)])
    current.to_csv(functions['VIOLATION_SNAPSHOT_PATH'], index=False)
    with warnings.catch_warnings():
        warnings.simplefilter('error', pd.errors.PerformanceWarning)
        functions['_print_violation_diff'](current)
    assert summary_values(capsys.readouterr().out) == ['3', '3', '0', '0', '0']
    pd.testing.assert_frame_equal(pd.read_csv(functions['VIOLATION_SNAPSHOT_PATH']), current)


def test_new_resolved_and_changed_counts_are_preserved(tmp_path, capsys):
    functions = cli_functions(tmp_path)
    previous = pd.DataFrame([violation('SO-Z', -10), violation('SO-A', -1), violation('SO-OLD', -8)])
    current = pd.DataFrame([violation('SO-Z', -12), violation('SO-A', -1), violation('SO-NEW', -9)])
    previous.to_csv(functions['VIOLATION_SNAPSHOT_PATH'], index=False)
    with warnings.catch_warnings():
        warnings.simplefilter('error', pd.errors.PerformanceWarning)
        functions['_print_violation_diff'](current)
    output = capsys.readouterr().out
    assert summary_values(output) == ['3', '3', '1', '1', '1']
    assert 'New violation rows:' in output
    assert 'Resolved violation rows:' in output
    assert 'Changed projected qty rows:' in output
    assert 'SO-NEW' in output and 'SO-OLD' in output
    assert 'Previous_Projected_NAV' in output
    pd.testing.assert_frame_equal(pd.read_csv(functions['VIOLATION_SNAPSHOT_PATH']), current)


def test_first_run_preserves_baseline_behavior(tmp_path, capsys):
    functions = cli_functions(tmp_path)
    current = pd.DataFrame([violation('SO-A', -1)])
    functions['_print_violation_diff'](current)
    output = capsys.readouterr().out
    assert 'VIOLATION CHANGES SINCE LAST RUN:' in output
    assert 'baseline' in output
    pd.testing.assert_frame_equal(pd.read_csv(functions['VIOLATION_SNAPSHOT_PATH']), current)

import pandas as pd

from mrp_system.runtime.cli_reports import format_cli_table


def test_bordered_table_left_aligns_headers_and_values():
    report = pd.DataFrame([{'QB Num': 'SO-20260750', 'Customer': 'Carnegie Robotics LLC'},
                           {'QB Num': 'SO-1', 'Customer': 'Short'}])
    lines = format_cli_table(report).splitlines()
    assert len({len(line) for line in lines}) == 1
    assert lines[0] == lines[2] == lines[-1]
    assert lines[1].startswith('| QB Num')
    assert lines[3] == '| SO-20260750 | Carnegie Robotics LLC |'
    assert lines[4] == '| SO-1        | Short                 |'
    assert len(lines) == 6


def test_missing_values_and_newlines_do_not_break_table_rows():
    report = pd.DataFrame([{'QB Num': 'SO-1', 'Customer': 'One\nTwo\tThree'},
                           {'QB Num': 'SO-2', 'Customer': None}])
    lines = format_cli_table(report).splitlines()
    assert len(lines) == 6
    assert len({len(line) for line in lines}) == 1
    assert 'One Two Three' in lines[3]
    assert 'None' not in lines[4]


def test_empty_report_still_has_a_valid_header():
    table = format_cli_table(pd.DataFrame(columns=['QB Num', 'Customer']))
    assert '| QB Num | Customer |' in table


def test_generic_columns_numbers_and_nullable_values():
    report = pd.DataFrame({'Table': ['inventory', 'ledger'],
                           'Rows': pd.array([0, None], dtype='Int64'),
                           'Projected Qty': [-5.0, float('nan')]})
    original = report.copy(deep=True)
    table = format_cli_table(report)
    assert 'Projected Qty' in table
    assert '-5.0' in table
    assert '| inventory | 0' in table
    assert '<NA>' not in table
    assert 'nan' not in table
    assert len({len(line) for line in table.splitlines()}) == 1
    pd.testing.assert_frame_equal(report, original)


def test_optional_column_selection_and_order():
    report = pd.DataFrame([{'Hidden': 'omit', 'Table': 'ledger', 'Rows': 10}])
    table = format_cli_table(report, columns=['Rows', 'Table'])
    assert '| Rows | Table  |' in table
    assert 'Hidden' not in table
    assert 'omit' not in table
    assert '| 10   | ledger |' in table


def test_header_control_characters_are_sanitized():
    table = format_cli_table(pd.DataFrame([{'One\nTwo': 'value'}]))
    assert 'One Two' in table
    assert len(table.splitlines()) == 5


def test_report_without_columns_returns_empty_text():
    assert format_cli_table(pd.DataFrame()) == ''

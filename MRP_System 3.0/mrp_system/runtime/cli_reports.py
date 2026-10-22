"""Shared bordered, left-aligned formatting for CLI DataFrame reports."""
import pandas as pd


def _cell(value):
    if pd.api.types.is_scalar(value) and pd.isna(value):
        return ''
    return str(value).replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')


def format_cli_table(report, columns=None):
    """Return a plain-text table, optionally selecting/ordering column labels.

    Does not mutate the report, print an index, or truncate rows. Missing values
    are blank; embedded line breaks/tabs become spaces. No extra dependencies.
    """
    frame = report if columns is None else report.loc[:, list(columns)]
    headers = [_cell(column) for column in frame.columns]
    if not headers:
        return ''
    rows = [[_cell(value) for value in row] for row in frame.itertuples(index=False, name=None)]
    widths = [max(len(header), max((len(row[i]) for row in rows), default=0))
              for i, header in enumerate(headers)]
    border = '+' + '+'.join('-' * (width + 2) for width in widths) + '+'

    def line(values):
        return '| ' + ' | '.join(value.ljust(width) for value, width in zip(values, widths)) + ' |'

    return '\n'.join([border, line(headers), border, *[line(row) for row in rows], border])

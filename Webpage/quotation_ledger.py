"""Presentation-only helpers for the quotation ledger timeline."""
from __future__ import annotations

import pandas as pd


def overall_minimum_flags(values: pd.Series) -> pd.Series:
    """Mark all numeric minima across the full timeline, including pending rows."""
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.notna() & numeric.eq(numeric.min())


def confirmed_minimum_flags(dates: pd.Series, values: pd.Series) -> pd.Series:
    """Mark every minimum before the unconfirmed-date sentinel, never pending rows.

    Compare numeric values before display rounding. Missing dates and values do
    not participate; an entirely pending/unknown timeline has no highlighted row.
    """
    parsed_dates = pd.to_datetime(dates, errors="coerce")
    numeric = pd.to_numeric(values, errors="coerce")
    eligible = parsed_dates.notna() & parsed_dates.lt(pd.Timestamp("2099-12-31")) & numeric.notna()
    minimum = numeric.loc[eligible].min()
    return eligible & numeric.eq(minimum)

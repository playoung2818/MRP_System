from __future__ import annotations

import pandas as pd


UNASSIGNED_LT_DATE = pd.Timestamp("2099-07-04")
FAR_FUTURE_DATE = pd.Timestamp("2099-12-31")
SHORTAGE_REPORT_CUTOFF = pd.Timestamp("2026-07-04")

DUMMY_SHIP_DATES = frozenset({UNASSIGNED_LT_DATE, FAR_FUTURE_DATE})


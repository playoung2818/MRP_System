import warnings
from unittest.mock import patch

import pandas as pd
import pytest

from mrp_system.ledger import events


@pytest.mark.parametrize('so_qty,hq_qty,pod_qty', [
    (7, 3.5, 2), (7, 3.5, 0), (7, 0, 0), (0, 3.5, 2), (0, 0, 0),
])
def test_concat_preserves_movements_and_schema_without_future_warnings(so_qty, hq_qty, pod_qty):
    day = pd.Timestamp('2026-10-22')
    so = pd.DataFrame({'Ship Date': [day], 'Item': ['TEST-A'], 'Qty(-)': [so_qty],
                       'QB Num': ['SO-TEST'], 'P. O. #': [None], 'Name': [None]})
    # All-null metadata and a missing date exercise concat's all-NA contributors.
    hq = pd.DataFrame({'Date': [None], 'Item': ['TEST-A'], 'Qty(+)': [hq_qty],
                       'QB Num': ['POD-HQ'], 'P. O. #': [None], 'Name': [None]})
    pod = pd.DataFrame({'Ship Date': [day], 'Item': ['TEST-A'], 'Qty(+)': [pod_qty],
                        'QB Num': ['POD-USA'], 'P. O. #': [None], 'Name': [None]})
    with patch.object(events, 'POD_SITE', {}), warnings.catch_warnings():
        warnings.simplefilter('error', FutureWarning)
        result = events.build_events(so, hq, pod)
    assert list(result.columns) == ['Date', 'Item', 'Delta', 'Kind', 'Source',
                                    'QB Num', 'P. O. #', 'Name', 'Item_raw']
    expected = ([(hq_qty, 'IN', 'HQ')] if hq_qty > 0 else [])
    expected += ([(pod_qty, 'IN', 'USA')] if pod_qty > 0 else [])
    expected += ([(-so_qty, 'OUT', 'SO')] if so_qty > 0 else [])
    assert list(result[['Delta', 'Kind', 'Source']].itertuples(index=False, name=None)) == expected
    assert result['Name'].isna().all()
    assert result['P. O. #'].isna().all()
    if hq_qty > 0:
        assert pd.isna(result.iloc[0]['Date'])


def test_numeric_po_identifier_still_matches_site_exclusion():
    day = pd.Timestamp('2026-10-22')
    so = pd.DataFrame({'Ship Date': [day], 'Item': ['TEST-A'], 'Qty(-)': [1],
                       'QB Num': ['SO-TEST'], 'P. O. #': [None], 'Name': [None]})
    hq = pd.DataFrame({'Date': [day], 'Item': ['TEST-A'], 'Qty(+)': [2.0],
                       'QB Num': [None], 'P. O. #': [123], 'Name': [None]})
    with patch.object(events, 'POD_SITE', {'123': 'Drop Ship'}), warnings.catch_warnings():
        warnings.simplefilter('error', FutureWarning)
        result = events.build_events(so, hq)
    # A float-promoted PO ('123.0') would miss the exclusion and leave an IN row.
    assert result['Kind'].tolist() == ['OUT']
    assert result['Delta'].tolist() == [-1]

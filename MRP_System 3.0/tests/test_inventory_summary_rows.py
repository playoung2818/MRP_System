import pandas as pd
import pytest

from mrp_system.ingest import sources
from mrp_system.normalize.inventory_rows import exclude_inventory_summary_rows
from mrp_system.transform.inventory import transform_inventory


@pytest.mark.parametrize('column', ['Unnamed: 0', 'Part_Number'])
def test_inventory_summary_labels_removed_without_changing_items(column):
    frame = pd.DataFrame({column: ['CPU', ' Total CPU ', 'TOTAL RAM', 'Total',
        'Subtotal SSD', 'Sub Total Memory', 'Grand Total', 'Grand Total Inventory',
        'TOTALIZER-01', 'CPU-TOTAL-01', None], 'On Hand': range(11)})
    original = frame.copy(deep=True)
    result = exclude_inventory_summary_rows(frame)
    assert result[column].tolist() == ['CPU', 'TOTALIZER-01', 'CPU-TOTAL-01', None]
    pd.testing.assert_frame_equal(frame, original)
    pd.testing.assert_frame_equal(exclude_inventory_summary_rows(result), result)


def test_inventory_transform_also_filters_direct_inputs():
    result = transform_inventory(pd.DataFrame({'Part_Number': ['CPU', 'Total CPU'],
                                               'On Hand': [4, 400]}))
    assert result['Part_Number'].tolist() == ['CPU']
    assert result['On Hand'].sum() == 4


def test_extract_filters_inventory_only(monkeypatch):
    def read_csv(path, **kwargs):
        if path == str(sources.WAREHOUSE_INV_FILE):
            return pd.DataFrame({'Unnamed: 0': ['CPU', 'Total CPU']})
        return pd.DataFrame({'Item': ['Total unchanged in other inputs']})
    monkeypatch.setattr(pd, 'read_csv', read_csv)
    monkeypatch.setattr(sources, 'read_excel_safe', lambda _: pd.DataFrame())
    so, inventory, _, pod = sources.extract_inputs()
    assert inventory['Unnamed: 0'].tolist() == ['CPU']
    assert len(so) == len(pod) == 1

"""Remove report summary rows before inventory data enters ETL calculations."""
import pandas as pd


def exclude_inventory_summary_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Match summary labels, not part names that merely contain 'total'."""
    column = 'Part_Number' if 'Part_Number' in frame.columns else 'Unnamed: 0'
    if column not in frame.columns:
        return frame.copy()
    labels = frame[column].astype('string').str.strip()
    summary = labels.str.match(r'(?i)^(?:grand\s+total|sub\s*total|total)\b', na=False)
    return frame.loc[~summary].copy()

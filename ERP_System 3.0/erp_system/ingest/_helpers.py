from __future__ import annotations

import os
import subprocess
import tempfile
import uuid
from io import BytesIO
from pathlib import Path

import pandas as pd

try:
    import gspread
    from gspread_dataframe import set_with_dataframe
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    gspread = None
    set_with_dataframe = None
    ServiceAccountCredentials = None

from erp_system.runtime.config import GOOGLE_SHEETS_CRED_PATH


def _copy_via_powershell(src: str, dst: str) -> None:
    safe_src = src.replace("'", "''")
    safe_dst = dst.replace("'", "''")
    ps_cmd = f"Copy-Item -LiteralPath '{safe_src}' -Destination '{safe_dst}' -Force"
    res = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        raise OSError(
            f"PowerShell copy failed for Excel fallback: {src} -> {dst}; stderr={res.stderr.strip()}"
        )


def read_excel_safe(path: str | Path, **kwargs) -> pd.DataFrame:
    path_str = str(path)
    try:
        return pd.read_excel(path_str, **kwargs)
    except OSError as exc:
        if exc.errno != 22:
            raise

    tmp_zip = os.path.join(tempfile.gettempdir(), f"excel_fallback_{uuid.uuid4().hex}.zip")
    try:
        _copy_via_powershell(path_str, tmp_zip)
        with open(tmp_zip, "rb") as f:
            data = f.read()
        return pd.read_excel(BytesIO(data), **kwargs)
    finally:
        try:
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
        except Exception:
            pass


def _google_cred_candidates(explicit_path: str | None = None) -> list[str]:
    candidates: list[str] = []

    def _add(path_value: str | os.PathLike[str] | None) -> None:
        if not path_value:
            return
        path_str = str(path_value).strip().strip('"').strip("'")
        if path_str and path_str not in candidates:
            candidates.append(path_str)

    _add(explicit_path)
    _add(GOOGLE_SHEETS_CRED_PATH)
    _add(os.getenv("GOOGLE_SHEETS_CRED_PATH"))
    return candidates


def _resolve_google_cred_path(explicit_path: str | None = None) -> str:
    candidates = _google_cred_candidates(explicit_path)
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    searched = "\n".join(f"  - {path}" for path in candidates) or "  - <none>"
    raise FileNotFoundError(
        "Google Sheets credential file was not found. Set GOOGLE_SHEETS_CRED_PATH in .env to a valid service-account JSON file.\n"
        f"Searched:\n{searched}"
    )


def _reset_gsheet_user_format(ws) -> None:
    try:
        ws.batch_format(
            [
                {
                    "range": None,
                    "format": {
                        "backgroundColor": None,
                        "textFormat": {
                            "foregroundColor": None,
                            "bold": False,
                            "italic": False,
                            "underline": False,
                            "strikethrough": False,
                        },
                    },
                }
            ]
        )
    except Exception:
        pass


__all__ = [
    "ServiceAccountCredentials",
    "_copy_via_powershell",
    "_reset_gsheet_user_format",
    "_resolve_google_cred_path",
    "gspread",
    "read_excel_safe",
    "set_with_dataframe",
]

# tools/emit_json.py
# Robust single-month emitter for RBI ATM/POS/Card monthly Excel files.
# Usage:
#   python tools/emit_json.py "<path_to_excel>" "<year>" "<month>"
#
# Outputs:
#   docs/data/YYYY-MM.json            # cleaned per-month data
#   docs/data/index.json              # list of months available
#   docs/data/banks.json              # unique sorted bank names

from __future__ import annotations
import json
import os
import re
import sys
from pathlib import Path
from typing import Tuple, List, Dict

import pandas as pd

DATA_DIR = Path("docs/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------
# Excel reading (engine-safe)
# --------------------------
def read_any_excel(path: str) -> pd.DataFrame:
    """Read .xlsx with openpyxl and .xls with xlrd; fail if HTML slipped in."""
    ext = os.path.splitext(path.lower())[1]
    if ext == ".html":
        raise ValueError("Downloaded HTML instead of Excel (check downloader headers).")
    if ext == ".xlsx":
        return pd.read_excel(path, engine="openpyxl", header=None)
    if ext == ".xls":
        return pd.read_excel(path, engine="xlrd", header=None)
    # Fallback sniffing
    try:
        return pd.read_excel(path, engine="openpyxl", header=None)
    except Exception:
        return pd.read_excel(path, engine="xlrd", header=None)

# --------------------------
# Header detection utilities
# --------------------------
def find_header_start(path: str, preview_rows: int = 15) -> int:
    """Find the row index where the header block starts by scanning for 'Bank Name' token (case-insensitive)."""
    preview = read_any_excel(path).head(preview_rows)
    for i in range(len(preview)):
        row = preview.iloc[i].astype(str).str.lower()
        if row.str.contains("bank name", case=False, regex=False).any():
            return i
    # Some files put 'Name of the bank' or similar
    for i in range(len(preview)):
        row = preview.iloc[i].astype(str).str.lower()
        if row.str.contains("name of the bank", case=False, regex=False).any():
            return i
    raise ValueError("Could not locate header start (no 'Bank Name' row found).")

def build_column_names(path: str, header_row_idx: int, header_depth: int = 3) -> List[str]:
    """
    Read a few rows from the header start and build a flattened header by forward-filling
    and joining with spaces, then normalizing to lowercase.
    """
    hdr = read_any_excel(path).iloc[header_row_idx : header_row_idx + header_depth]
    hdr = hdr.ffill(axis=1).astype(str)
    combined = hdr.apply(lambda col: " ".join([x.strip() for x in col if str(x).strip()]), axis=0)
    combined = combined.fillna("").astype(str).str.strip().str.lower()
    return combined.to_list()

def normalize_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Ensure df has the same number of columns as header list."""
    if len(cols) > len(df.columns):
        cols = cols[: len(df.columns)]
    elif len(cols) < len(df.columns):
        cols = cols + [f"extra_col_{i}" for i in range(len(df.columns) - len(cols))]
    df.columns = cols
    return df

# --------------------------
# Column pickers
# --------------------------
def pick_bank_col(columns: List[str]) -> str:
    candidates = [
        c for c in columns
        if "bank name" in c or "name of the bank" in c or c.strip() == "bank"
    ]
    if not candidates:
        raise ValueError("Could not find 'Bank Name' column.")
    # Prefer the shortest/cleanest match
    return sorted(candidates, key=len)[0]

def pick_credit_col(columns: List[str]) -> str:
    """
    RBI headers vary, we target the 'Credit Cards Outstanding' measure.
    """
    # Common markers
    keys = ["credit card", "credit cards"]
    # Prefer 'outstanding' over 'issued' or other sub-metrics
    def score(col: str) -> Tuple[int, int]:
        c = col.lower()
        has_credit = any(k in c for k in keys)
        has_outstanding = ("outstanding" in c)
        return (int(has_credit), int(has_outstanding))
    hits = [c for c in columns if any(k in c for k in keys)]
    if not hits:
        raise ValueError("Could not find 'Credit Cards' column.")
    # Sort by (has_credit, has_outstanding) descending, then shortest
    hits.sort(key=lambda x: (score(x)[0], score(x)[1], -len(x)), reverse=True)
    return hits[0]

def pick_debit_col(columns: List[str]) -> str:
    keys = ["debit card", "debit cards"]
    def score(col: str) -> Tuple[int, int]:
        c = col.lower()
        has_debit = any(k in c for k in keys)
        has_outstanding = ("outstanding" in c)
        return (int(has_debit), int(has_outstanding))
    hits = [c for c in columns if any(k in c for k in keys)]
    if not hits:
        raise ValueError("Could not find 'Debit Cards' column.")
    hits.sort(key=lambda x: (score(x)[0], score(x)[1], -len(x)), reverse=True)
    return hits[0]

# --------------------------
# Cleaning helpers
# --------------------------
def to_number(x) -> float | int | None:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    # kill commas & spaces
    s = re.sub(r"[,\s]", "", s)
    # keep digits and decimal point
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-.", ".-"):
        return None
    try:
        if "." in s:
            v = float(s)
            return v
        v = int(s)
        return v
    except Exception:
        return None

def clean_bank_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s{2,}", " ", s)
    return s

# --------------------------
# Emit logic
# --------------------------
def parse_excel(path: str) -> pd.DataFrame:
    """
    Return a dataframe with raw content (no header). We only read once per file.
    """
    return read_any_excel(path)

def extract_month_table(path: str) -> pd.DataFrame:
    raw = parse_excel(path)
    header_row_idx = find_header_start(path)
    # Build header names from a few rows
    cols = build_column_names(path, header_row_idx, header_depth=3)
    # The table data starts AFTER the header block we used (3 rows)
    df = raw.iloc[header_row_idx + 3 : ].copy()
    df = normalize_columns(df, cols)
    # Trim completely empty rows
    df = df.dropna(how="all")
    return df

def select_core(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, str]:
    columns = list(df.columns)
    bank_col = pick_bank_col(columns)
    credit_col = pick_credit_col(columns)
    debit_col = pick_debit_col(columns)

    keep = [bank_col, credit_col, debit_col]
    core = df[keep].copy()
    core = core.dropna(subset=[bank_col])  # bank rows only
    # Sometimes there are totals rows; drop those heuristically
    mask_total = core[bank_col].astype(str).str.contains(r"\b(total|grand total)\b", case=False, na=False)
    core = core[~mask_total].copy()

    # Rename to clean names
    core.columns = ["bank_name", "credit_cards_outstanding", "debit_cards_outstanding"]

    # Clean values
    core["bank_name"] = core["bank_name"].astype(str).map(clean_bank_name)
    core["credit_cards_outstanding"] = core["credit_cards_outstanding"].map(to_number)
    core["debit_cards_outstanding"]  = core["debit_cards_outstanding"].map(to_number)

    # Drop rows where both measures are NaN
    both_nan = core["credit_cards_outstanding"].isna() & core["debit_cards_outstanding"].isna()
    core = core[~both_nan].reset_index(drop=True)
    return core, bank_col, credit_col, debit_col

def emit_month(xlsx_path: str, year: int, month: int) -> Path:
    """
    Extracts bank-wise 'Credit Cards Outstanding' and 'Debit Cards Outstanding'
    and writes to docs/data/YYYY-MM.json
    """
    df = extract_month_table(xlsx_path)
    core, bank_col, credit_col, debit_col = select_core(df)

    out_rows: List[Dict] = []
    for _, r in core.iterrows():
        out_rows.append({
            "bank": r["bank_name"],
            "credit_cards_outstanding": r["credit_cards_outstanding"],
            "debit_cards_outstanding": r["debit_cards_outstanding"],
        })

    payload = {
        "year": int(year),
        "month": int(month),
        "schema": {
            "bank": "str",
            "credit_cards_outstanding": "number|null",
            "debit_cards_outstanding": "number|null",
        },
        "source": {
            "filename": os.path.basename(xlsx_path),
            "columns_used": {
                "bank": bank_col,
                "credit": credit_col,
                "debit": debit_col,
            },
        },
        "rows": out_rows,
    }

    out_path = DATA_DIR / f"{year:04d}-{month:02d}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path

# --------------------------
# Index / Banks maintenance
# --------------------------
def rebuild_index_and_banks() -> None:
    month_files = sorted(DATA_DIR.glob("[0-9][0-9][0-9][0-9]-[0-1][0-9].json"))
    index = []
    bank_set = set()

    for p in month_files:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        y = data.get("year"); m = data.get("month")
        index.append({"file": p.name, "year": y, "month": m, "path": str(p)})
        for row in data.get("rows", []):
            b = clean_bank_name(row.get("bank", ""))
            if b:
                bank_set.add(b)

    (DATA_DIR / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    (DATA_DIR / "banks.json").write_text(json.dumps(sorted(bank_set), ensure_ascii=False, indent=2), encoding="utf-8")

# --------------------------
# CLI
# --------------------------
def main():
    if len(sys.argv) != 4:
        print("Usage: python tools/emit_json.py <xlsx_path> <year> <month>")
        sys.exit(2)

    xlsx_path = sys.argv[1]
    year = int(sys.argv[2])
    month = int(sys.argv[3])

    if not Path(xlsx_path).exists():
        raise FileNotFoundError(f"Input file not found: {xlsx_path}")

    # Emit single month
    out_path = emit_month(xlsx_path, year, month)
    print(f"Wrote {out_path}")

    # Refresh index & banks (safe, idempotent)
    rebuild_index_and_banks()
    print("Updated docs/data/index.json and docs/data/banks.json")

if __name__ == "__main__":
    main()

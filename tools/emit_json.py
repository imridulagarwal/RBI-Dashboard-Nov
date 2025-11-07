# tools/emit_json.py
import json, sys, re
from pathlib import Path
import pandas as pd

# ---------- Helpers ----------
def _safe_read_preview(path, nrows=12):
    ext = Path(path).suffix.lower()
    engine = "openpyxl" if ext == ".xlsx" else ("xlrd" if ext == ".xls" else "openpyxl")
    return pd.read_excel(path, header=None, nrows=nrows, engine=engine)

def _safe_read(path, skiprows=None, nrows=None, header=None):
    ext = Path(path).suffix.lower()
    engine = "openpyxl" if ext == ".xlsx" else ("xlrd" if ext == ".xls" else "openpyxl")
    return pd.read_excel(path, header=header, skiprows=skiprows, nrows=nrows, engine=engine)

def _snake(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s or "")).strip().lower()
    s = s.replace("’", "'")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _detect_header_row(preview: pd.DataFrame) -> int:
    for i in range(len(preview)):
        if preview.iloc[i].astype(str).str.contains("Bank Name", case=False, na=False).any():
            return i
    # fallback: look for a row with “Bank” token
    for i in range(len(preview)):
        if preview.iloc[i].astype(str).str.contains(r"\bBank\b", case=False, na=False).any():
            return i
    raise ValueError("Could not find header row with 'Bank Name'")

def _combine_header_rows(header_rows: pd.DataFrame, rows_to_stack=3) -> pd.Index:
    # forward-fill across columns (handles merged headers turning to NaN)
    multi = header_rows.iloc[:rows_to_stack].ffill(axis=1).astype(str)
    combined = multi.apply(lambda col: ' '.join([c for c in col if str(c).strip()]).strip().lower(), axis=0)
    # normalize
    combined = combined.map(_snake)
    return pd.Index(combined)

def _pick(df_cols, include, exclude=()):
    inc = [re.compile(pat, re.I) for pat in include]
    exc = [re.compile(pat, re.I) for pat in exclude]
    best, score_best = None, -1
    for c in df_cols:
        s = 0
        for r in inc:
            if r.search(c): s += 1
        for r in exc:
            if r.search(c): s -= 2
        if s > score_best:
            best, score_best = c, s
    return best

def _to_number(x):
    try:
        if pd.isna(x): return 0.0
        if isinstance(x, str): x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return 0.0

def _slug(s): return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()

BANK_NAME_MAP = {
    "STATEBANKOFINDIA": "State Bank of India",
    "SBI": "State Bank of India",
    "STATEBANK": "State Bank of India",
    "BANKOFINDIA": "Bank of India",
    "BOI": "Bank of India",
    # add more variants here over time
}

def normalize_bank(name: str) -> str:
    key = _slug(name)
    return BANK_NAME_MAP.get(key, (name or "").strip())

# ---------- Core ----------
def parse_excel_like_colab(xlsx_path: str):
    # Step 1: preview to find header row
    preview = _safe_read_preview(xlsx_path, nrows=12)
    header_row_idx = _detect_header_row(preview)

    # Step 2: stack next 3 rows to build header
    header_rows = _safe_read(xlsx_path, skiprows=header_row_idx, nrows=3, header=None)
    combined_header = _combine_header_rows(header_rows, rows_to_stack=3)

    # Step 3: read all data after those 3 header rows
    data = _safe_read(xlsx_path, skiprows=header_row_idx + 3, header=None)

    # Step 4: align header count to data columns
    if len(combined_header) > len(data.columns):
        combined_header = combined_header[:len(data.columns)]
    elif len(combined_header) < len(data.columns):
        combined_header = combined_header.tolist() + [f"extra_col_{i}" for i in range(len(data.columns) - len(combined_header))]
        combined_header = pd.Index(combined_header)

    data.columns = combined_header

    # Step 5: detect columns
    bank_col  = _pick(data.columns, include=[r"\bbank_name\b", r"\bbank\b"])
    credit_col = _pick(data.columns, include=[r"\bcredit.*card", r"credit.*cards"], exclude=[r"value|amount|pos|txn|transaction"])
    debit_col  = _pick(data.columns, include=[r"\bdebit.*card",  r"debit.*cards"],  exclude=[r"value|amount|pos|txn|transaction"])

    if not bank_col:
        raise ValueError("Could not detect bank column (looked for 'bank name').")

    # Step 6: basic filtering & cleaning
    keep_cols = [bank_col] + [c for c in [credit_col, debit_col] if c]
    df = data[keep_cols].copy()
    df = df.dropna(subset=[bank_col])
    df = df[~df[bank_col].astype(str).str.contains(r"\btotal\b", case=False, na=False)]

    # normalize names & numbers
    df.rename(columns={
        bank_col: "bank_name",
        credit_col or "": "credit_cards_outstanding",
        debit_col  or "": "debit_cards_outstanding",
    }, inplace=True)

    if "credit_cards_outstanding" in df.columns:
        df["credit_cards_outstanding"] = df["credit_cards_outstanding"].map(_to_number)
    else:
        df["credit_cards_outstanding"] = 0.0

    if "debit_cards_outstanding" in df.columns:
        df["debit_cards_outstanding"] = df["debit_cards_outstanding"].map(_to_number)
    else:
        df["debit_cards_outstanding"] = 0.0

    df["bank_name"] = df["bank_name"].astype(str).str.strip()
    return df.reset_index(drop=True)

def emit_month(xlsx_path: str, year: int, month: int):
    out_dir = Path("docs/data"); out_dir.mkdir(parents=True, exist_ok=True)

    df = parse_excel_like_colab(xlsx_path)

    # Build / update bank catalog
    banks_path = out_dir / "banks.json"
    banks = json.loads(banks_path.read_text()) if banks_path.exists() else []
    name_to_id = {b["name"]: b["id"] for b in banks}
    next_id = max([b["id"] for b in banks], default=0) + 1

    rows = []
    for _, r in df.iterrows():
        canon = normalize_bank(r["bank_name"])
        if canon not in name_to_id:
            name_to_id[canon] = next_id
            banks.append({"id": next_id, "name": canon})
            next_id += 1
        rows.append({
            "year": year,
            "month": month,
            "bank_id": name_to_id[canon],
            "credit_cards_outstanding": float(r.get("credit_cards_outstanding", 0.0)),
            "debit_cards_outstanding":  float(r.get("debit_cards_outstanding", 0.0)),
        })

    # Emit month file (always)
    fn = f"{year}-{str(month).zfill(2)}.json"
    (out_dir / fn).write_text(json.dumps(rows, indent=2))

    # Emit banks.json (sorted)
    banks.sort(key=lambda x: x["name"])
    banks_path.write_text(json.dumps(banks, indent=2))

    # Update index.json
    index_path = out_dir / "index.json"
    index = json.loads(index_path.read_text()) if index_path.exists() else []
    path = f"data/{fn}"
    if not any(x["path"] == path for x in index):
        index.append({"year": year, "month": month, "path": path})
    index.sort(key=lambda x: (x["year"], x["month"]))
    index_path.write_text(json.dumps(index, indent=2))

# ---------- CLI ----------
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python tools/emit_json.py <xlsx_path> <year> <month>")
        sys.exit(2)
    emit_month(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))

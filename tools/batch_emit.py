# tools/batch_emit.py
from __future__ import annotations
import json, re, sys
from pathlib import Path

from emit_json import emit_month  # same folder import

DL = Path("downloads")
MANIFEST = DL / "manifest.jsonl"

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12
}

def parse_year_month_from_filename(name: str):
    base = name.upper()
    m = re.search(r"ATM([A-Z]+?)(\d{4})", base)
    if m:
        mon_s, year_s = m.group(1), m.group(2)
        mon_alpha = re.match(r"[A-Z]+", mon_s)
        if mon_alpha:
            mon_name = mon_alpha.group(0)
            if mon_name in MONTHS:
                return int(year_s), MONTHS[mon_name]
    m2 = re.search(r"(\d{4})", base)
    if m2:
        year = int(m2.group(1))
        for mon_name, mon_no in MONTHS.items():
            if f"ATM{mon_name}" in base:
                return year, mon_no
    return None, None

def sniff_zip_signature(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            sig = f.read(4)
        return sig == b"PK\x03\x04"  # zip local file header
    except Exception:
        return False

def try_extract_year_month_from_excel(path: Path):
    # Very light import to avoid heavy parsing if file is not zip
    import pandas as pd
    try:
        # Read first ~10 rows from first sheet to find "Month of <Month> <YYYY>"
        df = pd.read_excel(str(path), engine="openpyxl", header=None, nrows=12)
        txt = " ".join(df.fillna("").astype(str).values.flatten().tolist()).upper()
        m = re.search(r"MONTH OF\s+([A-Z]+)\s+(\d{4})", txt)
        if m:
            mon_name, year_s = m.group(1), m.group(2)
            if mon_name in MONTHS:
                return int(year_s), MONTHS[mon_name]
    except Exception:
        pass
    return None, None

def main():
    if not MANIFEST.exists():
        print(f"No manifest at {MANIFEST}")
        sys.exit(0)

    emitted = 0
    skipped = 0

    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        p = Path(rec["path"])
        y = rec.get("year")
        m = rec.get("month")

        if not p.exists():
            print(f"Skipping (missing file): {p}")
            skipped += 1
            continue

        # Derive y/m if missing
        if not y or not m:
            fy, fm = parse_year_month_from_filename(p.name)
            if fy and fm:
                y, m = fy, fm

        # Verify XLSX bytes (zip signature); if not, skip
        if not sniff_zip_signature(p):
            print(f"Skipping (not a real XLSX): {p.name}")
            skipped += 1
            continue

        # As a last resort, parse from inside the workbook
        if not y or not m:
            ey, em = try_extract_year_month_from_excel(p)
            if ey and em:
                y, m = ey, em

        if not y or not m:
            print(f"Skipping (no year/month): {p}")
            skipped += 1
            continue

        try:
            emit_month(str(p), int(y), int(m))
            emitted += 1
        except Exception as e:
            print(f"Emit failed for {p.name}: {e}")
            skipped += 1

    print(f"Emitted: {emitted} months; Skipped: {skipped}")

if __name__ == "__main__":
    main()

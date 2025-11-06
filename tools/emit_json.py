import json, os, sys, re, calendar
import pandas as pd
from pathlib import Path

# ... keep the rest of your imports and code ...

def _read_excel_safely(path: str):
  """Read RBI Excel robustly by forcing the right engine."""
  p = Path(path)
  ext = p.suffix.lower()

  # Small sanity check: RBI sometimes returns HTML or tiny stubs if blocked
  size = p.stat().st_size
  if size < 8_000:  # <8 KB is suspicious for a real workbook
    raise RuntimeError(f"Downloaded file looks too small ({size} bytes) — likely not a real Excel. Check download step.")

  if ext == ".xlsx":
    return pd.read_excel(path, engine="openpyxl", header=None)
  if ext == ".xls":
    return pd.read_excel(path, engine="xlrd", header=None)

  # Fallback by sniffing magic bytes
  with open(path, "rb") as f:
    sig = f.read(4)
  if sig == b"PK\x03\x04":  # XLSX is a zip
    return pd.read_excel(path, engine="openpyxl", header=None)

  # Last resort: try openpyxl anyway
  return pd.read_excel(path, engine="openpyxl", header=None)

def parse_excel(path:str):
  raw = _read_excel_safely(path)  # <-- use the safe reader
  headers=[]
  for col in range(3,29):
    parts=[]
    for r in range(2,7):
      v=raw.iloc[r,col]
      if isinstance(v,str) and v.strip(): parts.append(v.strip())
    base = re.sub(r'[^A-Za-z0-9]+','_', ' '.join(parts).lower()).strip('_')
    cand=base; i=1
    while cand in headers: cand=f"{base}_{i}"; i+=1
    headers.append(cand)
  # ... rest of your existing parse_excel logic unchanged ...

# tools/download_all.py
from __future__ import annotations
import json, re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

DL = Path("downloads"); DL.mkdir(exist_ok=True)
MANIFEST = DL / "manifest.jsonl"

DOC_BASE = "https://rbidocs.rbi.org.in/rdocs/ATM/DOCs/"

MONTHS = {
    "JANUARY":1,"FEBRUARY":2,"MARCH":3,"APRIL":4,"MAY":5,"JUNE":6,
    "JULY":7,"AUGUST":8,"SEPTEMBER":9,"OCTOBER":10,"NOVEMBER":11,"DECEMBER":12
}

def parse_year_month_from_filename(name: str):
    up = name.upper()
    # Patterns like ATMSEPTEMBER2025..., ATMMAY23062025..., etc.
    m = re.search(r"ATM([A-Z]+?)(\d{4})", up)
    if m:
        mon_token, year_s = m.group(1), m.group(2)
        mon_alpha = re.match(r"[A-Z]+", mon_token)
        if mon_alpha:
            mon_name = mon_alpha.group(0)
            if mon_name in MONTHS:
                return int(year_s), MONTHS[mon_name]
    # Fallback: find any year and month token
    m2 = re.search(r"(\d{4})", up)
    if m2:
        year = int(m2.group(1))
        for mon_name, mon_no in MONTHS.items():
            if f"ATM{mon_name}" in up:
                return year, mon_no
    return None, None

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(180_000)

        # Fetch the directory listing HTML directly
        resp = page.request.get(DOC_BASE)
        if not resp.ok:
            raise RuntimeError(f"Failed to fetch {DOC_BASE}: {resp.status} {resp.status_text()}")
        html = resp.text()
        soup = BeautifulSoup(html, "lxml")

        # Collect all ATM*.XLSX links
        hrefs = set()
        for a in soup.select('a[href]'):
            href = a["href"]
            if not href:
                continue
            # Normalize relative to DOC_BASE
            abs_url = urljoin(DOC_BASE, href)
            # Match file names like ATM*.XLSX (case-insensitive)
            fn = Path(urlparse(abs_url).path).name
            if re.match(r"(?i)^ATM.*\.XLSX$", fn):
                hrefs.add(abs_url)

        if not hrefs:
            print("No XLSX links found in directory listing")
            print("Directory page may be blocked or changed.")
        
        entries = []
        for url in sorted(hrefs):
            name = Path(urlparse(url).path).name
            local = (DL / name).with_suffix(".xlsx")
            print(f"Downloading: {local.name}")
            r = page.request.get(url)
            if not r.ok:
                print(f"  ❌ {r.status} {r.status_text()}")
                continue
            local.write_bytes(r.body())
            y, m = parse_year_month_from_filename(local.name)
            entries.append({
                "path": str(local),
                "year": y,
                "month": m,
                "source_url": url,
                "link_text": ""
            })

        with MANIFEST.open("w", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

        print(f"Saved {len(entries)} entries to {MANIFEST}")
        browser.close()

if __name__ == "__main__":
    main()

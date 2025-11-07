# tools/download_all.py
from __future__ import annotations
import json, re, time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

DL = Path("downloads")
DL.mkdir(exist_ok=True)
MANIFEST = DL / "manifest.jsonl"

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12
}

def parse_year_month_from_filename(name: str):
    """
    RBI filenames typically look like:
      ATMSEPTEMBER2025<GUID>.XLSX
      ATMMAY23062025<GUID>.XLSX (sometimes includes day)
    This extracts MONTH and YEAR robustly.
    """
    base = name.upper()
    # Try MONTHYYYY
    m = re.search(r"ATM([A-Z]+?)(\d{4})", base)
    if m:
        mon_s, year_s = m.group(1), m.group(2)
        # Keep only the alpha leading part of the month token (e.g., MAY2306 -> MAY)
        mon_alpha = re.match(r"[A-Z]+", mon_s)
        if mon_alpha:
            mon_name = mon_alpha.group(0)
            if mon_name in MONTHS:
                return int(year_s), MONTHS[mon_name]

    # Fallback: any 4-digit year, then month token around it
    m2 = re.search(r"(\d{4})", base)
    if m2:
        year = int(m2.group(1))
        for mon_name, mon_no in MONTHS.items():
            if f"ATM{mon_name}" in base:
                return year, mon_no
    return None, None

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(180_000)

        page.goto("https://rbi.org.in/Scripts/ATMView.aspx", timeout=180_000)
        page.wait_for_load_state("domcontentloaded")

        # Click the link to the monthly list if needed
        try:
            page.locator("a:has-text('Bank-wise ATM/POS/Card Statistics')").first.click()
            page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass  # already there

        # Collect XLS/XLSX links
        links = []
        for a in page.locator("a[href]").all():
            href = a.get_attribute("href") or ""
            if href.lower().endswith((".xlsx", ".xls")):
                links.append(urljoin(page.url, href))

        entries = []
        for url in links:
            name = Path(urlparse(url).path).name
            # Normalize local filename to .xlsx (server is usually .XLSX)
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

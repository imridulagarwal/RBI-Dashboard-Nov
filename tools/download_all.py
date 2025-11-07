# tools/download_all.py
import json
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag
from playwright.sync_api import sync_playwright

BASE = "https://www.rbi.org.in/Scripts/ATMView.aspx"
DL = Path("downloads")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "January","February","March","April","May","June","July","August","September","October","November","December"]
)}
MONTH_ALIASES = {
    "sept": "september",
    "sep": "september",
    "mar": "march",
    "jan": "january",
    "feb": "february",
    "apr": "april",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "oct": "october",
    "nov": "november",
    "dec": "december",
}

def _node_text(n) -> str:
    """Return plain text for any BeautifulSoup node or None."""
    if n is None:
        return ""
    if isinstance(n, NavigableString):
        return str(n).strip()
    if isinstance(n, Tag):
        # collapse with spaces so "Month of September 2025" survives
        return n.get_text(" ", strip=True)
    return str(n).strip()

def _norm_month(word: str):
    w = re.sub(r"[^a-z]", "", (word or "").strip().lower())
    w = MONTH_ALIASES.get(w, w)
    return w if w in MONTHS else None

def _infer_year_month(link_text: str, href: str, context_text: str):
    pool = " ".join(filter(None, [link_text or "", context_text or "", href or ""]))

    # Prefer “Month YYYY”
    m = re.search(r"\b([A-Za-z]{3,9})\s+(\d{4})\b", pool, flags=re.I)
    if m:
        mn = _norm_month(m.group(1))
        yr = int(m.group(2))
        if mn:
            return yr, MONTHS[mn]

    # Try “YYYY Month”
    m = re.search(r"\b(\d{4})\s+([A-Za-z]{3,9})\b", pool, flags=re.I)
    if m:
        yr = int(m.group(1))
        mn = _norm_month(m.group(2))
        if mn:
            return yr, MONTHS[mn]

    # Try in filename e.g. ATMAUGUST2025 / ATMSEPTEMBER2025
    m = re.search(r"([A-Za-z]{3,9})(\d{4})", href, flags=re.I)
    if m:
        mn = _norm_month(m.group(1))
        yr = int(m.group(2))
        if mn:
            return yr, MONTHS[mn]

    return None, None

def main():
    DL.mkdir(exist_ok=True, parents=True)
    manifest = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(180000)
        page.goto(BASE, timeout=180000)
        page.wait_for_load_state("domcontentloaded")

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        anchors = soup.find_all("a", href=True)
        excel_links = []
        for a in anchors:
            href = a["href"]
            if not href.lower().endswith((".xls", ".xlsx")):
                continue

            abs_url = href if href.startswith("http") else urljoin(BASE, href)
            text = _node_text(a)

            # Nearby text often carries "Month YYYY"
            parent_text = _node_text(a.parent)
            next_text = _node_text(a.next_sibling)
            prev_text = _node_text(a.previous_sibling)
            context_text = " ".join(t for t in [parent_text, next_text, prev_text] if t)

            y, m = _infer_year_month(text, abs_url, context_text)
            excel_links.append({"url": abs_url, "text": text, "year": y, "month": m})

        # Deduplicate by URL
        seen = set()
        unique_links = []
        for item in excel_links:
            if item["url"] in seen:
                continue
            seen.add(item["url"])
            unique_links.append(item)

        # Download files
        for item in unique_links:
            url = item["url"]
            fname = os.path.basename(urlparse(url).path) or "rbi_data.xlsx"
            if not fname.lower().endswith((".xls", ".xlsx")):
                fname += ".xlsx"
            dest = DL / fname

            print(f"Downloading: {fname}")
            resp = context.request.get(url)
            if not resp.ok:
                print(f"  ❌ HTTP {resp.status} {resp.status_text()} — skipped")
                continue
            dest.write_bytes(resp.body())

            manifest.append({
                "path": str(dest),
                "year": item["year"],
                "month": item["month"],
                "source_url": url,
                "link_text": item["text"]
            })

        browser.close()

    # Write manifest (jsonl)
    with open(DL / "manifest.jsonl", "w", encoding="utf-8") as f:
        for row in manifest:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Saved {len(manifest)} entries to downloads/manifest.jsonl")

if __name__ == "__main__":
    main()

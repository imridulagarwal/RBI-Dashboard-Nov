# tools/download_all.py
import json
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag
from playwright.sync_api import sync_playwright

BASE = "https://rbi.org.in/Scripts/ATMView.aspx"
DL = Path("downloads")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "January","February","March","April","May","June","July","August","September","October","November","December"]
)}
ALIASES = {
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

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def node_text(n) -> str:
    if n is None: return ""
    if isinstance(n, NavigableString): return str(n).strip()
    if isinstance(n, Tag): return n.get_text(" ", strip=True)
    return str(n).strip()

def norm_month(word: str) -> str | None:
    w = re.sub(r"[^a-z]", "", (word or "").strip().lower())
    w = ALIASES.get(w, w)
    return w if w in MONTHS else None

def _infer_from_text_pool(pool: str) -> tuple[int|None, int|None]:
    # “Month YYYY”
    m = re.search(r"\b([A-Za-z]{3,9})\s+(\d{2,4})\b", pool, flags=re.I)
    if m:
        mn = norm_month(m.group(1)); yr = int(m.group(2))
        if yr < 100: yr += 2000
        if mn: return yr, MONTHS[mn]
    # “YYYY Month”
    m = re.search(r"\b(\d{2,4})\s+([A-Za-z]{3,9})\b", pool, flags=re.I)
    if m:
        yr = int(m.group(1)); mn = norm_month(m.group(2))
        if yr < 100: yr += 2000
        if mn: return yr, MONTHS[mn]
    # Compact patterns in filenames like ATMAPRIL25, ATMMAY23062025, ATMJULY2025
    m = re.search(r"([A-Za-z]{3,9})[\-_]?(20)?(\d{2})\b", pool, flags=re.I)
    if m:
        mn = norm_month(m.group(1)); yy = int(m.group(3)); yr = 2000 + yy
        if mn: return yr, MONTHS[mn]
    # Also catch MONTHYYYY anywhere (e.g., JULY2025)
    m = re.search(r"([A-Za-z]{3,9})\s*[-_]?(\d{4})\b", pool, flags=re.I)
    if m:
        mn = norm_month(m.group(1)); yr = int(m.group(2))
        if mn: return yr, MONTHS[mn]
    return None, None

def infer_year_month(link_text: str, href: str, context_text: str):
    pool = " ".join(filter(None, [
        link_text or "", context_text or "",
        os.path.basename(urlparse(href).path), href
    ]))
    return _infer_from_text_pool(pool)

def looks_like_xlsx_bytes(b: bytes) -> bool:
    # XLSX starts with ZIP magic: PK\x03\x04
    return len(b) >= 4 and b[:2] == b"PK"

def detect_ext_from_headers(h: dict, fallback: str) -> str:
    ct = (h.get("content-type") or h.get("Content-Type") or "").lower()
    if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ct:
        return ".xlsx"
    if "application/vnd.ms-excel" in ct:
        return ".xls"
    if "text/html" in ct:
        return ".html"
    return fallback

def main():
    DL.mkdir(exist_ok=True, parents=True)
    manifest = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA, extra_http_headers={
            "Referer": BASE,
        })
        page = context.new_page()
        page.set_default_timeout(180000)
        page.goto(BASE, timeout=180000)
        page.wait_for_load_state("domcontentloaded")

        soup = BeautifulSoup(page.content(), "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith((".xls", ".xlsx")):
                continue
            abs_url = href if href.startswith("http") else urljoin(BASE, href)
            txt = node_text(a)
            ctx = " ".join(t for t in [
                node_text(a.parent), node_text(a.previous_sibling), node_text(a.next_sibling)
            ] if t)
            y, m = infer_year_month(txt, abs_url, ctx)
            links.append({"url": abs_url, "text": txt, "year": y, "month": m})

        # de-dup
        seen = set(); uniq = []
        for it in links:
            if it["url"] in seen: continue
            seen.add(it["url"]); uniq.append(it)

        # download with headers; validate bytes; adjust extension
        for it in uniq:
            url = it["url"]
            r = context.request.get(url, headers={"Referer": BASE, "User-Agent": UA})
            if not r.ok:
                print(f"❌ HTTP {r.status} {r.status_text()} — {url}")
                continue
            body = r.body()
            # suggested base filename
            base = os.path.basename(urlparse(url).path) or "rbi_data"
            base_no_ext = os.path.splitext(base)[0]
            # pick extension from headers, then content
            ext = detect_ext_from_headers(r.headers, os.path.splitext(base)[1] or ".xlsx")
            if ext.lower() == ".xlsx" and not looks_like_xlsx_bytes(body):
                # probably HTML or XLS served with wrong content-type
                if b"<html" in body[:200].lower():
                    ext = ".html"
                else:
                    ext = ".xls"  # RBI sometimes serves old Excel binaries
            dest = DL / (base_no_ext + ext.lower())
            dest.write_bytes(body)

            # if we failed to infer year/month earlier, try again from filename we just decided
            if it["year"] is None or it["month"] is None:
                y2, m2 = _infer_from_text_pool(dest.name)
                if y2 and m2:
                    it["year"], it["month"] = y2, m2

            manifest.append({
                "path": str(dest),
                "year": it["year"],
                "month": it["month"],
                "source_url": url,
                "link_text": it["text"]
            })
            print(f"Downloading: {dest.name}")

        browser.close()

    with open(DL / "manifest.jsonl", "w", encoding="utf-8") as f:
        for row in manifest:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Saved {len(manifest)} entries to downloads/manifest.jsonl")

if __name__ == "__main__":
    main()

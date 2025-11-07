# tools/batch_emit.py
import json
from pathlib import Path
from tools.emit_json import emit_month  # re-use your emitter

MANIFEST = Path("downloads/manifest.jsonl")

def main():
    if not MANIFEST.exists():
        raise SystemExit("manifest.jsonl not found — run tools/download_all.py first")

    emitted = 0
    skipped = 0

    with MANIFEST.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            xlsx = rec["path"]
            y = rec.get("year")
            m = rec.get("month")
            if not y or not m:
                # you can add a fallback here if you prefer:
                # e.g., infer from file content or filename more aggressively
                print(f"Skipping (no year/month): {xlsx}")
                skipped += 1
                continue
            try:
                emit_month(xlsx, int(y), int(m))
                emitted += 1
            except Exception as e:
                print(f"Emit failed for {xlsx}: {e}")
                skipped += 1

    print(f"Emitted: {emitted} months; Skipped: {skipped}")

if __name__ == "__main__":
    main()

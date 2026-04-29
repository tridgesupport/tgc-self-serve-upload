#!/usr/bin/env python3
"""
One-time import: Google Sheet CSV → Supabase products table
============================================================
Reads the existing content_template CSV and inserts all valid rows
directly into Supabase as approved products.

Usage:
    python3 import_csv_to_supabase.py
    python3 import_csv_to_supabase.py --dry-run        # preview without writing
    python3 import_csv_to_supabase.py --csv /path/to/file.csv
"""

import argparse
import csv
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

DEFAULT_CSV = Path(__file__).parent.parent.parent.parent / \
    "Downloads/Assets for Website - content_template(2).csv"


def truthy(val: str) -> bool:
    return (val or "").strip().lower() in ("yes", "true", "1")


def parse_row(row: dict, row_index: int = 0) -> dict | None:
    """Convert one CSV row to a Supabase products record. Returns None to skip."""
    product_name = (row.get("product_name") or "").strip()
    if not product_name:
        return None  # skip placeholder rows

    # Build assets JSON array from up to 4 asset columns
    assets = []
    for i in range(1, 5):
        url  = (row.get(f"asset_{i}_url") or "").strip()
        atype = (row.get(f"asset_{i}_type") or "image").strip()
        if url:
            assets.append({"url": url, "type": atype})

    # Use first asset URL as imagekit_url
    imagekit_url = assets[0]["url"] if assets else ""

    # Map yes/no → bool, treat blank show_product as True
    show_raw = (row.get("show_product") or "").strip().lower()
    show_product = show_raw != "no"  # blank or "yes" → True

    sold_raw = (row.get("sold_out") or "").strip().lower()
    sold_out = sold_raw == "yes"

    price_str = (row.get("price") or "0").strip()

    return {
        "id":                        str(uuid.uuid4()),
        "vendor_id":                 "legacy_import",
        "vendor_brand_name":         (row.get("brand") or "").strip(),
        "title":                     product_name,
        "description":               (row.get("product_description") or "").strip(),
        "price":                     price_str,
        "assets":                    json.dumps(assets),
        "status":                    "approved",
        "imagekit_url":              imagekit_url,
        "imagekit_file_id":          None,
        # Negative index avoids clashing with real Shopify IDs (always positive)
        "shopify_product_id":        -(row_index + 1),
        # Taxonomy
        "level_1":                   (row.get("level_1") or "").strip(),
        "level_2":                   (row.get("level_2") or "").strip(),
        "level_3":                   (row.get("level_3") or "").strip(),
        "level_4":                   (row.get("level_4") or "").strip(),
        "level_5":                   (row.get("level_5") or "").strip(),
        "level_6":                   (row.get("level_6") or "").strip(),
        # Collection / editorial
        "collection_description":    (row.get("collection_description") or "").strip(),
        "collection_editorial_url":  (row.get("collection_editorial_url") or "").strip(),
        "collection_editorial_type": (row.get("collection_editorial_type") or "image").strip(),
        "is_homepage":               truthy(row.get("is_homepage", "")),
        # Catalogue flags
        "price_visible":             truthy(row.get("price_visible", "yes") or "yes"),
        "min_order_qty":             int((row.get("min_order_qty") or "1").strip() or "1"),
        "sold_out":                  sold_out,
        "show_product":              show_product,
        # Timestamps
        "approved_at":               datetime.now(timezone.utc).isoformat(),
        "created_at":                datetime.now(timezone.utc).isoformat(),
    }


def run(csv_path: Path, dry_run: bool):
    print(f"\n{BOLD}TGC CSV → Supabase Import{RESET}")
    print(f"CSV:     {CYAN}{csv_path}{RESET}")
    print(f"Mode:    {YELLOW}{'DRY RUN — nothing will be written' if dry_run else 'LIVE'}{RESET}")
    print("─" * 60)

    # ── Parse CSV ────────────────────────────────────────────────────────────
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"Rows in CSV : {len(rows)}")

    records = []
    skipped = []
    for i, row in enumerate(rows):
        rec = parse_row(row, row_index=i)
        if rec:
            records.append(rec)
        else:
            skipped.append(i + 2)  # 1-based + header

    print(f"Valid records: {GREEN}{len(records)}{RESET}")
    print(f"Skipped rows (no product name): {YELLOW}{skipped}{RESET}")

    if dry_run:
        print(f"\n{BOLD}Preview (first 3 records):{RESET}")
        for r in records[:3]:
            print(f"\n  {CYAN}{r['title']}{RESET} — {r['vendor_brand_name']}")
            print(f"  levels: {r['level_1']} / {r['level_2']} / {r['level_3']}")
            print(f"  assets: {len(json.loads(r['assets']))} | price: {r['price']}")
            print(f"  is_homepage={r['is_homepage']} price_visible={r['price_visible']} show={r['show_product']}")
        print(f"\n{YELLOW}Dry run complete — re-run without --dry-run to write to Supabase.{RESET}")
        return

    # ── Connect to Supabase ──────────────────────────────────────────────────
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print(f"{RED}✗ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env{RESET}")
        sys.exit(1)

    try:
        from supabase import create_client
        sb = create_client(url, key)
    except ImportError:
        print(f"{RED}✗ supabase package not installed — run: pip install supabase{RESET}")
        sys.exit(1)

    # ── Insert in batches of 50 ───────────────────────────────────────────────
    BATCH = 50
    inserted = 0
    errors   = []

    for start in range(0, len(records), BATCH):
        batch = records[start:start + BATCH]
        try:
            res = sb.table("products").upsert(
                batch,
                on_conflict="id",   # safe re-run: same UUIDs won't duplicate
            ).execute()
            inserted += len(res.data)
            print(f"  {GREEN}✓{RESET} Inserted rows {start + 1}–{start + len(batch)}")
        except Exception as e:
            errors.append(str(e))
            print(f"  {RED}✗{RESET} Batch {start}–{start + len(batch)} failed: {e}")

    print("\n" + "─" * 60)
    if errors:
        print(f"{RED}Completed with {len(errors)} error(s):{RESET}")
        for e in errors:
            print(f"  {RED}✗{RESET} {e}")
    else:
        print(f"{GREEN}{BOLD}✓ Import complete — {inserted} products inserted into Supabase{RESET}")
        print(f"\n{CYAN}Verify: curl https://tgc-self-serve-upload.onrender.com/api/catalogue | python3 -m json.tool{RESET}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import CSV products into Supabase")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to CSV file")
    parser.add_argument("--dry-run", action="store_true", help="Parse and preview without writing")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"{RED}✗ CSV not found: {csv_path}{RESET}")
        sys.exit(1)

    run(csv_path, dry_run=args.dry_run)

"""
AEQUITAS — Clear synthetic packages and load real CSV
Run from backend/ folder: python load_packages.py

What it does:
1. Deletes ALL packages with source="seed" or source="synthetic"
   OR with no source field (old seeded data)
2. Also deletes packages for a specific warehouse if you want a clean slate
3. Loads your real CSV into MongoDB for the correct warehouse + date
"""

import csv
import random
from datetime import datetime, timedelta
from db import get_db

db = get_db()

# ── CONFIG — edit these ────────────────────────────────────────────────────────
CSV_FILE     = r"C:\Users\AMIRTHA SHREE L\Downloads\packages_WH004_2026-03-29.csv"
WAREHOUSE_ID = "WH001"          # Change to your admin's warehouse ID
DELIVERY_DATE = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
# ──────────────────────────────────────────────────────────────────────────────

CATEGORY_MAP = {
    "electronics":  "Electronics",
    "clothing":     "Clothing",
    "books":        "Books",
    "medicines":    "Medicines",
    "groceries":    "Groceries",
    "appliances":   "Appliances",
    "documents":    "Documents",
    "footwear":     "Footwear",
    "cosmetics":    "Cosmetics",
    "auto parts":   "Auto Parts",
    "fragile":      "Other",
    "general":      "Other",
    "heavy":        "Other",
    "other":        "Other",
}


def normalize_category(val):
    return CATEGORY_MAP.get(val.strip().lower(), "Other")


def parse_bool(val):
    return str(val).strip().lower() in ("true", "1", "yes")


def generate_package_id():
    return "PKG" + str(random.randint(100000, 999999))


# ── Step 1: Delete synthetic / seeded packages for this warehouse ─────────────
print(f"Deleting synthetic packages for warehouse {WAREHOUSE_ID}…")
result = db.packages.delete_many({
    "warehouse_id": WAREHOUSE_ID,
    "$or": [
        {"source": {"$in": ["seed", "synthetic", "seeded"]}},
        {"source": {"$exists": False}},
        {"source": ""},
    ]
})
print(f"  Deleted {result.deleted_count} synthetic package(s).\n")

# ── Step 2: Load CSV ───────────────────────────────────────────────────────────
print(f"Loading CSV: {CSV_FILE}")
print(f"Target warehouse : {WAREHOUSE_ID}")
print(f"Delivery date    : {DELIVERY_DATE}\n")

inserted = 0
skipped  = 0
errors   = []

with open(CSV_FILE, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader, start=2):
        row = {k.strip(): v.strip() for k, v in row.items() if k}

        recipient_name = row.get("recipient_name", "").strip()
        address        = row.get("address", "").strip()
        subarea        = row.get("subarea", "").strip()

        if not recipient_name or not address or not subarea:
            errors.append(f"Row {i}: missing required field")
            skipped += 1
            continue

        try:
            weight_kg = float(row.get("weight_kg", 0) or 0)
        except ValueError:
            errors.append(f"Row {i}: invalid weight_kg")
            skipped += 1
            continue

        try:
            floor = int(row.get("floor", 0) or 0)
        except ValueError:
            floor = 0

        category   = normalize_category(row.get("category", "Other") or "Other")
        time_window = row.get("time_window", "").strip()
        fragile    = parse_bool(row.get("fragile",  "false"))
        has_lift   = parse_bool(row.get("has_lift", "false"))
        is_gated   = parse_bool(row.get("is_gated", "false"))
        phone      = row.get("recipient_phone", "").strip()
        csv_pkg_id = row.get("package_id", "").strip()
        package_id = csv_pkg_id if csv_pkg_id else generate_package_id()

        package = {
            "package_id":      package_id,
            "recipient_name":  recipient_name,
            "recipient_phone": phone,
            "address":         address,
            "subarea":         subarea,
            "warehouse_id":    WAREHOUSE_ID,
            "delivery_date":   DELIVERY_DATE,
            "category":        category,
            "weight_kg":       weight_kg,
            "fragile":         fragile,
            "floor":           floor,
            "has_lift":        has_lift,
            "is_gated":        is_gated,
            "time_window":     time_window,
            "status":          "pending",
            "assigned_to":     None,
            "cluster_id":      None,
            "assignment_id":   None,
            "lat":             None,
            "lng":             None,
            "created_at":      datetime.utcnow().isoformat() + "Z",
            "source":          "csv_upload",
        }

        db.packages.insert_one(package)
        inserted += 1
        print(f"  ✅ {package_id} — {recipient_name} → {subarea}")

print(f"\n{'─'*50}")
print(f"Done.")
print(f"  Inserted : {inserted}")
print(f"  Skipped  : {skipped}")
if errors:
    print(f"  Errors   :")
    for e in errors:
        print(f"    {e}")
print(f"\nRefresh packages.html to see your data.")

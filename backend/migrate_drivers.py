"""
AEQUITAS — Driver warehouse_id migration
Run once from backend/ folder: python migrate_drivers.py

What it does:
- Shows all drivers missing warehouse_id
- Lets you assign warehouse_id to each one interactively
- Skips drivers that already have warehouse_id set
"""

from db import get_db

db = get_db()

# ── Show available warehouses (from admins collection) ────────────────────────
print("=== AVAILABLE WAREHOUSES (from admins) ===")
warehouses = db.admins.distinct("warehouse_id")
for w in sorted(warehouses):
    admin = db.admins.find_one({"warehouse_id": w}, {"name": 1, "username": 1})
    print(f"  {w}  →  admin: {admin.get('name')} (@{admin.get('username')})")

print()

# ── Find drivers missing warehouse_id ─────────────────────────────────────────
orphan_drivers = list(db.drivers.find({
    "$or": [
        {"warehouse_id": {"$exists": False}},
        {"warehouse_id": ""},
        {"warehouse_id": None},
    ]
}))

if not orphan_drivers:
    print("✅ All drivers already have a warehouse_id. Nothing to migrate.")
    exit()

print(f"=== {len(orphan_drivers)} DRIVER(S) MISSING warehouse_id ===\n")

for driver in orphan_drivers:
    print(f"Driver : {driver.get('name')}  |  Phone: {driver.get('phone')}  |  ID: {driver['_id']}")
    wh = input(f"  Enter warehouse_id for this driver (or SKIP to leave blank): ").strip()

    if wh.upper() == "SKIP" or wh == "":
        print("  ⚠️  Skipped.\n")
        continue

    if wh not in warehouses:
        confirm = input(f"  ⚠️  '{wh}' has no admin registered. Assign anyway? (y/n): ").strip().lower()
        if confirm != "y":
            print("  Skipped.\n")
            continue

    db.drivers.update_one(
        {"_id": driver["_id"]},
        {"$set": {"warehouse_id": wh}}
    )
    print(f"  ✅ Set warehouse_id = {wh}\n")

print("\n=== DONE — Updated driver list ===")
for d in db.drivers.find({}, {"name": 1, "warehouse_id": 1, "phone": 1}):
    print(f"  {d.get('name'):20}  phone: {d.get('phone'):15}  warehouse: {d.get('warehouse_id', '❌ MISSING')}")
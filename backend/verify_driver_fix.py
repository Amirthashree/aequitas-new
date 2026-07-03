"""
verify_driver_fix.py — one-off check that new driver registrations
are storing max_single_route_difficulty correctly (not max_difficulty).

Run from the backend/ folder:
    python verify_driver_fix.py
"""
from db import get_db

db = get_db()

# Most recently created driver
latest = db.drivers.find_one(sort=[("created_at", -1)])

if not latest:
    print("No drivers found in the database.")
else:
    print("Most recently created driver:")
    print(f"  name:                         {latest.get('name')}")
    print(f"  created_at:                   {latest.get('created_at')}")
    print(f"  capacity_tier:                {latest.get('capacity_tier')}")
    print(f"  max_single_route_difficulty:  {latest.get('max_single_route_difficulty')}")
    print(f"  max_difficulty (legacy field):{latest.get('max_difficulty')}")
    print()
    if latest.get("max_single_route_difficulty") is not None:
        print("PASS: max_single_route_difficulty is set correctly.")
    else:
        print("FAIL: max_single_route_difficulty is missing — fix did not take effect.")

# Count how many drivers still only have the legacy field
legacy_count = db.drivers.count_documents({
    "max_difficulty": {"$exists": True},
    "max_single_route_difficulty": {"$exists": False}
})
total = db.drivers.count_documents({})
print()
print(f"Drivers with ONLY the legacy 'max_difficulty' field: {legacy_count} / {total}")
if legacy_count > 0:
    print("These are pre-fix registrations — the fallback in balancer.py/cluster.py")
    print("handles them for now, but a migration script can clean this up permanently.")

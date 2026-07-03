"""
verify_driver_fix.py — one-off check that new driver registrations
are storing max_single_route_difficulty correctly (not max_difficulty).

Sorts by _id (not created_at) because created_at is stored inconsistently
across the collection — some records have it as a string (.isoformat()),
others as a native BSON date. Mixed types break sort-by-created_at, since
MongoDB sorts by BSON type before value. ObjectId embeds a real creation
timestamp and sorts correctly regardless.

Run from the backend/ folder:
    python verify_driver_fix.py
"""
from db import get_db

db = get_db()

# Most recently created driver, by _id (reliable) not created_at (mixed types)
latest = db.drivers.find_one(sort=[("_id", -1)])

if not latest:
    print("No drivers found in the database.")
else:
    gen_time = latest["_id"].generation_time
    print("Most recently created driver (by _id):")
    print(f"  name:                         {latest.get('name')}")
    print(f"  _id generation_time:          {gen_time}")
    print(f"  created_at field (raw):       {latest.get('created_at')!r}")
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

# Bonus: flag the created_at type inconsistency directly
string_dates = db.drivers.count_documents({"created_at": {"$type": "string"}})
date_dates   = db.drivers.count_documents({"created_at": {"$type": "date"}})
print()
print(f"created_at stored as string: {string_dates} / {total}")
print(f"created_at stored as native date: {date_dates} / {total}")
if string_dates > 0 and date_dates > 0:
    print("NOTE: created_at has mixed types across the collection.")
    print("Any query that sorts by created_at will be unreliable until this is normalized.")

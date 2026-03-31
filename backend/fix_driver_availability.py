"""
fix_driver_availability.py — Sets available_dates for all active drivers
so the pipeline can assign packages to them.

Run from backend/:  python fix_driver_availability.py
Run this BEFORE clicking "Assign All" in packages.html.
"""

from db import get_db
from datetime import datetime, timedelta

def fix_availability():
    db = get_db()

    # Set availability for today AND the next 7 days so you don't need to re-run often
    today = datetime.utcnow().date()
    dates = [(today + timedelta(days=i)).isoformat() for i in range(8)]

    print(f"Setting available_dates for: {dates}")

    result = db.drivers.update_many(
        {'active': True},
        {'$addToSet': {'available_dates': {'$each': dates}}}
    )

    print(f"✅ Updated {result.modified_count} drivers.")
    print("Now go to packages.html and click 'Assign All'.")

if __name__ == "__main__":
    fix_availability()
    
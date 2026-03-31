"""
wipe_packages.py — Run once to clear synthetic/seeded packages.
Place in backend/ and run: python wipe_packages.py
"""

from db import get_db

def wipe_synthetic_packages():
    db = get_db()

    # Count before
    total_before = db.packages.count_documents({})
    print(f"Packages before wipe: {total_before}")

    # Delete everything (wipe all packages so your CSV upload starts fresh)
    result = db.packages.delete_many({})
    print(f"Deleted {result.deleted_count} packages.")

    # Also clear assignments so stale assignments don't block pipeline
    assign_result = db.assignments.delete_many({})
    print(f"Deleted {assign_result.deleted_count} assignments.")

    print("\n✅ Done. Now upload your CSV from packages.html.")

if __name__ == "__main__":
    confirm = input("This will DELETE all packages and assignments. Type YES to confirm: ")
    if confirm.strip() == "YES":
        wipe_synthetic_packages()
    else:
        print("Aborted.")
        
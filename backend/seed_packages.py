"""
AEQUITAS v3.0 — Synthetic Package Data Loader
Run once to populate MongoDB with ~50 realistic Chennai delivery packages.

Usage:
  cd backend
  python seed_packages.py

Inserts into: aequitas.packages
Clears existing packages for today + tomorrow before inserting.
"""

import random
from datetime import datetime, timedelta, timezone
from db import get_db

# ── DATE TARGETS ──────────────────────────────
today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
tomorrow  = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
dates     = [today, tomorrow]

# ── CHENNAI SUBAREAS (matches your seed.py subareas) ──────────────────────────
SUBAREAS = [
    "Anna Nagar",
    "T. Nagar",
    "Adyar",
    "Velachery",
    "Tambaram",
    "Porur",
    "Chromepet",
    "Perambur",
    "Sholinganallur",
    "Mylapore",
]

# ── REALISTIC CHENNAI STREET ADDRESSES PER SUBAREA ────────────────────────────
ADDRESSES = {
    "Anna Nagar": [
        "14, 2nd Avenue, Anna Nagar West",
        "37, 9th Main Road, Anna Nagar",
        "Block C, Shanthi Colony, Anna Nagar East",
        "22, Shree Nagar, Anna Nagar West",
        "Plot 5, 11th Cross Street, Anna Nagar",
    ],
    "T. Nagar": [
        "18, Thanikachalam Road, T. Nagar",
        "4/2, Venkatnarayana Road, T. Nagar",
        "7, Kodambakkam High Road, T. Nagar",
        "31, South Usman Road, T. Nagar",
        "12, GN Chetty Road, T. Nagar",
    ],
    "Adyar": [
        "3, Gandhi Nagar 1st Street, Adyar",
        "22, Kasturba Nagar, Adyar",
        "10, Lattice Bridge Road, Adyar",
        "5A, Nehru Nagar, Adyar",
        "18, Warren Road, Adyar",
    ],
    "Velachery": [
        "Plot 12, Vijaya Nagar, Velachery",
        "45, 100 Feet Road, Velachery",
        "8, Taramani Link Road, Velachery",
        "22, Lake View Road, Velachery",
        "3, Bharathi Nagar, Velachery",
    ],
    "Tambaram": [
        "14, Railway Feeder Road, Tambaram",
        "7, East Tambaram Main Road",
        "22, Mudichur Road, Tambaram",
        "Plot 9, Bharathiyar Nagar, Tambaram",
        "5, Srinivasa Nagar, Tambaram",
    ],
    "Porur": [
        "18, Trunk Road, Porur",
        "4, Arcot Road, Porur",
        "11, Bharathi Street, Porur",
        "27, Kannagi Nagar, Porur",
        "6, Kumaran Colony, Porur",
    ],
    "Chromepet": [
        "33, GST Road, Chromepet",
        "7, Nehru Nagar, Chromepet",
        "14, Old Trunk Road, Chromepet",
        "2, Rajiv Gandhi Nagar, Chromepet",
        "9, Airport Road, Chromepet",
    ],
    "Perambur": [
        "12, Perambur Barracks Road",
        "5, Easa Colony, Perambur",
        "21, Rattan Bazaar, Perambur",
        "8, New Scheme Road, Perambur",
        "16, Kolathur Main Road, Perambur",
    ],
    "Sholinganallur": [
        "Plot 44, OMR, Sholinganallur",
        "12, Rajiv Gandhi Salai, Sholinganallur",
        "7, Perungudi, Sholinganallur",
        "3A, SIPCOT IT Park Road, Sholinganallur",
        "21, ECR Link Road, Sholinganallur",
    ],
    "Mylapore": [
        "14, R.K. Mutt Road, Mylapore",
        "6, Luz Church Road, Mylapore",
        "22, Kutchery Road, Mylapore",
        "9, Mandaveli Street, Mylapore",
        "4, Venkatakrishna Road, Mylapore",
    ],
}

# ── RECIPIENT NAMES ────────────────────────────
FIRST_NAMES = [
    "Arjun", "Priya", "Karthik", "Deepa", "Ravi",
    "Meena", "Suresh", "Lakshmi", "Vijay", "Anitha",
    "Balaji", "Kavitha", "Sathish", "Nithya", "Murugan",
    "Revathi", "Dinesh", "Saranya", "Arun", "Divya",
    "Shankar", "Bharathi", "Ganesh", "Sindhu", "Manoj",
]
LAST_NAMES = [
    "Kumar", "Raj", "Krishnan", "Murthy", "Rajan",
    "Venkat", "Subramanian", "Natarajan", "Pillai", "Iyer",
    "Naidu", "Reddy", "Sharma", "Bose", "Das",
]

# ── PACKAGE TYPES ──────────────────────────────
PACKAGE_TYPES = [
    ("Electronics",    2.5,  True),   # (category, base_weight_kg, fragile)
    ("Clothing",       1.0,  False),
    ("Books",          1.8,  False),
    ("Medicines",      0.5,  True),
    ("Groceries",      3.5,  False),
    ("Appliances",     8.0,  True),
    ("Documents",      0.2,  False),
    ("Footwear",       1.2,  False),
    ("Cosmetics",      0.8,  True),
    ("Auto Parts",     5.0,  False),
]

# ── PHONE PREFIXES (Chennai) ───────────────────
PHONE_PREFIXES = ["9841", "9884", "9940", "9962", "8754", "7339", "6374", "9500"]

# ── WAREHOUSE IDs (match your seed data) ──────
WAREHOUSE_IDS = ["WH001"]


def random_phone():
    prefix = random.choice(PHONE_PREFIXES)
    suffix = str(random.randint(100000, 999999))
    return prefix + suffix


def random_package_id():
    return "PKG" + str(random.randint(100000, 999999))


def build_package(target_date: str) -> dict:
    subarea   = random.choice(SUBAREAS)
    address   = random.choice(ADDRESSES[subarea])
    first     = random.choice(FIRST_NAMES)
    last      = random.choice(LAST_NAMES)
    recipient = f"{first} {last}"
    cat, base_wt, fragile = random.choice(PACKAGE_TYPES)
    weight    = round(base_wt + random.uniform(-0.3, 1.5), 2)
    weight    = max(0.1, weight)

    # Difficulty hints — used by scoring.py
    floor       = random.randint(0, 12)
    has_lift    = random.choice([True, False]) if floor > 3 else True
    is_gated    = random.choice([True, False])
    time_window = random.choice([None, "09:00-12:00", "12:00-15:00", "15:00-18:00", "18:00-21:00"])

    return {
        "package_id":     random_package_id(),
        "recipient_name": recipient,
        "recipient_phone": random_phone(),
        "address":        address,
        "subarea":        subarea,
        "warehouse_id":   random.choice(WAREHOUSE_IDS),
        "delivery_date":  target_date,
        "category":       cat,
        "weight_kg":      weight,
        "fragile":        fragile,
        "floor":          floor,
        "has_lift":       has_lift,
        "is_gated":       is_gated,
        "time_window":    time_window,
        "status":         "pending",
        "assigned_to":    None,
        "cluster_id":     None,
        "assignment_id":  None,
        "created_at":     datetime.now(timezone.utc).isoformat(),
    }


def seed_packages():
    db = get_db()
    col = db.packages

    # Clear existing packages for today + tomorrow only
    deleted = col.delete_many({"delivery_date": {"$in": dates}})
    print(f"Cleared {deleted.deleted_count} existing packages for {today} and {tomorrow}.")

    packages = []

    # Split ~50 packages: 22 today, 28 tomorrow
    for _ in range(22):
        packages.append(build_package(today))
    for _ in range(28):
        packages.append(build_package(tomorrow))

    random.shuffle(packages)

    result = col.insert_many(packages)
    print(f"\n✅ Inserted {len(result.inserted_ids)} packages successfully.")
    print(f"   Today    ({today}): 22 packages")
    print(f"   Tomorrow ({tomorrow}): 28 packages")

    # Summary by subarea
    print("\nSubarea breakdown:")
    from collections import Counter
    counts = Counter(p["subarea"] for p in packages)
    for subarea, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {subarea:<22} {count} packages")

    print("\nRun the pipeline now:")
    print("  POST http://localhost:5000/api/pipeline/run")
    print("  or hit 'Assign All' in packages.html → should cluster + assign all packages.\n")


if __name__ == "__main__":
    seed_packages()

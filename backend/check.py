from db import get_db

db = get_db()

print("=== ADMINS ===")
for a in db.admins.find({}, {"username": 1, "warehouse_id": 1, "name": 1}):
    print(a)

print("\n=== DRIVERS ===")
for d in db.drivers.find({}, {"name": 1, "warehouse_id": 1, "phone": 1}):
    print(d)
    
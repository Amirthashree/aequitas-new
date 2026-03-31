from flask import Blueprint, request, jsonify
from bson import ObjectId
from datetime import datetime, timezone, timedelta
from db import get_db

driver_bp = Blueprint("driver", __name__)


def _serialize(doc):
    """Recursively convert ObjectId and datetime fields to JSON-safe types."""
    if doc is None:
        return None
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# GET /api/driver/packages/today?driver_id=<id>
# ---------------------------------------------------------------------------
@driver_bp.route("/api/driver/packages/today", methods=["GET"])
def get_driver_packages_today():
    driver_id_str = request.args.get("driver_id", "").strip()
    if not driver_id_str:
        return jsonify({"error": "driver_id is required"}), 400

    try:
        driver_oid = ObjectId(driver_id_str)
    except Exception:
        return jsonify({"error": "Invalid driver_id format"}), 400

    db = get_db()

    driver = db.drivers.find_one({"_id": driver_oid})
    if not driver:
        return jsonify({"error": "Driver not found"}), 404

    tomorrow_str = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    today_str    =  datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── FIX: use find() not find_one() — driver may have MULTIPLE assignments
    # (one per subarea cluster). Collect ALL of them.
    assignments = list(db.assignments.find({
        "driver_id":    {"$in": [driver_id_str, driver_oid]},
        "delivery_date": {"$in": [tomorrow_str, today_str]}
    }))

    # Fallback: legacy 'date' field
    if not assignments:
        assignments = list(db.assignments.find({
            "driver_id": {"$in": [driver_id_str, driver_oid]},
            "date":      {"$in": [tomorrow_str, today_str]}
        }))

    if not assignments:
        return jsonify({
            "driver_id":        driver_id_str,
            "driver_name":      driver.get("name"),
            "warehouse_id":     driver.get("warehouse_id"),
            "date":             tomorrow_str,
            "assignment_ids":   [],
            "total_difficulty": 0,
            "packages":         []
        }), 200

    # ── Collect ALL package_ids across ALL assignments ─────────────────────
    all_package_oids  = []
    all_assignment_ids = [str(a["_id"]) for a in assignments]

    for assignment in assignments:
        for pid in assignment.get("package_ids", []):
            try:
                oid = ObjectId(str(pid)) if not isinstance(pid, ObjectId) else pid
                all_package_oids.append(oid)
            except Exception:
                pass

    # ── Fetch all packages in one query ───────────────────────────────────
    packages_cursor = db.packages.find({"_id": {"$in": all_package_oids}})
    packages = [_serialize(p) for p in packages_cursor]

    # ── Sort by route_order if stored, otherwise preserve insertion order ──
    def sort_key(p):
        ro = p.get("route_order")
        if ro is not None:
            try:
                return int(ro)
            except (TypeError, ValueError):
                pass
        # fallback: position in all_package_oids list
        try:
            return all_package_oids.index(ObjectId(p["_id"]))
        except Exception:
            return 9999

    packages.sort(key=sort_key)

    return jsonify({
        "driver_id":         driver_id_str,
        "driver_name":       driver.get("name"),
        "warehouse_id":      driver.get("warehouse_id"),
        "capacity_tier":     driver.get("capacity_tier"),
        "date":              tomorrow_str,
        "assignment_ids":    all_assignment_ids,
        "assignment_status": assignments[0].get("status", "pending"),
        "total_difficulty":  sum(a.get("total_difficulty", 0) for a in assignments),
        "packages":          packages
    }), 200


# ---------------------------------------------------------------------------
# PATCH /api/packages/<package_id>/status
# Body: { "status": "delivered" | "turned-down", "driver_id": "<id>" }
# ---------------------------------------------------------------------------
@driver_bp.route("/api/packages/<package_id>/status", methods=["PATCH"])
def update_package_status(package_id):
    data          = request.get_json(silent=True) or {}
    new_status    = data.get("status", "").strip()
    driver_id_str = data.get("driver_id", "").strip()

    allowed_statuses = {"delivered", "turned-down"}
    if new_status not in allowed_statuses:
        return jsonify({"error": f"status must be one of: {', '.join(allowed_statuses)}"}), 400

    if not driver_id_str:
        return jsonify({"error": "driver_id is required"}), 400

    try:
        driver_oid = ObjectId(driver_id_str)
    except Exception:
        return jsonify({"error": "Invalid driver_id format"}), 400

    db = get_db()

    # package_id may be a string like "PKG005" or a Mongo ObjectId string
    # Try ObjectId first, fall back to querying by string package_id field
    package = None
    package_oid = None
    try:
        package_oid = ObjectId(package_id)
        package = db.packages.find_one({"_id": package_oid})
    except Exception:
        pass

    if not package:
        # Fall back: match by string package_id field
        package = db.packages.find_one({"package_id": package_id})
        if package:
            package_oid = package["_id"]
    if not package:
        return jsonify({"error": "Package not found"}), 404

    now_iso = datetime.now(timezone.utc).isoformat()
    update_fields = {
        "status":            new_status,
        "updated_at":        now_iso,
        "updated_by_driver": driver_id_str,
    }
    if new_status == "delivered":
        update_fields["delivered_at"]   = now_iso
    elif new_status == "turned-down":
        update_fields["turned_down_at"] = now_iso

    db.packages.update_one({"_id": package_oid}, {"$set": update_fields})

    # ── Roll up ALL assignments for this driver ────────────────────────────
    tomorrow_str = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    today_str    =  datetime.now(timezone.utc).strftime("%Y-%m-%d")

    assignments = list(db.assignments.find({
        "driver_id":    {"$in": [driver_id_str, driver_oid]},
        "delivery_date": {"$in": [tomorrow_str, today_str]}
    }))
    if not assignments:
        assignments = list(db.assignments.find({
            "driver_id": {"$in": [driver_id_str, driver_oid]},
            "date":      {"$in": [tomorrow_str, today_str]}
        }))

    resolved = {"delivered", "turned-down"}

    for assignment in assignments:
        raw_ids  = assignment.get("package_ids", [])
        pkg_oids = []
        for pid in raw_ids:
            try:
                pkg_oids.append(ObjectId(str(pid)) if not isinstance(pid, ObjectId) else pid)
            except Exception:
                pass

        if not pkg_oids:
            continue

        sibling_pkgs = list(db.packages.find({"_id": {"$in": pkg_oids}}, {"status": 1}))
        all_done     = all(p.get("status") in resolved for p in sibling_pkgs)

        if all_done:
            any_delivered         = any(p.get("status") == "delivered" for p in sibling_pkgs)
            new_assignment_status = "completed" if any_delivered else "failed"
            db.assignments.update_one(
                {"_id": assignment["_id"]},
                {"$set": {"status": new_assignment_status}}
            )

    return jsonify({
        "updated":    True,
        "package_id": package_id,
        "new_status": new_status,
        "updated_at": now_iso
    }), 200

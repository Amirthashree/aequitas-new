# backend/pipeline.py
# ─────────────────────────────────────────────────────────────────────────────
# Morning pipeline — orchestrates the full daily assignment cycle.
# Called once each morning (or manually triggered by admin via POST /api/pipeline/run).
#
# Flow:
#   1. Fetch tomorrow's unassigned packages from DB
#   2. Fetch available drivers for tomorrow
#   3. Cluster packages → cluster.py
#   4. Assign clusters to drivers → balancer.py
#   5. Persist assignments to DB
#   6. Return summary
# ─────────────────────────────────────────────────────────────────────────────

from datetime import date, timedelta
from bson import ObjectId

from db import get_db
from cluster import build_clusters          # Phase 5 cluster builder
from balancer import assign_cluster         # Phase 6 fairness balancer


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _tomorrow() -> date:
    return date.today() + timedelta(days=1)


def _serialize_id(doc: dict) -> dict:
    """Convert ObjectId fields to strings so the result is JSON-safe."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        else:
            out[k] = v
    return out


# ─── Main entry point ────────────────────────────────────────────────────────

def run_morning_pipeline(
    warehouse_id: str,
    target_date: date | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Execute the full morning assignment pipeline for a given warehouse.

    Args:
        warehouse_id:  The warehouse this pipeline run belongs to.
        target_date:   Date to assign packages for. Defaults to tomorrow.
        dry_run:       If True, compute assignments but do NOT write to DB.

    Returns:
        A summary dict with status, assignments, errors, etc.
    """
    target_date = target_date or _tomorrow()
    date_str = target_date.isoformat()

    result = {
        "status": "ok",
        "date": date_str,
        "warehouse_id": warehouse_id,
        "packages_total": 0,
        "clusters_built": 0,
        "drivers_used": 0,
        "assignments": [],
        "unassigned_clusters": [],
        "errors": [],
    }

    db = get_db()

    # ── Step 1: Fetch unassigned packages for target date ─────────────────────
    # Field is delivery_date (not scheduled_date)
    try:
        packages = list(
            db.packages.find({
                "warehouse_id": warehouse_id,
                "delivery_date": date_str,                   # ← fixed
                "status": {"$in": ["pending", "unassigned"]},
            })
        )
    except Exception as exc:
        result["status"] = "error"
        result["errors"].append(f"Package fetch failed: {exc}")
        return result

    if not packages:
        # Debug: show what warehouse_ids and delivery_dates exist
        sample = db.packages.find_one({"delivery_date": date_str})
        if not sample:
            all_dates = db.packages.distinct("delivery_date")
            result["errors"].append(
                f"No packages found for delivery_date='{date_str}'. "
                f"Available dates in DB: {all_dates}"
            )
        else:
            result["errors"].append(
                f"Packages exist for {date_str} but none match warehouse_id='{warehouse_id}'. "
                f"Sample package warehouse_id: '{sample.get('warehouse_id')}'"
            )
        result["status"] = "ok"
        return result

    result["packages_total"] = len(packages)

    # ── Step 2: Fetch available drivers ──────────────────────────────────────
    try:
        drivers = list(
            db.drivers.find({
                "warehouse_id": warehouse_id,
                "available_dates": date_str,
                "active": True,
            })
        )
    except Exception as exc:
        result["status"] = "error"
        result["errors"].append(f"Driver fetch failed: {exc}")
        return result

    if not drivers:
        # Fall back — try any active driver for this warehouse
        drivers = list(db.drivers.find({"warehouse_id": warehouse_id, "active": True}))
        if not drivers:
            result["status"] = "error"
            result["errors"].append(
                f"No active drivers found for warehouse '{warehouse_id}'."
            )
            return result

    # ── Step 3: Build clusters ────────────────────────────────────────────────
    # build_clusters expects (packages, city_id) — derive city from warehouse_id
    city_id = warehouse_id.lower().replace('wh001', 'chennai').replace('wh', 'chennai')
    try:
        clusters = build_clusters(packages, city_id)
    except Exception as exc:
        result["status"] = "error"
        result["errors"].append(f"Clustering failed: {exc}")
        return result

    result["clusters_built"] = len(clusters)

    if not clusters:
        result["errors"].append("Clustering produced zero clusters.")
        return result

    # ── Step 4: Assign clusters to drivers ───────────────────────────────────
    try:
        from balancer import balance
        bal_result = balance(clusters, drivers)
        assignments = bal_result.get("assigned", [])
        unassigned  = bal_result.get("unassigned", [])
    except Exception as exc:
        result["status"] = "error"
        result["errors"].append(f"Balancer failed: {exc}")
        return result

    result["unassigned_clusters"] = [str(c.get("_id", c.get("cluster_id", "?"))) for c in unassigned]
    result["drivers_used"] = len({a["driver_id"] for a in assignments})

    if unassigned:
        result["errors"].append(
            f"{len(unassigned)} cluster(s) could not be assigned — insufficient driver capacity."
        )

    # ── Step 5: Persist assignments ──────────────────────────────────────────
    if not dry_run and assignments:
        persisted = []
        failed = []

        for a in assignments:
            try:
                doc = {
                    "warehouse_id":     warehouse_id,
                    "date":             date_str,
                    "delivery_date":    date_str,            # ← store both for compatibility
                    "driver_id":        a["driver_id"],
                    "cluster_id":       a.get("cluster_id"),
                    "package_ids":      [str(p["_id"]) if isinstance(p, dict) else str(p)
                                         for p in a.get("packages", [])],
                    "total_difficulty": a.get("difficulty", 0),
                    "status":           "assigned",
                    "created_at":       date_str,
                }
                res = db.assignments.insert_one(doc)

                # Mark each package as assigned
                pkg_ids = [
                    ObjectId(pid) if ObjectId.is_valid(pid) else pid
                    for pid in doc["package_ids"]
                ]
                db.packages.update_many(
                    {"_id": {"$in": pkg_ids}},
                    {"$set": {"status": "assigned", "assignment_id": str(res.inserted_id)}},
                )

                persisted.append(_serialize_id({**doc, "_id": res.inserted_id}))

            except Exception as exc:
                failed.append(str(exc))

        result["assignments"] = persisted

        if failed:
            result["errors"].extend(failed)
            if len(failed) == len(assignments):
                result["status"] = "error"

    elif dry_run:
        result["assignments"] = assignments
        result["dry_run"] = True

    return result


# ─── Convenience: re-run for a specific date (admin override) ────────────────

def rerun_pipeline(warehouse_id: str, date_str: str, dry_run: bool = False) -> dict:
    """
    Re-run the pipeline for a past or future date (admin override).
    Clears existing assignments for that date first (unless dry_run).
    """
    db = get_db()

    if not dry_run:
        # Remove existing assignments so we start clean
        db.assignments.delete_many({"warehouse_id": warehouse_id, "date": date_str})
        # Reset package status — use delivery_date (not scheduled_date)
        db.packages.update_many(
            {
                "warehouse_id": warehouse_id,
                "delivery_date": date_str,               # ← fixed
                "status": "assigned",
            },
            {"$set": {"status": "pending"}, "$unset": {"assignment_id": ""}},
        )

    try:
        target = date.fromisoformat(date_str)
    except ValueError:
        return {
            "status": "error",
            "errors": [f"Invalid date format: '{date_str}'. Use YYYY-MM-DD."],
        }

    return run_morning_pipeline(warehouse_id, target_date=target, dry_run=dry_run)

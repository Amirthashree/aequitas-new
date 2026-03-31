# backend/balancer.py
# ─────────────────────────────────────────────────────────────────────────────
# Fairness-weighted cluster → driver assignment engine.
# Called by the morning pipeline (Phase 9).
# ─────────────────────────────────────────────────────────────────────────────

from db import get_db
from scoring import is_assignable, scale_to_units
from bson import ObjectId
from datetime import date


# ── Fairness weight constants ─────────────────────────────────────────────────
W_DIFFICULTY  = 0.50   # How well the cluster fits the driver's ceiling
W_WORKLOAD    = 0.30   # Prefer drivers with fewer assigned clusters today
W_RECENCY     = 0.20   # Prefer drivers who haven't had a hard route recently


def get_active_drivers(city_id: str) -> list:
    """
    Fetch all active drivers for today in this city/warehouse.
    Accepts either a city_id or warehouse_id.
    """
    db = get_db()
    # Try warehouse_id first, fall back to city_id
    drivers = list(db.drivers.find({
        "warehouse_id": city_id,
        "active": True,
    }))
    if not drivers:
        drivers = list(db.drivers.find({
            "city_id": city_id,
            "is_active_today": True,
        }))
    return drivers


def get_driver_load_today(driver_id) -> int:
    """Count how many clusters are already assigned to this driver today."""
    db = get_db()
    today = date.today().isoformat()
    return db.assignments.count_documents({
        "driver_id": str(driver_id),
        "date": today,
    })


def get_last_hard_route_days(driver_id) -> int:
    """
    How many days since this driver last had a route with difficulty_units > 90?
    Returns 99 if no hard route found (treat as well-rested).
    """
    db = get_db()
    last = db.assignments.find_one(
        {
            "driver_id":        str(driver_id),
            "difficulty_units": {"$gt": 90},
        },
        sort=[("date", -1)],
    )
    if not last:
        return 99

    from datetime import date as dt
    last_date = dt.fromisoformat(last["date"])
    return (dt.today() - last_date).days


def fairness_score(driver: dict, cluster: dict) -> float:
    """
    Compute a fairness score for assigning this cluster to this driver.
    Higher = better match. Returns -1.0 if driver cannot take the cluster.
    """
    max_units     = driver.get("max_single_route_difficulty", 72)
    cluster_units = cluster.get("difficulty_units", 0)

    # Hard block — driver cannot take this cluster
    if cluster_units > max_units:
        return -1.0

    fit_ratio      = cluster_units / max_units if max_units else 0
    difficulty_fit = fit_ratio

    load         = get_driver_load_today(driver["_id"])
    workload_fit = 1.0 / (1.0 + load)

    days_since  = get_last_hard_route_days(driver["_id"])
    recency_fit = min(days_since / 7.0, 1.0)

    score = (
        difficulty_fit * W_DIFFICULTY +
        workload_fit   * W_WORKLOAD   +
        recency_fit    * W_RECENCY
    )
    return round(score, 4)


def sanitize(obj):
    """Recursively convert ObjectIds and other non-serializable types to strings."""
    if isinstance(obj, list):
        return [sanitize(i) for i in obj]
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj


def _write_assignment(cluster: dict, driver: dict) -> dict:
    """Write an assignment document to MongoDB and return it."""
    db    = get_db()
    today = date.today().isoformat()

    doc = {
        "date":              today,
        "driver_id":         str(driver["_id"]),
        "driver_name":       driver.get("name", ""),
        "subarea_id":        str(cluster.get("subarea_id", "")),
        "subarea_name":      cluster.get("subarea_name", ""),
        "package_count":     cluster.get("package_count", 0),
        "total_weight_kg":   cluster.get("total_weight_kg", 0),
        "route_distance_km": cluster.get("route_distance_km", 0),
        "difficulty_units":  cluster.get("difficulty_units", 0),
        "difficulty_score":  cluster.get("difficulty_score", 0),
        "breakdown":         cluster.get("breakdown", {}),
        "packages":          sanitize(cluster.get("packages", [])),
        "status":            "pending",
    }

    result         = db.assignments.insert_one(doc)
    doc["_id"]     = str(result.inserted_id)
    return doc


def balance(clusters: list, drivers) -> dict:
    """
    Main function. Assigns each cluster to the best available driver.

    Args:
        clusters:  Sorted list from cluster.build_clusters() — hardest first.
        drivers:   List of driver dicts OR a city_id/warehouse_id string (legacy).

    Returns:
        {
            "assigned":   [ { driver_id, cluster_id, difficulty, packages } ],
            "unassigned": [ cluster, ... ],
        }
    """
    # Accept either a drivers list or a city_id string (legacy support)
    if isinstance(drivers, str):
        drivers = get_active_drivers(drivers)

    if not drivers:
        return {"assigned": [], "unassigned": clusters}

    assigned   = []
    unassigned = []

    for cluster in clusters:
        best_driver = None
        best_score  = -1.0

        for driver in drivers:
            score = fairness_score(driver, cluster)
            if score > best_score:
                best_score  = score
                best_driver = driver

        if best_driver is None or best_score < 0:
            unassigned.append(cluster)
            continue

        assignment = _write_assignment(cluster, best_driver)
        assigned.append({
            "driver_id":      str(best_driver["_id"]),
            "cluster_id":     str(cluster.get("subarea_id", "")),
            "difficulty":     cluster.get("difficulty_units", 0),
            "packages":       sanitize(cluster.get("packages", [])),
            "assignment_id":  assignment["_id"],
            "driver_name":    best_driver.get("name", ""),
            "fairness_score": best_score,
        })

    return {
        "assigned":   assigned,
        "unassigned": unassigned,
    }

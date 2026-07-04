# backend/balancer.py
# ─────────────────────────────────────────────────────────────────────────────
# Fairness-weighted cluster → driver assignment engine.
# Called by the morning pipeline (Phase 9).
#
# NOTE: balance() is a pure function — it does NOT write to MongoDB.
# pipeline.py is solely responsible for persisting assignments, using one
# canonical schema. Previously this module wrote its own assignment doc per
# cluster (using date.today() and a different schema) IN ADDITION to
# pipeline.py's write for the same cluster — a double-write bug that also
# silently broke workload/recency fairness scoring, since those lookups only
# ever matched this module's own throwaway docs, not the real assignment
# history written by pipeline.py.
# ─────────────────────────────────────────────────────────────────────────────

from db import get_db
from bson import ObjectId
from datetime import date


# ── Fairness weight constants ─────────────────────────────────────────────────
W_DIFFICULTY  = 0.50   # How well the cluster fits the driver's ceiling
W_WORKLOAD    = 0.30   # Prefer drivers with fewer assigned clusters on target date
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


def get_driver_load_today(driver_id, target_date_str: str) -> int:
    """
    Count how many clusters are already assigned to this driver for
    target_date_str, against the canonical assignments schema (field "date"
    holds the delivery date being planned, not necessarily today's calendar
    date — the morning pipeline typically plans for tomorrow).
    """
    db = get_db()
    return db.assignments.count_documents({
        "driver_id": str(driver_id),
        "date":      target_date_str,
    })


def get_last_hard_route_days(driver_id, target_date_str: str) -> int:
    """
    How many days before target_date_str did this driver last have a route
    with total_difficulty > 90? Returns 99 if none found (treat as well-rested).

    Only considers assignments strictly before target_date_str, so this is
    unaffected by other clusters being assigned to the same driver within the
    current balance() run.
    """
    db = get_db()
    last = db.assignments.find_one(
        {
            "driver_id":        str(driver_id),
            "total_difficulty": {"$gt": 90},
            "date":             {"$lt": target_date_str},
        },
        sort=[("date", -1)],
    )
    if not last:
        return 99

    target    = date.fromisoformat(target_date_str)
    last_date = date.fromisoformat(last["date"])
    return (target - last_date).days


def fairness_score(driver: dict, cluster: dict, workload: dict, recency_days: dict) -> float:
    """
    Compute a fairness score for assigning this cluster to this driver.
    Higher = better match. Returns -1.0 if driver cannot take the cluster.

    workload and recency_days are precomputed per-driver dicts (keyed by
    str(driver_id)) for the current balance() run. workload is mutated by
    the caller as clusters get tentatively assigned within the run; recency
    is static for the run since it only depends on history strictly before
    target_date_str.
    """
    max_units     = driver.get("max_single_route_difficulty", driver.get("max_difficulty", 72))
    cluster_units = cluster.get("difficulty_units", 0)

    # Hard block — driver cannot take this cluster
    if cluster_units > max_units:
        return -1.0

    fit_ratio      = cluster_units / max_units if max_units else 0
    difficulty_fit = fit_ratio

    did          = str(driver["_id"])
    load         = workload.get(did, 0)
    workload_fit = 1.0 / (1.0 + load)

    days_since  = recency_days.get(did, 99)
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


def balance(clusters: list, drivers, target_date_str: str) -> dict:
    """
    Main function. Assigns each cluster to the best available driver.
    Pure function — does NOT write to MongoDB. The caller (pipeline.py)
    persists the returned assignments using one canonical schema.

    Args:
        clusters:        Sorted list from cluster.build_clusters() — hardest first.
        drivers:         List of driver dicts OR a city_id/warehouse_id string (legacy).
        target_date_str: The delivery date (YYYY-MM-DD) being planned for. Used
                          to correctly look up each driver's existing workload
                          and recency history for that specific date.

    Returns:
        {
            "assigned":   [ { driver_id, driver_name, cluster_id, subarea_name,
                               difficulty, packages, fairness_score } ],
            "unassigned": [ cluster, ... ],
        }
    """
    # Accept either a drivers list or a city_id string (legacy support)
    if isinstance(drivers, str):
        drivers = get_active_drivers(drivers)

    if not drivers:
        return {"assigned": [], "unassigned": clusters}

    # Seed per-driver state for this run. workload starts from real assignment
    # history for target_date_str and is incremented in-memory as clusters get
    # assigned within this run (so a single run still spreads load evenly,
    # without needing to write to the DB before the run completes).
    workload     = {}
    recency_days = {}
    for d in drivers:
        did               = str(d["_id"])
        workload[did]     = get_driver_load_today(d["_id"], target_date_str)
        recency_days[did] = get_last_hard_route_days(d["_id"], target_date_str)

    assigned   = []
    unassigned = []

    for cluster in clusters:
        best_driver = None
        best_score  = -1.0

        for driver in drivers:
            score = fairness_score(driver, cluster, workload, recency_days)
            if score > best_score:
                best_score  = score
                best_driver = driver

        if best_driver is None or best_score < 0:
            unassigned.append(cluster)
            continue

        did = str(best_driver["_id"])
        workload[did] = workload.get(did, 0) + 1

        assigned.append({
            "driver_id":      did,
            "driver_name":    best_driver.get("name", ""),
            "cluster_id":     str(cluster.get("subarea_id", "")),
            "subarea_name":   cluster.get("subarea_name", ""),
            "difficulty":     cluster.get("difficulty_units", 0),
            "packages":       sanitize(cluster.get("packages", [])),
            "fairness_score": best_score,
        })

    return {
        "assigned":   assigned,
        "unassigned": unassigned,
    }
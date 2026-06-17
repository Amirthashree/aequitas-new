# backend/cluster.py
# ─────────────────────────────────────────────────────────────────────────────
# Morning cluster builder.
# Groups packages by subarea, scores each cluster, and recursively splits
# any cluster that exceeds the median driver ceiling using the OSM graph.
# ─────────────────────────────────────────────────────────────────────────────

import networkx as nx
from scoring import score_cluster, scale_to_units
from db import get_db
from statistics import median

# Reference max for a single route (Tier 7 driver ceiling)
ABSOLUTE_MAX_UNITS = 126

# Default median ceiling if no drivers exist yet
FALLBACK_MEDIAN_CEILING = 72   # Tier 4 × 18

# Map warehouse_id → OSM city query string
WAREHOUSE_CITY_MAP = {
    "WH001":   "Chennai, Tamil Nadu, India",
    "WH002":   "Mumbai, Maharashtra, India",
    "chennai": "Chennai, Tamil Nadu, India",
    "mumbai":  "Mumbai, Maharashtra, India",
}


def get_median_ceiling(warehouse_id: str) -> float:
    """
    Compute the median max_single_route_difficulty across all active drivers
    in this warehouse. Falls back to FALLBACK_MEDIAN_CEILING if none found.
    """
    db = get_db()
    drivers = list(db.drivers.find(
        {"warehouse_id": warehouse_id, "active": True},
        {"max_single_route_difficulty": 1, "_id": 0}
    ))
    if not drivers:
        return FALLBACK_MEDIAN_CEILING
    ceilings = [d.get("max_single_route_difficulty", FALLBACK_MEDIAN_CEILING) for d in drivers]
    return median(ceilings)


def load_osm_graph(warehouse_id: str):
    """
    Load the OSM walk graph for the warehouse's city.
    Uses a cached .graphml file if present, otherwise downloads from OSM.
    """
    import os
    import osmnx as ox  # lazy import — only loaded when pipeline actually runs

    key = warehouse_id.lower().replace(" ", "_")
    cache_path = f"models/{key}_walk.graphml"

    if os.path.exists(cache_path):
        return ox.load_graphml(cache_path)

    query = WAREHOUSE_CITY_MAP.get(warehouse_id, "Chennai, Tamil Nadu, India")
    G = ox.graph_from_place(query, network_type="walk")
    os.makedirs("models", exist_ok=True)
    ox.save_graphml(G, cache_path)
    return G


def nearest_node(G, lat: float, lon: float) -> int:
    """Return the nearest OSM node to a lat/lon coordinate."""
    import osmnx as ox  # lazy import
    return ox.nearest_nodes(G, lon, lat)


def compute_route_distance_km(G, packages: list) -> float:
    """
    Estimate total route distance for a list of packages using OSM graph.
    Packages must have 'lat' and 'lon' fields.
    Returns distance in km.
    """
    valid_packages = [p for p in packages if p.get("lat") and p.get("lng", p.get("lon"))]
    if len(valid_packages) < 2:
        return 0.5

    nodes = [nearest_node(G, float(p["lat"]), float(p.get("lng", p.get("lon", 0)))) for p in valid_packages]
    total_meters = 0.0

    for i in range(len(nodes) - 1):
        try:
            length = nx.shortest_path_length(
                G, nodes[i], nodes[i + 1], weight="length"
            )
            total_meters += length
        except nx.NetworkXNoPath:
            total_meters += 500

    return round(total_meters / 1000, 3)


def split_cluster_by_blocks(packages: list, G, subarea: dict, depth: int = 0) -> list:
    """
    Recursively split a cluster that's too hard into block-level sub-clusters.
    Max recursion depth = 3.
    """
    if depth >= 3 or len(packages) <= 1:
        return [packages]

    node_buckets = {}
    for pkg in packages:
        try:
            if pkg.get("lat") and pkg.get("lng", pkg.get("lon")):
                node = nearest_node(G, float(pkg["lat"]), float(pkg.get("lng", pkg.get("lon", 0))))
            else:
                node = 0
        except Exception:
            node = 0
        node_buckets.setdefault(node, []).append(pkg)

    if len(node_buckets) <= 1:
        return [packages]

    sorted_nodes = sorted(node_buckets.keys())
    mid = len(sorted_nodes) // 2
    half_a, half_b = [], []
    for i, node in enumerate(sorted_nodes):
        if i < mid:
            half_a.extend(node_buckets[node])
        else:
            half_b.extend(node_buckets[node])

    return half_a, half_b


def score_package_list(packages: list, subarea: dict, G) -> dict:
    """Score a flat list of packages against a subarea document."""
    total_weight        = sum(p.get("weight_kg", 1.0) for p in packages)
    package_count       = len(packages)
    stair_density       = subarea.get("stair_density", 0.3)
    difficulty_modifier = subarea.get("difficulty_modifier", 1.0)
    route_distance_km   = compute_route_distance_km(G, packages)

    result = score_cluster(
        total_weight_kg=total_weight,
        package_count=package_count,
        stair_density=stair_density,
        route_distance_km=route_distance_km,
        difficulty_modifier=difficulty_modifier,
    )

    return {
        "subarea_id":        subarea.get("_id", ""),
        "subarea_name":      subarea.get("name", ""),
        "packages":          packages,
        "package_count":     package_count,
        "total_weight_kg":   round(total_weight, 2),
        "route_distance_km": route_distance_km,
        "raw_score":         result["raw_score"],
        "difficulty_score":  result["difficulty_score"],
        "difficulty_units":  scale_to_units(result["difficulty_score"]),
        "breakdown":         result["breakdown"],
    }


def build_clusters(packages: list, warehouse_id: str) -> list:
    """
    Main function called by the morning pipeline.

    Args:
        packages:      List of package dicts with subarea_id, lat, lon, weight_kg.
        warehouse_id:  e.g. "WH001" or "chennai"

    Returns:
        Flat list of scored cluster dicts, ready for balancer.py.
    """
    db = get_db()
    median_ceiling = get_median_ceiling(warehouse_id)
    G = load_osm_graph(warehouse_id)

    # ── Group packages by subarea ─────────────────────────────────────────────
    subarea_buckets = {}
    for pkg in packages:
        sid = str(pkg.get("subarea_id", "unknown"))
        subarea_buckets.setdefault(sid, []).append(pkg)

    # ── Fetch all relevant subarea documents ──────────────────────────────────
    from bson import ObjectId
    subarea_ids = [s for s in subarea_buckets.keys() if s != "unknown"]
    valid_oids  = []
    for i in subarea_ids:
        try:
            valid_oids.append(ObjectId(i))
        except Exception:
            pass

    subareas = {}
    if valid_oids:
        subareas = {
            str(s["_id"]): s
            for s in db.subareas.find({"_id": {"$in": valid_oids}})
        }

    final_clusters = []

    for sid, pkgs in subarea_buckets.items():
        subarea = subareas.get(sid, {"_id": sid, "name": sid})
        cluster = score_package_list(pkgs, subarea, G)

        # ── Split if cluster exceeds median driver ceiling ────────────────────
        if cluster["difficulty_units"] > median_ceiling:
            halves = split_cluster_by_blocks(pkgs, G, subarea)

            if isinstance(halves, tuple):
                for half in halves:
                    sub = score_package_list(half, subarea, G)
                    if sub["difficulty_units"] > median_ceiling and len(half) > 1:
                        sub_halves = split_cluster_by_blocks(half, G, subarea, depth=1)
                        if isinstance(sub_halves, tuple):
                            for sh in sub_halves:
                                final_clusters.append(score_package_list(sh, subarea, G))
                        else:
                            final_clusters.append(sub)
                    else:
                        final_clusters.append(sub)
            else:
                final_clusters.append(cluster)
        else:
            final_clusters.append(cluster)

    # ── Sort hardest to easiest ───────────────────────────────────────────────
    final_clusters.sort(key=lambda c: c["difficulty_units"], reverse=True)

    return final_clusters

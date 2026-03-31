# backend/scoring.py
# ─────────────────────────────────────────────────────────────────────────────
# Pure scoring logic — no database calls, no side effects.
# Imported by cluster.py and balancer.py.
# ─────────────────────────────────────────────────────────────────────────────

# Feature weights (must sum to 1.0)
WEIGHT_WEIGHT   = 0.30   # total package weight in kg
WEIGHT_COUNT    = 0.25   # number of packages
WEIGHT_STAIRS   = 0.25   # stair density of the subarea
WEIGHT_DISTANCE = 0.20   # total route distance in km


def score_cluster(
    total_weight_kg: float,
    package_count:   int,
    stair_density:   float,
    route_distance_km: float,
    difficulty_modifier: float = 1.0,
) -> dict:
    """
    Compute the difficulty score for a delivery cluster.

    Args:
        total_weight_kg:     Sum of all package weights in the cluster (kg)
        package_count:       Number of individual packages
        stair_density:       From the subarea document (0.0 – 1.0 scale)
        route_distance_km:   Total OSM route distance for the cluster (km)
        difficulty_modifier: Subarea multiplier from MongoDB (default 1.0)

    Returns:
        {
            "raw_score":        float,   ← score before modifier
            "difficulty_score": float,   ← final score after modifier
            "breakdown": {
                "weight_component":   float,
                "count_component":    float,
                "stairs_component":   float,
                "distance_component": float,
            }
        }

    Notes:
        - Inputs are normalised against reference maximums before weighting
          so that no single dimension dominates unfairly.
        - Reference maxes: weight=50kg, count=40pkgs, stairs=1.0, distance=20km
        - Scores are NOT capped — a cluster can exceed 1.0 if inputs exceed refs.
          That is intentional: it flags clusters that need splitting (Phase 5).
    """

    # ── Normalise against reference maximums ─────────────────────────────────
    REF_WEIGHT   = 50.0   # kg
    REF_COUNT    = 40     # packages
    REF_STAIRS   = 1.0    # already 0–1
    REF_DISTANCE = 20.0   # km

    norm_weight   = total_weight_kg    / REF_WEIGHT
    norm_count    = package_count      / REF_COUNT
    norm_stairs   = stair_density      / REF_STAIRS
    norm_distance = route_distance_km  / REF_DISTANCE

    # ── Weighted sum ──────────────────────────────────────────────────────────
    weight_component   = norm_weight   * WEIGHT_WEIGHT
    count_component    = norm_count    * WEIGHT_COUNT
    stairs_component   = norm_stairs   * WEIGHT_STAIRS
    distance_component = norm_distance * WEIGHT_DISTANCE

    raw_score = (
        weight_component +
        count_component  +
        stairs_component +
        distance_component
    )

    difficulty_score = round(raw_score * difficulty_modifier, 4)

    return {
        "raw_score":        round(raw_score, 4),
        "difficulty_score": difficulty_score,
        "breakdown": {
            "weight_component":   round(weight_component,   4),
            "count_component":    round(count_component,    4),
            "stairs_component":   round(stairs_component,   4),
            "distance_component": round(distance_component, 4),
        }
    }


def is_assignable(difficulty_score: float, driver_max: int) -> bool:
    """
    Quick pre-filter check used in balancer.py.
    Returns True if this cluster is within the driver's ceiling.

    Args:
        difficulty_score: Output of score_cluster()["difficulty_score"]
        driver_max:       Driver's max_single_route_difficulty from MongoDB
    """
    # Convert normalised score → difficulty units (scale to 126 max)
    scaled = difficulty_score * 126
    return scaled <= driver_max


def scale_to_units(difficulty_score: float) -> float:
    """
    Convert a normalised difficulty_score (0.0–1.0+) to difficulty units
    (0–126 scale matching driver capacity tiers).
    Used for display and for the pre-filter in balancer.py.
    """
    return round(difficulty_score * 126, 2)

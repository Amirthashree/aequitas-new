import os
import osmnx as ox
import geopandas as gpd
from shapely.geometry import mapping
from dotenv import load_dotenv
from db import get_db

load_dotenv()

# ─── Phase 1: Collection init (already done, kept for safety) ───────────────

def init_collections():
    db = get_db()
    collections = [
        "drivers", "areas", "subareas", "routes", "assignments",
        "feedback", "city_config", "pipeline_state", "deferred_packages",
    ]
    existing = db.list_collection_names()
    for name in collections:
        if name not in existing:
            db.create_collection(name)
            print(f"Created collection: {name}")
        else:
            print(f"Already exists: {name}")

    db.drivers.create_index("city_config_id")
    db.subareas.create_index("area_id")
    db.routes.create_index("subarea_id")
    db.assignments.create_index("driver_id")
    db.feedback.create_index([("driver_id", 1), ("submitted_at", -1)])
    db.pipeline_state.create_index([("city_id", 1), ("run_date", -1)])
    db.deferred_packages.create_index("original_date")
    print("All collections and indexes ready.\n")


# ─── Phase 2: OSM extraction for Chennai ────────────────────────────────────

# Chennai subareas — name, OSM search string
CHENNAI_SUBAREAS = [
    ("Anna Nagar West",      "Anna Nagar West, Chennai, India"),
    ("Sholinganallur",       "Sholinganallur, Chennai, India"),
    ("Vijaya Nagar Colony",  "Vijaya Nagar, Chennai, India"),
    ("Tambaram East",        "Tambaram, Chennai, India"),
    ("OMR Perungudi",        "Perungudi, Chennai, India"),
    ("Adyar",                "Adyar, Chennai, India"),
    ("Velachery",            "Velachery, Chennai, India"),
    ("Porur",                "Porur, Chennai, India"),
]

def compute_lane_width_category(roads_gdf):
    """Return 1 (narrow), 2 (medium), 3 (wide) from road widths."""
    if roads_gdf is None or len(roads_gdf) == 0:
        return 2
    if "lanes" not in roads_gdf.columns:
        return 2
    lane_values = []
    for val in roads_gdf["lanes"].dropna():
        try:
            # Handle comma-separated values like "1,2,3" or lists
            if isinstance(val, (list, tuple)):
                nums = [float(v) for v in val]
            elif isinstance(val, str) and "," in val:
                nums = [float(v.strip()) for v in val.split(",")]
            else:
                nums = [float(val)]
            lane_values.extend(nums)
        except (ValueError, TypeError):
            continue
    if not lane_values:
        return 2
    avg = sum(lane_values) / len(lane_values)
    if avg < 2:
        return 1
    elif avg <= 3:
        return 2
    else:
        return 3

def compute_apt_density(buildings_gdf, area_km2):
    """Buildings per sq km — proxy for apartment density."""
    if buildings_gdf is None or len(buildings_gdf) == 0 or area_km2 == 0:
        return 2.0
    return round(len(buildings_gdf) / area_km2, 2)

def compute_stair_density(buildings_gdf):
    """Average floor count from OSM building:levels tag."""
    if buildings_gdf is None or len(buildings_gdf) == 0:
        return 2.0
    if "building:levels" in buildings_gdf.columns:
        levels = buildings_gdf["building:levels"].dropna().astype(float)
        if len(levels) > 0:
            return round(levels.mean(), 2)
    return 2.0

def compute_difficulty_modifier(apt_density, stair_density, lane_width):
    """
    Combine three tags into a single modifier (0.8 – 1.5).
    Higher density + more stairs + narrower lanes = harder.
    """
    density_score = min(apt_density / 10, 1.0)      # normalise to 0-1
    stair_score   = min(stair_density / 10, 1.0)
    lane_score    = (4 - lane_width) / 3             # narrow=1 → score=1, wide=3 → score=0.33

    raw = (density_score * 0.4) + (stair_score * 0.3) + (lane_score * 0.3)
    modifier = 0.8 + (raw * 0.7)                     # scale to 0.8 – 1.5
    return round(modifier, 3)

def extract_subarea(name, query):
    """Download OSM data for one subarea and return computed tags."""
    print(f"  Extracting: {name} ...")
    try:
        # Get boundary polygon
        area_gdf = ox.geocode_to_gdf(query)
        area_km2 = area_gdf.to_crs("EPSG:32644").area.iloc[0] / 1e6

        # Get buildings
        try:
            buildings = ox.features_from_place(query, tags={"building": True})
        except Exception:
            buildings = None

        # Get roads
        try:
            G = ox.graph_from_place(query, network_type="drive")
            _, roads = ox.graph_to_gdfs(G)
        except Exception:
            roads = None

        apt_density      = compute_apt_density(buildings, area_km2)
        stair_density    = compute_stair_density(buildings)
        lane_width       = compute_lane_width_category(roads)
        diff_modifier    = compute_difficulty_modifier(apt_density, stair_density, lane_width)

        print(f"    apt_density={apt_density}, stair_density={stair_density}, "
              f"lane_width={lane_width}, modifier={diff_modifier}")

        return {
            "subarea_name":        name,
            "apt_density":         apt_density,
            "stair_density":       stair_density,
            "lane_width":          lane_width,
            "difficulty_modifier": diff_modifier,
            "area_km2":            round(area_km2, 3),
        }

    except Exception as e:
        print(f"    WARNING: Could not extract {name}: {e}")
        # Return safe defaults so the pipeline never has a missing subarea
        return {
            "subarea_name":        name,
            "apt_density":         2.0,
            "stair_density":       2.0,
            "lane_width":          2,
            "difficulty_modifier": 1.0,
            "area_km2":            0.0,
        }

def seed_chennai():
    db = get_db()

    # Upsert city_config for Chennai
    city_id = "chennai"
    db.city_config.update_one(
        {"city_id": city_id},
        {"$set": {
            "city_id":       city_id,
            "city_name":     "Chennai",
            "timezone":      "Asia/Kolkata",
            "model_path":    f"models/{city_id}_model.pkl",
            "tile_server_url": "",
            "weight_formula": {
                "weight":   0.30,
                "count":    0.25,
                "stairs":   0.25,
                "distance": 0.20,
            },
            "active_drivers_today":  0,
            "total_difficulty_today": 0,
            "daily_target_score":    0,
        }},
        upsert=True
    )
    print("city_config upserted for Chennai.")

    # Upsert parent area
    db.areas.update_one(
        {"area_id": "chennai_main"},
        {"$set": {
            "area_id":               "chennai_main",
            "area_name":             "Chennai Main",
            "city_id":               city_id,
            "base_difficulty_modifier": 1.0,
        }},
        upsert=True
    )
    print("Parent area upserted.\n")

    print("Starting OSM extraction for all subareas...")
    print("(This will take 2-5 minutes — downloading real Chennai data)\n")

    for name, query in CHENNAI_SUBAREAS:
        tags = extract_subarea(name, query)
        subarea_id = name.lower().replace(" ", "_")

        db.subareas.update_one(
            {"subarea_id": subarea_id},
            {"$set": {
                "subarea_id":          subarea_id,
                "subarea_name":        tags["subarea_name"],
                "area_id":             "chennai_main",
                "apt_density":         tags["apt_density"],
                "stair_density":       tags["stair_density"],
                "lane_width":          tags["lane_width"],
                "difficulty_modifier": tags["difficulty_modifier"],
                "area_km2":            tags["area_km2"],
            }},
            upsert=True
        )
        print(f"    Saved to MongoDB: {subarea_id}\n")

    count = db.subareas.count_documents({"area_id": "chennai_main"})
    print(f"Phase 2 complete. {count} subareas seeded for Chennai.")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_collections()
    seed_chennai()
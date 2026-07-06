"""
regenerate_osm_cache.py — rebuilds backend/models/chennai_walk.graphml as the
union of small, radius-limited networks around each actual delivery subarea,
instead of the entire city of Chennai.

Why not one bounding box: the 8 known subareas are scattered across a wide
span of the city (tens of km apart in places), so a single bounding box that
stretches to cover all of them ends up covering almost the entire metro area
anyway, plus all the empty space between subareas that's never actually
needed. Building a small network around each subarea individually and
unioning just those together avoids that.

Run from the backend/ folder, with your venv active:
    python regenerate_osm_cache.py

Requires real internet access to OpenStreetMap's geocoding/Overpass services
— this will NOT work on Render itself, only run it locally.
"""
import os
import sys
import time

sys.path.insert(0, '.')

import osmnx as ox
from cluster import get_delivery_area_graph, DELIVERY_SUBAREA_QUERIES, SUBAREA_RADIUS_METERS

CACHE_PATH = "models/chennai_walk.graphml"

print(f"Building small networks around {len(DELIVERY_SUBAREA_QUERIES['chennai'])} "
      f"known delivery subareas ({SUBAREA_RADIUS_METERS}m radius each)...\n")

old_size_mb = None
if os.path.exists(CACHE_PATH):
    old_size_mb = os.path.getsize(CACHE_PATH) / (1024 * 1024)
    print(f"Existing cache found: {old_size_mb:.1f} MB (will be overwritten)\n")

t0 = time.time()
G = get_delivery_area_graph("chennai")
elapsed = time.time() - t0
print(f"\nBuilt combined network in {elapsed:.1f}s — "
      f"{G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

os.makedirs("models", exist_ok=True)
ox.save_graphml(G, CACHE_PATH)

new_size_mb = os.path.getsize(CACHE_PATH) / (1024 * 1024)
print(f"\nSaved to {CACHE_PATH}: {new_size_mb:.1f} MB")
if old_size_mb:
    print(f"Size change: {old_size_mb:.1f} MB -> {new_size_mb:.1f} MB "
          f"({new_size_mb / old_size_mb * 100:.0f}% of original)")

print("\nDone. Commit and push this file to deploy the smaller graph to Render.")

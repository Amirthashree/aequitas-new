# backend/app.py
# ─────────────────────────────────────────────────────────────────────────────
# AEQUITAS v3.1 — Main Flask application
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
import random

from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
from bson import ObjectId
from datetime import datetime, date

load_dotenv()

from db import get_db
from capacity import get_capacity
from auth_routes import auth_bp

# ─── App setup ───────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)

# Register blueprints
app.register_blueprint(auth_bp)

from entrix_routes import entrix_bp
app.register_blueprint(entrix_bp)

from driver_routes import driver_bp
app.register_blueprint(driver_bp)

from admin_package_routes import admin_pkg_bp
app.register_blueprint(admin_pkg_bp)

from public_routes import public_bp
app.register_blueprint(public_bp)


# ─── JSON helpers ─────────────────────────────────────────────────────────────

def serialize(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")

def mongo_response(data, status=200):
    return Response(
        json.dumps(data, default=serialize),
        mimetype="application/json",
        status=status,
    )


# ─── Static frontend serving ──────────────────────────────────────────────────
# Serves tracker.html and entrix.html directly from Flask so any device
# on the same WiFi can reach both the API and frontend via port 5000.
# QR codes point to http://<local-ip>:5000/tracker.html?driver=<id>&date=<date>

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')

@app.route('/tracker.html')
def serve_tracker():
    return send_from_directory(FRONTEND_DIR, 'tracker.html')

@app.route('/entrix.html')
def serve_entrix():
    return send_from_directory(FRONTEND_DIR, 'entrix.html')

@app.route('/frontend/<path:filename>')
def serve_frontend_static(filename):
    return send_from_directory(FRONTEND_DIR, filename)


# ─── Health check ─────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    db = get_db()
    return jsonify({
        "status":      "ok",
        "collections": sorted(db.list_collection_names()),
    })


# ─── Admin dashboard stats ────────────────────────────────────────────────────

@app.route("/api/admin/dashboard")
def admin_dashboard():
    db           = get_db()
    warehouse_id = request.args.get("warehouse_id", "")
    today        = date.today().isoformat()
    tomorrow     = (date.today().__class__.fromisoformat(today).__class__
                    .fromordinal(date.today().toordinal() + 1)).isoformat()

    drivers_available = db.drivers.count_documents({
        "warehouse_id":    warehouse_id,
        "available_dates": today,
        "active":          True,
    })
    packages_tomorrow = db.packages.count_documents({
        "warehouse_id":  warehouse_id,
        "delivery_date": tomorrow,
        "status":        {"$in": ["pending", "unassigned"]},
    })
    total_today     = db.assignments.count_documents({"warehouse_id": warehouse_id, "date": today})
    completed_today = db.assignments.count_documents({"warehouse_id": warehouse_id, "date": today, "status": "completed"})

    return mongo_response({
        "date":               today,
        "warehouse_id":       warehouse_id,
        "drivers_available":  drivers_available,
        "packages_tomorrow":  packages_tomorrow,
        "total_today":        total_today,
        "completed":          completed_today,
    })


# ─── Pipeline trigger ─────────────────────────────────────────────────────────

@app.route("/api/pipeline/run", methods=["POST"])
def pipeline_run():
    from pipeline import run_morning_pipeline
    body   = request.get_json(force=True) or {}
    wh_id  = body.get("warehouse_id", "")
    if not wh_id:
        return mongo_response({"error": "warehouse_id is required."}, 400)
    result = run_morning_pipeline(wh_id, dry_run=bool(body.get("dry_run", False)))
    return mongo_response(result, 200 if result["status"] == "ok" else 500)


@app.route("/api/pipeline/rerun", methods=["POST"])
def pipeline_rerun():
    from pipeline import rerun_pipeline
    body     = request.get_json(force=True) or {}
    wh_id    = body.get("warehouse_id", "")
    date_str = body.get("date", "")
    if not wh_id or not date_str:
        return mongo_response({"error": "warehouse_id and date are required."}, 400)
    result = rerun_pipeline(wh_id, date_str, dry_run=bool(body.get("dry_run", False)))
    return mongo_response(result, 200 if result["status"] == "ok" else 500)


# ─── Driver registration (legacy / onboarding form) ───────────────────────────

@app.route("/api/drivers", methods=["POST"])
def register_driver():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body received."}), 400

    required = ["name", "dob", "experience_years", "vehicle_type", "phone", "warehouse_id"]
    missing  = [f for f in required if f not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    if data["vehicle_type"] not in ("van", "bike", "car", "motorcycle", "truck"):
        return jsonify({"error": "Invalid vehicle_type."}), 400

    try:
        capacity = get_capacity(data["dob"], int(data["experience_years"]))
    except ValueError as e:
        return jsonify({"error": str(e)}), 422

    db = get_db()

    if db.drivers.find_one({"phone": data["phone"]}):
        return jsonify({"error": "A driver with this phone number already exists."}), 409

    driver_doc = {
        "name":                        data["name"],
        "dob":                         data["dob"],
        "phone":                       data["phone"],
        "vehicle_type":                data["vehicle_type"],
        "warehouse_id":                data.get("warehouse_id", ""),
        "experience_years":            int(data["experience_years"]),
        "age":                         capacity["age"],
        "capacity_tier":               capacity["capacity_tier"],
        "max_single_route_difficulty": capacity["max_single_route_difficulty"],
        "experience_bonus_applied":    capacity["experience_bonus_applied"],
        "daily_score_so_far":          0,
        "active":                      True,
        "is_active_today":             True,
        "past_feedback_avg":           0.0,
        "available_dates":             [],
        "created_at":                  datetime.utcnow().isoformat(),
    }

    result = db.drivers.insert_one(driver_doc)

    return jsonify({
        "message":                     "Driver registered successfully.",
        "driver_id":                   str(result.inserted_id),
        "age":                         capacity["age"],
        "capacity_tier":               capacity["capacity_tier"],
        "max_single_route_difficulty": capacity["max_single_route_difficulty"],
        "experience_bonus_applied":    capacity["experience_bonus_applied"],
    }), 201


# ─── List drivers ─────────────────────────────────────────────────────────────

@app.route("/api/drivers", methods=["GET"])
def list_drivers():
    db           = get_db()
    today        = date.today().isoformat()
    warehouse_id = request.args.get("warehouse_id", "")

    query = {}
    if warehouse_id:
        query["warehouse_id"] = warehouse_id

    drivers = list(db.drivers.find(query))
    result  = []
    for d in drivers:
        load = db.assignments.count_documents({
            "driver_id": d["_id"],
            "date":      today,
        })
        result.append({
            "_id":                         str(d["_id"]),
            "name":                        d.get("name", ""),
            "phone":                       d.get("phone", ""),
            "capacity_tier":               d.get("capacity_tier", ""),
            "max_single_route_difficulty": d.get("max_single_route_difficulty", 0),
            "max_difficulty":              d.get("max_difficulty", d.get("max_single_route_difficulty", 0)),
            "is_active_today":             d.get("is_active_today", False),
            "active":                      d.get("active", True),
            "warehouse_id":                d.get("warehouse_id", ""),
            "vehicle_type":                d.get("vehicle_type", ""),
            "assignments_today":           load,
        })

    return mongo_response({"count": len(result), "drivers": result})


# ─── Assignments ──────────────────────────────────────────────────────────────

@app.route("/api/assignments/today")
def assignments_today():
    db           = get_db()
    today        = date.today().isoformat()
    warehouse_id = request.args.get("warehouse_id", "")

    query = {"date": today}
    if warehouse_id:
        query["warehouse_id"] = warehouse_id

    assignments = list(db.assignments.find(query))
    return mongo_response({
        "date":        today,
        "count":       len(assignments),
        "assignments": assignments,
    })


@app.route("/api/assignments/<assignment_id>/status", methods=["PATCH"])
def update_assignment_status(assignment_id):
    db         = get_db()
    body       = request.get_json(force=True) or {}
    allowed    = {"pending", "in_progress", "completed", "failed"}
    new_status = body.get("status", "")

    if new_status not in allowed:
        return mongo_response({"error": f"status must be one of {allowed}"}, 400)

    result = db.assignments.update_one(
        {"_id": ObjectId(assignment_id)},
        {"$set": {"status": new_status}},
    )

    if result.matched_count == 0:
        return mongo_response({"error": "Assignment not found."}, 404)

    return mongo_response({
        "updated":       True,
        "assignment_id": assignment_id,
        "new_status":    new_status,
    })


# ─── Clusters ─────────────────────────────────────────────────────────────────

@app.route("/api/clusters/unassigned")
def unassigned_clusters():
    db           = get_db()
    today        = date.today().isoformat()
    warehouse_id = request.args.get("warehouse_id", "")

    query = {"date": today, "status": "pending", "driver_id": {"$in": [None, ""]}}
    if warehouse_id:
        query["warehouse_id"] = warehouse_id

    unassigned = list(db.assignments.find(query))
    return mongo_response({
        "date":     today,
        "count":    len(unassigned),
        "clusters": unassigned,
    })


# ─── Stats ────────────────────────────────────────────────────────────────────

@app.route("/api/stats/today")
def stats_today():
    db           = get_db()
    today        = date.today().isoformat()
    warehouse_id = request.args.get("warehouse_id", "")

    q = {"date": today}
    if warehouse_id:
        q["warehouse_id"] = warehouse_id

    dq = {}
    if warehouse_id:
        dq["warehouse_id"] = warehouse_id

    return mongo_response({
        "date":             today,
        "total":            db.assignments.count_documents(q),
        "pending":          db.assignments.count_documents({**q, "status": "pending"}),
        "in_progress":      db.assignments.count_documents({**q, "status": "in_progress"}),
        "completed":        db.assignments.count_documents({**q, "status": "completed"}),
        "failed":           db.assignments.count_documents({**q, "status": "failed"}),
        "active_drivers":   db.drivers.count_documents({**dq, "is_active_today": True}),
        "packages_pending": db.packages.count_documents({
            **dq,
            "status": {"$in": ["pending", "unassigned"]},
        }) if warehouse_id else 0,
    })


# ─── Admin: active drivers for Entrix Panel ───────────────────────────────────

@app.route("/api/admin/drivers/active", methods=["GET"])
def get_active_drivers():
    db           = get_db()
    warehouse_id = request.args.get("warehouse_id", "").strip()
    query        = {"active": True}
    if warehouse_id:
        query["warehouse_id"] = warehouse_id
    drivers = list(db.drivers.find(query))
    for d in drivers:
        d["_id"] = str(d["_id"])
    return jsonify({"drivers": drivers}), 200


# ─── Dev / test endpoints ─────────────────────────────────────────────────────

@app.route("/api/score-test")
def score_test():
    from scoring import score_cluster, scale_to_units
    result = score_cluster(
        total_weight_kg=22,
        package_count=18,
        stair_density=0.6,
        route_distance_km=8.5,
        difficulty_modifier=1.15,
    )
    result["difficulty_units"] = scale_to_units(result["difficulty_score"])
    return jsonify(result)


@app.route("/api/cluster-test")
def cluster_test():
    from cluster import build_clusters

    db       = get_db()
    subareas = list(db.subareas.find({}, {"_id": 1, "name": 1}))
    if not subareas:
        return jsonify({"error": "No subareas found. Run seed.py first."}), 400

    random.seed(42)
    test_packages = [
        {
            "subarea_id": subareas[i % 3]["_id"],
            "lat":        13.05 + random.uniform(-0.02, 0.02),
            "lon":        80.20 + random.uniform(-0.02, 0.02),
            "weight_kg":  random.uniform(0.5, 5.0),
        }
        for i in range(20)
    ]

    clusters = build_clusters(test_packages, "chennai")
    return jsonify({
        "cluster_count": len(clusters),
        "clusters": [
            {
                "subarea_name":      c["subarea_name"],
                "package_count":     c["package_count"],
                "total_weight_kg":   c["total_weight_kg"],
                "route_distance_km": c["route_distance_km"],
                "difficulty_units":  c["difficulty_units"],
            }
            for c in clusters
        ],
    })


@app.route("/api/balance-test")
def balance_test():
    from cluster import build_clusters
    from balancer import balance

    db       = get_db()
    subareas = list(db.subareas.find({}, {"_id": 1, "name": 1}))
    if not subareas:
        return jsonify({"error": "No subareas found."}), 400

    random.seed(42)
    test_packages = [
        {
            "subarea_id": subareas[i % 3]["_id"],
            "lat":        13.05 + random.uniform(-0.02, 0.02),
            "lon":        80.20 + random.uniform(-0.02, 0.02),
            "weight_kg":  random.uniform(0.5, 5.0),
        }
        for i in range(20)
    ]

    clusters = build_clusters(test_packages, "chennai")
    result   = balance(clusters, "chennai")

    return mongo_response({
        "assigned_count":   len(result["assigned"]),
        "unassigned_count": len(result["unassigned"]),
        "assignments":      result["assigned"],
        "unassigned":       result["unassigned"],
    })


# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)
    
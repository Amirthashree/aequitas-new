# backend/auth_routes.py
# ─────────────────────────────────────────────────────────────────────────────
# Phase 8: All authentication endpoints
# Warehouse-matching: drivers can only connect to warehouses that have an admin
# ─────────────────────────────────────────────────────────────────────────────

import os
from flask import Blueprint, request, jsonify
from db import get_db
from auth import verify_google_token, hash_password, check_password
from capacity import get_capacity
from datetime import datetime

auth_bp = Blueprint("auth", __name__)

# ── Secret key for admin registration ────────────────────────────────────────
# Set ADMIN_SECRET_KEY in your .env file. Default is shown below.
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "AEQUITAS_ADMIN_2024")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def warehouse_exists(db, warehouse_id: str) -> bool:
    """Returns True if at least one admin is registered for this warehouse."""
    return db.admins.find_one({"warehouse_id": warehouse_id}) is not None


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@auth_bp.route("/api/auth/admin/login", methods=["POST"])
def admin_login():
    """
    Password-based admin login.
    Body: { warehouse_id, username, password }
    """
    body         = request.get_json(force=True) or {}
    warehouse_id = body.get("warehouse_id", "").strip()
    username     = body.get("username", "").strip()
    password     = body.get("password", "")

    if not all([warehouse_id, username, password]):
        return jsonify({"error": "warehouse_id, username, and password are required."}), 400

    db    = get_db()
    admin = db.admins.find_one({"warehouse_id": warehouse_id, "username": username})

    if not admin:
        return jsonify({"error": "Invalid credentials."}), 401

    if not check_password(password, admin.get("password_hash", "")):
        return jsonify({"error": "Invalid credentials."}), 401

    return jsonify({
        "message":      "Login successful.",
        "admin_id":     str(admin["_id"]),
        "warehouse_id": warehouse_id,
        "username":     username,
        "name":         admin.get("name", username),
    }), 200


@auth_bp.route("/api/auth/admin/google", methods=["POST"])
def admin_google():
    """
    Google Sign-In for admins.
    Body: { token, warehouse_id }
    """
    body         = request.get_json(force=True) or {}
    token        = body.get("token", "")
    warehouse_id = body.get("warehouse_id", "").strip()

    if not token or not warehouse_id:
        return jsonify({"error": "token and warehouse_id are required."}), 400

    info = verify_google_token(token)
    if not info:
        return jsonify({"error": "Invalid or expired Google token."}), 401

    db    = get_db()
    admin = db.admins.find_one({
        "google_id":    info["google_id"],
        "warehouse_id": warehouse_id,
    })

    if not admin:
        return jsonify({"error": "No admin account linked to this Google account."}), 404

    return jsonify({
        "message":      "Google login successful.",
        "admin_id":     str(admin["_id"]),
        "warehouse_id": warehouse_id,
        "name":         admin.get("name", info["name"]),
        "email":        info["email"],
    }), 200


@auth_bp.route("/api/auth/admin/register", methods=["POST"])
def admin_register():
    """
    Register a new admin for a warehouse.
    Body: { warehouse_id, username, name, password, secret_key }

    Rules:
    - secret_key must match ADMIN_SECRET_KEY env variable
    - username must be unique per warehouse
    """
    body         = request.get_json(force=True) or {}
    warehouse_id = body.get("warehouse_id", "").strip()
    username     = body.get("username", "").strip()
    name         = body.get("name", "").strip()
    password     = body.get("password", "")
    secret_key   = body.get("secret_key", "")

    # ── Field validation ──────────────────────────────────────────────────────
    if not all([warehouse_id, username, name, password, secret_key]):
        return jsonify({"error": "All fields are required."}), 400

    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400

    # ── Secret key check ──────────────────────────────────────────────────────
    if secret_key != ADMIN_SECRET_KEY:
        return jsonify({"error": "Invalid secret key. Contact your system administrator."}), 403

    db = get_db()

    # ── Duplicate username check per warehouse ────────────────────────────────
    if db.admins.find_one({"warehouse_id": warehouse_id, "username": username}):
        return jsonify({"error": "An admin with this username already exists for this warehouse."}), 409

    result = db.admins.insert_one({
        "warehouse_id":  warehouse_id,
        "username":      username,
        "name":          name,
        "password_hash": hash_password(password),
        "google_id":     None,
        "created_at":    datetime.utcnow().isoformat(),
    })

    return jsonify({
        "message":      "Admin registered successfully.",
        "admin_id":     str(result.inserted_id),
        "warehouse_id": warehouse_id,
        "username":     username,
        "name":         name,
    }), 201


# ─────────────────────────────────────────────────────────────────────────────
# DRIVER ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@auth_bp.route("/api/auth/driver/login", methods=["POST"])
def driver_login():
    """
    Password-based driver login.
    Body: { phone, password }

    Warehouse check: driver's stored warehouse_id must have
    a registered admin — otherwise the warehouse is invalid.
    """
    body     = request.get_json(force=True) or {}
    phone    = body.get("phone", "").strip()
    password = body.get("password", "")

    if not phone or not password:
        return jsonify({"error": "phone and password are required."}), 400

    db     = get_db()
    driver = db.drivers.find_one({"phone": phone})

    if not driver:
        return jsonify({"error": "Invalid credentials."}), 401

    if not check_password(password, driver.get("password_hash", "")):
        return jsonify({"error": "Invalid credentials."}), 401

    # ── Warehouse check ───────────────────────────────────────────────────────
    driver_warehouse = driver.get("warehouse_id", "")
    if not warehouse_exists(db, driver_warehouse):
        return jsonify({
            "error": "You are not assigned to this warehouse. Contact your admin."
        }), 403

    return jsonify({
        "message":       "Login successful.",
        "driver_id":     str(driver["_id"]),
        "name":          driver.get("name", ""),
        "phone":         phone,
        "capacity_tier": driver.get("capacity_tier"),
        "warehouse_id":  driver_warehouse,
    }), 200


@auth_bp.route("/api/auth/driver/google", methods=["POST"])
def driver_google():
    """
    Google Sign-In for drivers.
    Body: { token }
    """
    body  = request.get_json(force=True) or {}
    token = body.get("token", "")

    if not token:
        return jsonify({"error": "token is required."}), 400

    info = verify_google_token(token)
    if not info:
        return jsonify({"error": "Invalid or expired Google token."}), 401

    db     = get_db()
    driver = db.drivers.find_one({"google_id": info["google_id"]})

    if not driver:
        return jsonify({"error": "No driver account linked to this Google account."}), 404

    # ── Warehouse check ───────────────────────────────────────────────────────
    driver_warehouse = driver.get("warehouse_id", "")
    if not warehouse_exists(db, driver_warehouse):
        return jsonify({
            "error": "You are not assigned to this warehouse. Contact your admin."
        }), 403

    return jsonify({
        "message":       "Google login successful.",
        "driver_id":     str(driver["_id"]),
        "name":          driver.get("name", info["name"]),
        "email":         info["email"],
        "capacity_tier": driver.get("capacity_tier"),
        "warehouse_id":  driver_warehouse,
    }), 200


@auth_bp.route("/api/auth/driver/register", methods=["POST"])
def driver_register():
    """
    Full driver sign-up with auto capacity computation.
    Body: {
        name, dob (DD/MM/YYYY), phone, password,
        vehicle_type, experience_years,
        warehouse_id, home_lat, home_lng
    }

    Warehouse check: warehouse_id must have a registered admin.
    A driver cannot register for a warehouse that doesn't exist.
    """
    body = request.get_json(force=True) or {}

    required = ["name", "dob", "phone", "password", "vehicle_type",
                "experience_years", "warehouse_id"]
    missing  = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    db = get_db()

    # ── Warehouse check: must have an admin ───────────────────────────────────
    warehouse_id = body["warehouse_id"].strip()
    if not warehouse_exists(db, warehouse_id):
        return jsonify({
            "error": "You are not assigned to this warehouse. Contact your admin."
        }), 403

    # ── Duplicate phone check ─────────────────────────────────────────────────
    if db.drivers.find_one({"phone": body["phone"]}):
        return jsonify({"error": "A driver with this phone number already exists."}), 409

    # ── Compute capacity tier from DOB + experience ───────────────────────────
    try:
        cap = get_capacity(body["dob"], int(body["experience_years"]))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422

    driver_doc = {
        "name":             body["name"].strip(),
        "dob":              body["dob"].strip(),
        "phone":            body["phone"].strip(),
        "password_hash":    hash_password(body["password"]),
        "vehicle_type":     body["vehicle_type"],
        "experience_years": int(body["experience_years"]),
        "warehouse_id":     warehouse_id,
        "home_lat":         body.get("home_lat"),
        "home_lng":         body.get("home_lng"),
        "capacity_tier":    cap["capacity_tier"],
        "max_difficulty":   cap["max_single_route_difficulty"],
        "active":           True,
        "available_dates":  [],
        "created_at":       datetime.utcnow().isoformat(),
    }

    result = db.drivers.insert_one(driver_doc)

    return jsonify({
        "message":        "Driver registered successfully.",
        "driver_id":      str(result.inserted_id),
        "capacity_tier":  cap["capacity_tier"],
        "max_difficulty": cap["max_single_route_difficulty"],
        "age":            cap["age"],
    }), 201

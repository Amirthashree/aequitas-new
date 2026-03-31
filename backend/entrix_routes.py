"""
AEQUITAS v3.0 — Phase 10
entrix_routes.py — Real-time tracking + QR download endpoints

Endpoints:
  GET  /api/entrix/realtime              → per-driver delivery summary
  GET  /api/entrix/qr/assignment/<id>   → ZIP of all QR PNGs for assignment
  GET  /api/entrix/qr/package/<id>      → single QR PNG download

Register in app.py:
  from entrix_routes import entrix_bp
  app.register_blueprint(entrix_bp)
"""

import io
import zipfile
from datetime import datetime, timezone

import qrcode
from bson import ObjectId
from flask import Blueprint, jsonify, send_file

from db import get_db 

entrix_bp = Blueprint("entrix", __name__)


# ─────────────────────────────────────────────
# Helper: safe ObjectId conversion
# ─────────────────────────────────────────────

def to_object_id(id_str):
    """Return ObjectId or None if invalid."""
    try:
        return ObjectId(id_str)
    except Exception:
        return None


# ─────────────────────────────────────────────
# Helper: generate a QR PNG as bytes
# ─────────────────────────────────────────────

def _make_qr_bytes(data: str) -> bytes:
    """Render a QR code to PNG bytes (in-memory, no disk writes)."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────
# Helper: build the QR payload for a package
# ─────────────────────────────────────────────

def _qr_payload(package: dict, assignment_id: str) -> str:
    """
    The string encoded inside the QR code.
    Format: AEQUITAS|<package_id>|<assignment_id>|<recipient>|<address>
    The driver app / entrix scan page reads this to mark delivered / turn down.
    """
    pkg_id = str(package.get("_id", ""))
    recipient = package.get("recipient_name", "Unknown")
    address = package.get("address", "")
    return f"AEQUITAS|{pkg_id}|{assignment_id}|{recipient}|{address}"


# ═════════════════════════════════════════════
# ENDPOINT 1 — GET /api/entrix/realtime
# ═════════════════════════════════════════════

@entrix_bp.route("/api/entrix/realtime", methods=["GET"])
def entrix_realtime():
    """
    Returns per-driver delivery summary for today.

    Response shape:
    {
      "date": "2026-03-28",
      "drivers": [
        {
          "driver_id": "...",
          "driver_name": "Ravi Kumar",
          "assignment_id": "...",
          "assigned":  12,
          "completed":  8,
          "pending":    3,
          "failed":     1,
          "progress":  66.7,          ← percentage of assigned completed
          "packages": [               ← per-package detail for expandable rows
            {
              "package_id": "...",
              "recipient":  "...",
              "address":    "...",
              "status":     "delivered" | "pending" | "failed"
            },
            ...
          ]
        },
        ...
      ]
    }
    """
    db = get_db()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Fetch all assignments for today that have a driver attached
    assignments = list(
        db.assignments.find(
            {"date": today_str, "driver_id": {"$exists": True, "$ne": None}}
        )
    )

    if not assignments:
        return jsonify({"date": today_str, "drivers": []}), 200

    # Collect all driver IDs so we can batch-fetch names
    driver_ids = [a.get("driver_id") for a in assignments if a.get("driver_id")]
    driver_docs = {
        str(d["_id"]): d.get("name", "Unknown Driver")
        for d in db.drivers.find({"_id": {"$in": [to_object_id(did) for did in driver_ids]}})
    }

    result = []
    for asgn in assignments:
        driver_id = str(asgn.get("driver_id", ""))
        assignment_id = str(asgn["_id"])
        packages_raw = asgn.get("packages", [])  # list of package dicts embedded in assignment

        # Count statuses
        counts = {"delivered": 0, "pending": 0, "failed": 0}
        pkg_details = []

        for pkg in packages_raw:
            status = pkg.get("status", "pending")
            if status not in counts:
                status = "pending"
            counts[status] += 1
            pkg_details.append(
                {
                    "package_id": str(pkg.get("_id", pkg.get("package_id", ""))),
                    "recipient":  pkg.get("recipient_name", pkg.get("recipient", "—")),
                    "address":    pkg.get("address", "—"),
                    "status":     status,
                }
            )

        total = len(packages_raw)
        completed = counts["delivered"]
        progress = round((completed / total * 100), 1) if total > 0 else 0.0

        result.append(
            {
                "driver_id":     driver_id,
                "driver_name":   driver_docs.get(driver_id, "Unknown Driver"),
                "assignment_id": assignment_id,
                "assigned":      total,
                "completed":     completed,
                "pending":       counts["pending"],
                "failed":        counts["failed"],
                "progress":      progress,
                "packages":      pkg_details,
            }
        )

    # Sort by driver name for consistent table order
    result.sort(key=lambda x: x["driver_name"])

    return jsonify({"date": today_str, "drivers": result}), 200


# ═════════════════════════════════════════════
# ENDPOINT 2 — GET /api/entrix/qr/assignment/<id>
# ═════════════════════════════════════════════

@entrix_bp.route("/api/entrix/qr/assignment/<assignment_id>", methods=["GET"])
def entrix_qr_assignment(assignment_id):
    """
    Returns a ZIP file containing one QR PNG per package in the assignment.

    ZIP structure:
      qr_<assignment_id>/
        <package_id>_<recipient_slug>.png
        ...
    """
    db = get_db()
    oid = to_object_id(assignment_id)
    if not oid:
        return jsonify({"error": "Invalid assignment ID"}), 400

    asgn = db.assignments.find_one({"_id": oid})
    if not asgn:
        return jsonify({"error": "Assignment not found"}), 404

    packages = asgn.get("packages", [])
    if not packages:
        return jsonify({"error": "No packages in this assignment"}), 404

    # Build ZIP in memory
    zip_buf = io.BytesIO()
    folder = f"qr_{assignment_id}"

    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for pkg in packages:
            payload = _qr_payload(pkg, assignment_id)
            qr_bytes = _make_qr_bytes(payload)

            pkg_id = str(pkg.get("_id", pkg.get("package_id", "unknown")))
            recipient = pkg.get("recipient_name", pkg.get("recipient", "unknown"))
            # Slugify recipient for filename
            slug = "".join(c if c.isalnum() else "_" for c in recipient)[:30]
            filename = f"{folder}/{pkg_id}_{slug}.png"
            zf.writestr(filename, qr_bytes)

    zip_buf.seek(0)
    zip_filename = f"qr_assignment_{assignment_id}.zip"

    return send_file(
        zip_buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_filename,
    )


# ═════════════════════════════════════════════
# ENDPOINT 3 — GET /api/entrix/qr/package/<id>
# ═════════════════════════════════════════════

@entrix_bp.route("/api/entrix/qr/package/<package_id>", methods=["GET"])
def entrix_qr_package(package_id):
    """
    Returns a single QR PNG for one package.

    Looks up the package inside any assignment document.
    Falls back to packages collection if you have one.
    """
    db = get_db()

    # Strategy 1: find the package embedded inside an assignment
    asgn = db.assignments.find_one(
        {"packages._id": to_object_id(package_id)},
        {"packages.$": 1}  # project only the matching package
    )

    if asgn and asgn.get("packages"):
        pkg = asgn["packages"][0]
        assignment_id = str(asgn["_id"])
    else:
        # Strategy 2: look in a top-level packages collection (if it exists)
        oid = to_object_id(package_id)
        pkg = db.packages.find_one({"_id": oid}) if oid else None
        if not pkg:
            # Strategy 3: try string package_id field
            pkg = db.packages.find_one({"package_id": package_id})
        if not pkg:
            return jsonify({"error": "Package not found"}), 404
        assignment_id = str(pkg.get("assignment_id", "unassigned"))

    payload = _qr_payload(pkg, assignment_id)
    qr_bytes = _make_qr_bytes(payload)

    recipient = pkg.get("recipient_name", pkg.get("recipient", "package"))
    slug = "".join(c if c.isalnum() else "_" for c in recipient)[:30]
    filename = f"qr_{package_id}_{slug}.png"

    return send_file(
        io.BytesIO(qr_bytes),
        mimetype="image/png",
        as_attachment=True,
        download_name=filename,
    )

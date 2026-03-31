"""
public_routes.py — Public (no-auth) endpoints for QR code tracking page.

Endpoints:
  GET /api/public/driver/<driver_id>/progress  — live driver delivery progress
  Optional query param: ?date=YYYY-MM-DD       — fetch for a specific date
                                                 (defaults to today then tomorrow)
"""

from flask import Blueprint, jsonify, request
from bson import ObjectId
from datetime import datetime, timezone, timedelta
from db import get_db

public_bp = Blueprint('public', __name__)


@public_bp.route('/api/public/driver/<driver_id>/progress', methods=['GET'])
def driver_progress(driver_id):
    db = get_db()

    # ── Resolve driver ────────────────────────────────────────────────────────
    driver = None
    try:
        driver = db.drivers.find_one({'_id': ObjectId(driver_id)})
    except Exception:
        pass
    if not driver:
        driver = db.drivers.find_one({'driver_id': driver_id})
    if not driver:
        return jsonify({'error': 'Driver not found'}), 404

    driver_id_str = str(driver['_id'])

    # ── Date resolution ───────────────────────────────────────────────────────
    # If ?date=YYYY-MM-DD is passed (from QR / Entrix Panel), use that date.
    # Otherwise fall back to today + tomorrow (original behaviour).
    requested_date = request.args.get('date', '').strip()
    if requested_date:
        date_candidates = [requested_date]
    else:
        today_str    = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        tomorrow_str = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')
        date_candidates = [today_str, tomorrow_str]

    # ── Fetch assignments ─────────────────────────────────────────────────────
    assignments = list(db.assignments.find({
        'driver_id':     {'$in': [driver_id_str, driver_id]},
        'delivery_date': {'$in': date_candidates}
    }))
    if not assignments:
        assignments = list(db.assignments.find({
            'driver_id': {'$in': [driver_id_str, driver_id]},
            'date':      {'$in': date_candidates}
        }))

    # ── Collect package IDs ───────────────────────────────────────────────────
    all_pkg_oids = []
    for a in assignments:
        for pid in a.get('package_ids', []):
            try:
                all_pkg_oids.append(ObjectId(str(pid)) if not isinstance(pid, ObjectId) else pid)
            except Exception:
                pass

    packages = []
    if all_pkg_oids:
        raw = list(db.packages.find({'_id': {'$in': all_pkg_oids}}))
        for p in raw:
            packages.append({
                'package_id':     p.get('package_id') or str(p['_id']),
                'recipient_name': p.get('recipient_name', ''),
                'address':        p.get('address', ''),
                'subarea':        p.get('subarea', ''),
                'weight_kg':      p.get('weight_kg'),
                'status':         p.get('status', 'pending'),
                'route_order':    p.get('route_order'),
                'distance_km':    p.get('distance_km'),
                'fragile':        p.get('fragile', False),
                'delivered_at':   p.get('delivered_at'),
                'turned_down_at': p.get('turned_down_at'),
            })

    # ── Sort by route_order ───────────────────────────────────────────────────
    packages.sort(key=lambda p: (p.get('route_order') is None, p.get('route_order') or 0))

    total     = len(packages)
    delivered = sum(1 for p in packages if p['status'] == 'delivered')
    turned    = sum(1 for p in packages if p['status'] == 'turned-down')
    pending   = total - delivered - turned
    pct       = round((delivered / total) * 100) if total else 0

    # Use the first resolved date for the response label
    resolved_date = date_candidates[0]

    return jsonify({
        'driver': {
            'id':            driver_id_str,
            'name':          driver.get('name', ''),
            'warehouse_id':  driver.get('warehouse_id', ''),
            'capacity_tier': driver.get('capacity_tier', ''),
            'active':        driver.get('active', True),
        },
        'date': resolved_date,
        'summary': {
            'total':     total,
            'delivered': delivered,
            'turned':    turned,
            'pending':   pending,
            'pct':       pct,
        },
        'packages':   packages,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
    }), 200

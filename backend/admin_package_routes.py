"""
admin_package_routes.py — All package-related routes for the admin UI.

Endpoints:
  POST /api/admin/packages/upload     — upload CSV
  GET  /api/admin/packages/tomorrow   — list tomorrow's packages for a warehouse
  GET  /api/admin/packages/template   — download blank CSV template
  POST /api/admin/assign              — run assignment pipeline (nearest → farthest)
"""

from flask import Blueprint, request, jsonify, Response
from db import get_db
from datetime import datetime, timedelta
import csv
import io
import math

admin_pkg_bp = Blueprint('admin_pkg', __name__)

REQUIRED_COLUMNS   = {'recipient_name', 'address', 'subarea', 'weight_kg'}
VALID_CATEGORIES   = {'electronics', 'fragile', 'general', 'documents', 'heavy', 'other'}
VALID_TIME_WINDOWS = {'morning', 'afternoon', 'evening', 'anytime'}

# ── Chennai subarea centroids (lat, lng) ──────────────────────────────────────
SUBAREA_COORDS = {
    'Anna Nagar':     (13.0850, 80.2101),
    'T Nagar':        (13.0418, 80.2341),
    'T. Nagar':       (13.0418, 80.2341),
    'Adyar':          (13.0012, 80.2565),
    'Velachery':      (12.9815, 80.2180),
    'Tambaram':       (12.9249, 80.1000),
    'Porur':          (13.0324, 80.1574),
    'Chromepet':      (12.9516, 80.1462),
    'Perambur':       (13.1143, 80.2329),
    'Sholinganallur': (12.9010, 80.2279),
    'Mogappair':      (13.0950, 80.1754),
    'Avadi':          (13.1152, 80.1046),
    'Ambattur':       (13.0982, 80.1688),
    'Besant Nagar':   (13.0002, 80.2668),
    'Kilpauk':        (13.0814, 80.2382),
    'Guindy':         (13.0067, 80.2206),
}

# WH004 warehouse location (Chennai central depot)
WAREHOUSE_COORDS = {
    'WH004': (13.0827, 80.2707),   # Egmore area default
}
DEFAULT_WAREHOUSE_COORD = (13.0827, 80.2707)


# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize_bool(val):
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('true', '1', 'yes')

def normalize_category(val):
    if not val or str(val).strip() == '':
        return 'Other'
    v = str(val).strip().lower()
    return v.capitalize() if v in VALID_CATEGORIES else 'Other'

def haversine_km(lat1, lng1, lat2, lng2):
    """Return great-circle distance in km between two lat/lng points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def get_pkg_coords(pkg):
    """Return (lat, lng) for a package — real coords first, subarea centroid fallback."""
    if pkg.get('lat') and pkg.get('lng'):
        return (pkg['lat'], pkg['lng'])
    subarea = pkg.get('subarea', '')
    return SUBAREA_COORDS.get(subarea) or SUBAREA_COORDS.get(subarea.strip())

def dist_from_origin(pkg, origin):
    coords = get_pkg_coords(pkg)
    if not coords:
        return float('inf')
    return haversine_km(origin[0], origin[1], coords[0], coords[1])

def sort_packages_nearest_to_farthest(packages, origin):
    """
    Sort packages nearest → farthest from origin using a greedy
    nearest-neighbour approach (good enough for delivery routing).
    Returns list of (package, distance_km) tuples.
    """
    remaining = list(packages)
    ordered   = []
    current   = origin

    while remaining:
        # pick the nearest unvisited package
        best     = None
        best_d   = float('inf')
        best_idx = 0
        for i, p in enumerate(remaining):
            coords = get_pkg_coords(p)
            if not coords:
                d = float('inf')
            else:
                d = haversine_km(current[0], current[1], coords[0], coords[1])
            if d < best_d:
                best_d, best, best_idx = d, p, i

        ordered.append((best, round(best_d, 2)))
        coords = get_pkg_coords(best)
        current = coords if coords else current
        remaining.pop(best_idx)

    return ordered


# ── 1. CSV Upload ─────────────────────────────────────────────────────────────

@admin_pkg_bp.route('/api/admin/packages/upload', methods=['POST'])
def upload_packages():
    db = get_db()

    warehouse_id  = request.form.get('warehouse_id', '').strip()
    delivery_date = request.form.get('delivery_date', '').strip()

    if not warehouse_id:
        return jsonify({'error': 'warehouse_id is required'}), 400
    if not delivery_date:
        return jsonify({'error': 'delivery_date is required'}), 400
    try:
        datetime.strptime(delivery_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'delivery_date must be YYYY-MM-DD'}), 400

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Only .csv files are accepted'}), 400

    content    = file.read().decode('utf-8-sig')
    reader     = csv.DictReader(io.StringIO(content))
    fieldnames = [f.strip() for f in (reader.fieldnames or [])]

    missing = REQUIRED_COLUMNS - set(fieldnames)
    if missing:
        return jsonify({
            'error': f'Missing required columns: {missing}',
            'your_columns': fieldnames,
            'required_columns': list(REQUIRED_COLUMNS)
        }), 400

    inserted = 0
    skipped  = []
    warnings = []

    for i, raw_row in enumerate(reader, start=2):
        row = {k.strip(): (v.strip() if v else '') for k, v in raw_row.items()}

        recipient_name = row.get('recipient_name', '')
        address        = row.get('address', '')
        subarea        = row.get('subarea', '')
        weight_str     = row.get('weight_kg', '')

        if not recipient_name or not address or not subarea:
            skipped.append({'row': i, 'reason': 'Missing required text field'})
            continue
        try:
            weight_kg = float(weight_str)
        except (ValueError, TypeError):
            skipped.append({'row': i, 'reason': f'Invalid weight_kg: {weight_str!r}'})
            continue

        package_id      = row.get('package_id', '').strip() or None
        recipient_phone = row.get('recipient_phone', '').strip() or None
        if recipient_phone and not recipient_phone.isdigit():
            warnings.append({'row': i, 'warning': f'Invalid phone {recipient_phone!r}, ignored'})
            recipient_phone = None

        try:
            floor = int(row.get('floor', 0) or 0)
        except ValueError:
            floor = 0

        # Parse lat/lng from CSV if provided
        try:
            lat = float(row.get('lat') or 0) or None
        except (ValueError, TypeError):
            lat = None
        try:
            lng = float(row.get('lng') or 0) or None
        except (ValueError, TypeError):
            lng = None

        fragile    = normalize_bool(row.get('fragile',   False))
        has_lift   = normalize_bool(row.get('has_lift',  False))
        is_gated   = normalize_bool(row.get('is_gated',  False))
        category   = normalize_category(row.get('category', ''))
        time_window = str(row.get('time_window', 'anytime')).strip().lower()
        if time_window not in VALID_TIME_WINDOWS:
            time_window = 'anytime'

        doc = {
            'package_id':      package_id,
            'recipient_name':  recipient_name,
            'recipient_phone': recipient_phone,
            'address':         address,
            'subarea':         subarea,
            'warehouse_id':    warehouse_id,
            'delivery_date':   delivery_date,
            'category':        category,
            'weight_kg':       weight_kg,
            'fragile':         fragile,
            'floor':           floor,
            'has_lift':        has_lift,
            'is_gated':        is_gated,
            'time_window':     time_window,
            'status':          'pending',
            'assigned_to':     None,
            'cluster_id':      None,
            'assignment_id':   None,
            'lat':             lat,
            'lng':             lng,
            'route_order':     None,   # filled during assignment
            'distance_km':     None,   # filled during assignment
            'created_at':      datetime.utcnow().isoformat(),
            'source':          'csv_upload'
        }

        if package_id:
            db.packages.update_one(
                {'package_id': package_id, 'warehouse_id': warehouse_id},
                {'$set': doc},
                upsert=True
            )
        else:
            db.packages.insert_one(doc)

        inserted += 1

    return jsonify({
        'success':         True,
        'inserted':        inserted,
        'skipped':         len(skipped),
        'skipped_details': skipped,
        'warnings':        warnings,
        'delivery_date':   delivery_date,
        'message':         f'{inserted} packages uploaded for {delivery_date}.'
    }), 200


# ── 2. List tomorrow's packages ───────────────────────────────────────────────

@admin_pkg_bp.route('/api/admin/packages/tomorrow', methods=['GET'])
def list_tomorrow_packages():
    db           = get_db()
    warehouse_id = request.args.get('warehouse_id', '').strip()

    tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
    date     = request.args.get('date', tomorrow)

    query = {'delivery_date': date}
    if warehouse_id:
        query['warehouse_id'] = warehouse_id

    packages = list(db.packages.find(query))
    for p in packages:
        p['_id'] = str(p['_id'])

    return jsonify({'packages': packages, 'date': date}), 200


# ── 3. CSV Template download ──────────────────────────────────────────────────

@admin_pkg_bp.route('/api/admin/packages/template', methods=['GET'])
def download_template():
    headers = [
        'package_id', 'recipient_name', 'recipient_phone', 'address',
        'subarea', 'weight_kg', 'lat', 'lng', 'floor', 'fragile',
        'has_lift', 'is_gated', 'category', 'time_window'
    ]
    example = [
        'PKG001', 'Arjun Sharma', '9940678007',
        '12A, Anna Nagar East, Chennai',
        'Anna Nagar', '2.5', '13.0850', '80.2101',
        '0', 'false', 'false', 'false', 'electronics', 'morning'
    ]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerow(example)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=packages_template.csv'}
    )


# ── 4. Run assignment pipeline (nearest → farthest) ───────────────────────────

@admin_pkg_bp.route('/api/admin/assign', methods=['POST'])
def run_assign():
    from collections import defaultdict
    from bson import ObjectId

    db   = get_db()
    body = request.get_json() or {}
    warehouse_id = body.get('warehouse_id', '').strip()

    if not warehouse_id:
        return jsonify({'error': 'warehouse_id is required'}), 400

    tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')

    # ── Get pending packages ──────────────────────────────────────────────────
    packages = list(db.packages.find({
        'warehouse_id': warehouse_id,
        'delivery_date': tomorrow,
        'status': 'pending'
    }))

    if not packages:
        return jsonify({
            'status': 'ok',
            'message': 'No pending packages to assign',
            'drivers_used': 0,
            'assignments': []
        }), 200

    # ── Get active drivers ────────────────────────────────────────────────────
    drivers = list(db.drivers.find({
        'warehouse_id': warehouse_id,
        'active': True
    }))

    if not drivers:
        return jsonify({'error': 'No active drivers found for this warehouse'}), 400

    # ── Warehouse origin for distance sorting ─────────────────────────────────
    origin = WAREHOUSE_COORDS.get(warehouse_id, DEFAULT_WAREHOUSE_COORD)

    # ── Sort ALL packages nearest → farthest from warehouse ───────────────────
    sorted_packages = sort_packages_nearest_to_farthest(packages, origin)
    # sorted_packages is list of (pkg_doc, distance_km)

    # ── Group packages by subarea (preserving nearest-first order within each) ─
    subarea_groups  = defaultdict(list)
    subarea_order   = []   # track first-seen order for subareas
    for pkg, dist_km in sorted_packages:
        sa = pkg.get('subarea', 'Unknown')
        if sa not in subarea_groups:
            subarea_order.append(sa)
        subarea_groups[sa].append((pkg, dist_km))

    # ── Assign each subarea cluster to a driver (round-robin) ─────────────────
    assignments  = []
    drivers_used = set()
    driver_index = 0
    global_route_order = 0   # continuous stop counter across all clusters

    for subarea in subarea_order:
        pkgs_with_dist = subarea_groups[subarea]

        if driver_index >= len(drivers):
            driver_index = 0

        driver      = drivers[driver_index]
        driver_id   = str(driver['_id'])
        driver_name = driver.get('name', 'Driver')

        package_ids = [str(p['_id']) for p, _ in pkgs_with_dist]

        assignment = {
            'warehouse_id':  warehouse_id,
            'driver_id':     driver_id,
            'driver_name':   driver_name,
            'delivery_date': tomorrow,
            'cluster_id':    subarea,
            'package_ids':   package_ids,
            'status':        'pending',
            'created_at':    datetime.utcnow().isoformat()
        }

        result        = db.assignments.insert_one(assignment)
        assignment_id = str(result.inserted_id)

        for pkg, dist_km in pkgs_with_dist:
            db.packages.update_one(
                {'_id': pkg['_id']},
                {'$set': {
                    'status':        'assigned',
                    'assigned_to':   driver_id,
                    'cluster_id':    subarea,
                    'assignment_id': assignment_id,
                    'route_order':   global_route_order,   # ← stop number
                    'distance_km':   dist_km,              # ← km from warehouse
                }}
            )
            global_route_order += 1

        assignments.append({
            'assignment_id': assignment_id,
            'driver_id':     driver_id,
            'driver_name':   driver_name,
            'cluster_id':    subarea,
            'package_ids':   package_ids,
            'package_count': len(pkgs_with_dist)
        })

        drivers_used.add(driver_id)
        driver_index += 1

    return jsonify({
        'status':              'ok',
        'drivers_used':        len(drivers_used),
        'assignments':         assignments,
        'unassigned_clusters': []
    }), 200

"""
upload_routes.py — Handles CSV upload for packages.
Supports the WH004 CSV format with columns:
  package_id, recipient_name, address, subarea, weight_kg,
  floor, fragile, has_lift, is_gated, category, time_window
"""

from flask import Blueprint, request, jsonify
from db import get_db
from datetime import datetime
import csv
import io

upload_bp = Blueprint('upload', __name__)

REQUIRED_COLUMNS = {'recipient_name', 'address', 'subarea', 'weight_kg'}

VALID_CATEGORIES = {
    'electronics', 'fragile', 'general', 'documents', 'heavy', 'other'
}

VALID_TIME_WINDOWS = {'morning', 'afternoon', 'evening', 'anytime'}


def normalize_bool(val):
    """Convert 'true'/'false'/1/0 strings to Python bool."""
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('true', '1', 'yes')


def normalize_category(val):
    """Normalize category string to Title Case, default 'Other'."""
    if not val or str(val).strip() == '':
        return 'Other'
    v = str(val).strip().lower()
    if v not in VALID_CATEGORIES:
        return 'Other'
    return v.capitalize()


@upload_bp.route('/api/packages/upload', methods=['POST'])
def upload_packages():
    db = get_db()

    warehouse_id = request.form.get('warehouse_id', '').strip()
    delivery_date = request.form.get('delivery_date', '').strip()

    if not warehouse_id:
        return jsonify({'error': 'warehouse_id is required'}), 400
    if not delivery_date:
        return jsonify({'error': 'delivery_date is required'}), 400

    # Validate date format
    try:
        datetime.strptime(delivery_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'delivery_date must be YYYY-MM-DD'}), 400

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Only .csv files are accepted'}), 400

    content = file.read().decode('utf-8-sig')  # handle BOM if present
    reader = csv.DictReader(io.StringIO(content))

    # Normalize column names (strip whitespace)
    fieldnames = [f.strip() for f in (reader.fieldnames or [])]
    missing = REQUIRED_COLUMNS - set(fieldnames)
    if missing:
        return jsonify({
            'error': f'Missing required columns: {missing}',
            'your_columns': fieldnames,
            'required_columns': list(REQUIRED_COLUMNS)
        }), 400

    inserted = []
    skipped = []
    warnings = []

    for i, raw_row in enumerate(reader, start=2):  # row 2 = first data row
        # Strip whitespace from all keys and values
        row = {k.strip(): (v.strip() if v else '') for k, v in raw_row.items()}

        # --- Required fields ---
        recipient_name = row.get('recipient_name', '')
        address = row.get('address', '')
        subarea = row.get('subarea', '')
        weight_str = row.get('weight_kg', '')

        if not recipient_name or not address or not subarea:
            skipped.append({'row': i, 'reason': 'Missing required text field'})
            continue

        try:
            weight_kg = float(weight_str)
        except (ValueError, TypeError):
            skipped.append({'row': i, 'reason': f'Invalid weight_kg: {weight_str!r}'})
            continue

        # --- Optional fields ---
        package_id = row.get('package_id', '').strip() or None

        # Phone — optional
        recipient_phone = row.get('recipient_phone', '').strip() or None
        if recipient_phone and not recipient_phone.isdigit():
            warnings.append({'row': i, 'warning': f'Invalid phone {recipient_phone!r}, ignored'})
            recipient_phone = None

        # Numeric optional
        try:
            floor = int(row.get('floor', 0) or 0)
        except ValueError:
            floor = 0

        fragile = normalize_bool(row.get('fragile', False))
        has_lift = normalize_bool(row.get('has_lift', False))
        is_gated = normalize_bool(row.get('is_gated', False))

        category = normalize_category(row.get('category', ''))

        time_window = str(row.get('time_window', 'anytime')).strip().lower()
        if time_window not in VALID_TIME_WINDOWS:
            time_window = 'anytime'

        # --- Build document ---
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
            'lat':             None,
            'lng':             None,
            'created_at':      datetime.utcnow().isoformat(),
            'source':          'csv_upload'
        }

        # Upsert by package_id if provided, else always insert
        if package_id:
            db.packages.update_one(
                {'package_id': package_id, 'warehouse_id': warehouse_id},
                {'$set': doc},
                upsert=True
            )
        else:
            db.packages.insert_one(doc)

        inserted.append(package_id or 'auto-id')

    return jsonify({
        'success': True,
        'inserted': len(inserted),
        'skipped': len(skipped),
        'skipped_details': skipped,
        'warnings': warnings,
        'message': f'{len(inserted)} packages uploaded successfully for {delivery_date}.'
    }), 200

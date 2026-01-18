# backend/routes/walmart_dashboard.py
# Walmart Forecast Dashboard API endpoints
# Band break analysis for demand planning

from typing import Dict, List, Optional
from fastapi import APIRouter, Query
from datetime import datetime, timedelta, date
from collections import defaultdict
import os
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/api/walmart", tags=["walmart-dashboard"])

# Neon database connection - MUST be set in environment
WALMART_DATABASE_URL = os.getenv("WALMART_DATABASE_URL")
if not WALMART_DATABASE_URL:
    raise RuntimeError("WALMART_DATABASE_URL environment variable is required")


def _connect():
    return psycopg.connect(WALMART_DATABASE_URL, autocommit=True)


def to_date(val):
    """Convert various date types to date object."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except ValueError:
            return datetime.fromisoformat(val.replace('Z', '+00:00')).date()
    if hasattr(val, 'date'):
        return val.date()
    return val


def get_period_range(week: str, forecast_type: str):
    """Get the date range for charts based on forecast type."""
    week_date = to_date(week)
    if forecast_type == 'monthly':
        start = week_date - timedelta(days=45)
        end = week_date + timedelta(days=15)
    else:
        start = week_date - timedelta(days=120)
        end = week_date + timedelta(days=30)
    return start, end


def get_band_breaks(rows: List[Dict], week_date: date) -> Dict:
    """Calculate band breaks from rows."""
    breaks = {
        'upper_85': 0, 'upper_95': 0,
        'lower_85': 0, 'lower_95': 0,
        'upper_85_consec': 0, 'lower_85_consec': 0,
        'total_days': 0
    }
    
    current_upper_consec = 0
    current_lower_consec = 0
    
    sorted_rows = sorted([r for r in rows if to_date(r['date']) <= week_date], key=lambda x: to_date(x['date']))
    
    for row in sorted_rows:
        val = row.get('value')
        ci85_high = row.get('ci85_high')
        ci85_low = row.get('ci85_low')
        ci95_high = row.get('ci95_high')
        ci95_low = row.get('ci95_low')
        
        if val is None:
            continue
            
        breaks['total_days'] += 1
        
        if ci85_high is not None and val > ci85_high:
            breaks['upper_85'] += 1
            current_upper_consec += 1
            breaks['upper_85_consec'] = max(breaks['upper_85_consec'], current_upper_consec)
        else:
            current_upper_consec = 0
            
        if ci95_high is not None and val > ci95_high:
            breaks['upper_95'] += 1
            
        if ci85_low is not None and val < ci85_low:
            breaks['lower_85'] += 1
            current_lower_consec += 1
            breaks['lower_85_consec'] = max(breaks['lower_85_consec'], current_lower_consec)
        else:
            current_lower_consec = 0
            
        if ci95_low is not None and val < ci95_low:
            breaks['lower_95'] += 1
    
    return breaks


def get_band_breaks_from_rows(rows: List[Dict], week_date: date) -> Dict:
    """Wrapper for get_band_breaks that handles row format."""
    return get_band_breaks(rows, week_date)


# =============================================================================
# API ENDPOINTS
# =============================================================================

def get_weeks_in_range(start_date, end_date):
    """Get all week-ending Saturdays in a date range."""
    weeks = []
    if isinstance(start_date, str):
        current = datetime.strptime(start_date, '%Y-%m-%d')
    else:
        current = datetime.combine(start_date, datetime.min.time())
    if isinstance(end_date, str):
        end = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end = datetime.combine(end_date, datetime.min.time())
    
    # Find first Saturday
    days_until_saturday = (5 - current.weekday()) % 7
    if days_until_saturday == 0 and current.weekday() != 5:
        days_until_saturday = 7
    current = current + timedelta(days=days_until_saturday)
    
    while current <= end:
        weeks.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=7)
    
    return weeks


@router.get("/weeks")
def get_weeks(forecast_type: str = "monthly"):
    """Get available weeks (Saturdays) for the forecast type."""
    table = f"walmart_aggregate_{forecast_type}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MIN(date), MAX(date) FROM "{table}"')
            row = cur.fetchone()
    
    if row and row[0] and row[1]:
        start_date = to_date(row[0])
        end_date = to_date(row[1])
        return get_weeks_in_range(start_date, end_date)
    
    return []


@router.get("/geo-ids")
def get_geo_ids(geo_level: str = "all_locations", forecast_type: str = "monthly"):
    """Get geographic IDs for a level."""
    table = f"walmart_aggregate_{forecast_type}"
    with _connect() as conn:
        with conn.cursor() as cur:
            if geo_level == "all_locations":
                return ["ALL"]
            cur.execute(f'SELECT DISTINCT geo_id FROM "{table}" WHERE geo_level = %s ORDER BY geo_id', [geo_level])
            return [row[0] for row in cur.fetchall()]


@router.get("/departments")
def get_departments(
    week: str,
    forecast_type: str = "monthly",
    geo_level: str = "all_locations",
    geo_id: str = "ALL"
):
    """Get department band breaks."""
    table = f"walmart_aggregate_{forecast_type}"
    week_d = to_date(week)
    window_days = 30 if forecast_type == 'monthly' else 90
    window_start = week_d - timedelta(days=window_days - 1)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT product_id, type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND geo_level = %s 
                  AND geo_id = %s
                  AND product_level = 'department_id'
                ORDER BY product_id, type_id, date
            ''', [window_start, week_d, geo_level, geo_id])
            rows = cur.fetchall()
    
    dept_data = defaultdict(lambda: {'U': [], 'R': []})
    for row in rows:
        dept_data[row['product_id']][row['type_id']].append(dict(row))
    
    result = []
    for dept_id, type_rows in dept_data.items():
        units_breaks = get_band_breaks_from_rows(type_rows.get('U', []), week_d)
        revenue_breaks = get_band_breaks_from_rows(type_rows.get('R', []), week_d)
        result.append({
            'department_id': dept_id,
            'units': units_breaks,
            'revenue': revenue_breaks
        })
    
    return sorted(result, key=lambda x: x['department_id'])


@router.get("/categories")
def get_categories(
    week: str,
    forecast_type: str = "monthly",
    geo_level: str = "all_locations",
    geo_id: str = "ALL",
    department_id: Optional[str] = None
):
    """Get category band breaks."""
    table = f"walmart_aggregate_{forecast_type}"
    week_d = to_date(week)
    window_days = 30 if forecast_type == 'monthly' else 90
    window_start = week_d - timedelta(days=window_days - 1)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if department_id:
                cur.execute(f'''
                    SELECT product_id, type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                    FROM "{table}"
                    WHERE date >= %s AND date <= %s 
                      AND geo_level = %s 
                      AND geo_id = %s
                      AND product_level = 'category_id'
                      AND product_id LIKE %s
                    ORDER BY product_id, type_id, date
                ''', [window_start, week_d, geo_level, geo_id, f"{department_id}_%"])
            else:
                cur.execute(f'''
                    SELECT product_id, type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                    FROM "{table}"
                    WHERE date >= %s AND date <= %s 
                      AND geo_level = %s 
                      AND geo_id = %s
                      AND product_level = 'category_id'
                    ORDER BY product_id, type_id, date
                ''', [window_start, week_d, geo_level, geo_id])
            rows = cur.fetchall()
    
    cat_data = defaultdict(lambda: {'U': [], 'R': []})
    for row in rows:
        cat_data[row['product_id']][row['type_id']].append(dict(row))
    
    result = []
    for cat_id, type_rows in cat_data.items():
        units_breaks = get_band_breaks_from_rows(type_rows.get('U', []), week_d)
        revenue_breaks = get_band_breaks_from_rows(type_rows.get('R', []), week_d)
        result.append({
            'category_id': cat_id,
            'units': units_breaks,
            'revenue': revenue_breaks
        })
    
    return sorted(result, key=lambda x: x['category_id'])


@router.get("/location-summary")
def get_location_summary(
    week: str,
    forecast_type: str = "monthly",
    geo_level: str = "all_locations",
    geo_id: str = "ALL"
):
    """Get summary metrics for a location."""
    table = f"walmart_aggregate_{forecast_type}"
    week_d = to_date(week)
    window_days = 30 if forecast_type == 'monthly' else 90
    window_start = week_d - timedelta(days=window_days - 1)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND geo_level = %s 
                  AND geo_id = %s
                  AND product_level = 'total'
                  AND product_id = 'ALL'
                ORDER BY type_id, date
            ''', [window_start, week_d, geo_level, geo_id])
            rows = cur.fetchall()
    
    type_rows = {'U': [], 'R': []}
    for row in rows:
        type_rows[row['type_id']].append(dict(row))
    
    units_breaks = get_band_breaks_from_rows(type_rows.get('U', []), week_d)
    revenue_breaks = get_band_breaks_from_rows(type_rows.get('R', []), week_d)
    
    total_revenue = sum(r.get('value', 0) or 0 for r in type_rows.get('R', []))
    
    return {
        'units': units_breaks,
        'revenue': {
            **revenue_breaks,
            'total': total_revenue
        }
    }


@router.get("/chart/location")
def get_chart_location(
    week: str,
    forecast_type: str = "monthly",
    type_id: str = "U",
    geo_level: str = "all_locations",
    geo_id: str = "ALL"
):
    """Get chart data for location total."""
    table = f"walmart_aggregate_{forecast_type}"
    week_date = to_date(week)
    period_start, period_end = get_period_range(week, forecast_type)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT date, value as actual, fv as forecast, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND type_id = %s
                  AND geo_level = %s
                  AND geo_id = %s
                  AND product_level = 'total'
                  AND product_id = 'ALL'
                ORDER BY date
            ''', [period_start, period_end, type_id, geo_level, geo_id])
            rows = cur.fetchall()
    
    return [{
        'date': to_date(r['date']).isoformat(),
        'actual': float(r['actual']) if r['actual'] and to_date(r['date']) <= week_date else None,
        'forecast': float(r['forecast']) if r['forecast'] is not None else None,
        'ci85_low': float(r['ci85_low']) if r['ci85_low'] is not None else None,
        'ci85_high': float(r['ci85_high']) if r['ci85_high'] is not None else None,
        'ci95_low': float(r['ci95_low']) if r['ci95_low'] is not None else None,
        'ci95_high': float(r['ci95_high']) if r['ci95_high'] is not None else None,
    } for r in rows]


@router.get("/chart/department")
def get_chart_department(
    week: str,
    forecast_type: str = "monthly",
    type_id: str = "U",
    geo_level: str = "all_locations",
    geo_id: str = "ALL",
    department_id: str = ""
):
    """Get chart data for a department."""
    table = f"walmart_aggregate_{forecast_type}"
    week_date = to_date(week)
    period_start, period_end = get_period_range(week, forecast_type)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT date, value as actual, fv as forecast, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND type_id = %s
                  AND geo_level = %s
                  AND geo_id = %s
                  AND product_level = 'department_id'
                  AND product_id = %s
                ORDER BY date
            ''', [period_start, period_end, type_id, geo_level, geo_id, department_id])
            rows = cur.fetchall()
    
    return [{
        'date': to_date(r['date']).isoformat(),
        'actual': float(r['actual']) if r['actual'] and to_date(r['date']) <= week_date else None,
        'forecast': float(r['forecast']) if r['forecast'] is not None else None,
        'ci85_low': float(r['ci85_low']) if r['ci85_low'] is not None else None,
        'ci85_high': float(r['ci85_high']) if r['ci85_high'] is not None else None,
        'ci95_low': float(r['ci95_low']) if r['ci95_low'] is not None else None,
        'ci95_high': float(r['ci95_high']) if r['ci95_high'] is not None else None,
    } for r in rows]


@router.get("/chart/category")
def get_chart_category(
    week: str,
    forecast_type: str = "monthly",
    type_id: str = "U",
    geo_level: str = "all_locations",
    geo_id: str = "ALL",
    category_id: str = ""
):
    """Get chart data for a category."""
    table = f"walmart_aggregate_{forecast_type}"
    week_date = to_date(week)
    period_start, period_end = get_period_range(week, forecast_type)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT date, value as actual, fv as forecast, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND type_id = %s
                  AND geo_level = %s
                  AND geo_id = %s
                  AND product_level = 'category_id'
                  AND product_id = %s
                ORDER BY date
            ''', [period_start, period_end, type_id, geo_level, geo_id, category_id])
            rows = cur.fetchall()
    
    return [{
        'date': to_date(r['date']).isoformat(),
        'actual': float(r['actual']) if r['actual'] and to_date(r['date']) <= week_date else None,
        'forecast': float(r['forecast']) if r['forecast'] is not None else None,
        'ci85_low': float(r['ci85_low']) if r['ci85_low'] is not None else None,
        'ci85_high': float(r['ci85_high']) if r['ci85_high'] is not None else None,
        'ci95_low': float(r['ci95_low']) if r['ci95_low'] is not None else None,
        'ci95_high': float(r['ci95_high']) if r['ci95_high'] is not None else None,
    } for r in rows]


# =============================================================================
# SKU ENDPOINTS (CA_1 only)
# =============================================================================

@router.get("/sku-list")
def get_sku_list(forecast_type: str = "monthly"):
    """Get list of SKUs for CA_1."""
    table = f"walmart_ca_1_sku_final_{forecast_type}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT DISTINCT sku_id FROM "{table}" ORDER BY sku_id LIMIT 1000')
            return [{'sku_id': row[0]} for row in cur.fetchall()]


@router.get("/skus")
def get_skus(
    week: str,
    forecast_type: str = "monthly",
    category_id: str = "",
    limit: int = 50
):
    """Get SKU band breaks for a category."""
    table = f"walmart_ca_1_sku_final_{forecast_type}"
    week_d = to_date(week)
    window_days = 30 if forecast_type == 'monthly' else 90
    window_start = week_d - timedelta(days=window_days - 1)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT sku_id, type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND category_id = %s
                ORDER BY sku_id, type_id, date
            ''', [window_start, week_d, category_id])
            rows = cur.fetchall()
    
    sku_data = defaultdict(lambda: {'U': [], 'R': []})
    for row in rows:
        sku_data[row['sku_id']][row['type_id']].append(dict(row))
    
    result = []
    for sku_id, type_rows in sku_data.items():
        units_breaks = get_band_breaks_from_rows(type_rows.get('U', []), week_d)
        revenue_breaks = get_band_breaks_from_rows(type_rows.get('R', []), week_d)
        total_breaks = (
            units_breaks['upper_85'] + units_breaks['lower_85'] + 
            revenue_breaks['lower_85']
        )
        result.append({
            'sku_id': sku_id,
            'units': units_breaks,
            'revenue': revenue_breaks,
            '_total_breaks': total_breaks
        })
    
    result.sort(key=lambda x: -x['_total_breaks'])
    for r in result:
        del r['_total_breaks']
    
    return result[:limit]


@router.get("/chart/sku")
def get_chart_sku(
    week: str,
    forecast_type: str = "monthly",
    type_id: str = "U",
    sku_id: str = ""
):
    """Get chart data for a SKU."""
    table = f"walmart_ca_1_sku_final_{forecast_type}"
    week_date = to_date(week)
    period_start, period_end = get_period_range(week, forecast_type)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT date, value as actual, fv as forecast, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND type_id = %s
                  AND sku_id = %s
                ORDER BY date
            ''', [period_start, period_end, type_id, sku_id])
            rows = cur.fetchall()
    
    return [{
        'date': to_date(r['date']).isoformat(),
        'actual': float(r['actual']) if r['actual'] and to_date(r['date']) <= week_date else None,
        'forecast': float(r['forecast']) if r['forecast'] is not None else None,
        'ci85_low': float(r['ci85_low']) if r['ci85_low'] is not None else None,
        'ci85_high': float(r['ci85_high']) if r['ci85_high'] is not None else None,
        'ci95_low': float(r['ci95_low']) if r['ci95_low'] is not None else None,
        'ci95_high': float(r['ci95_high']) if r['ci95_high'] is not None else None,
    } for r in rows]


@router.get("/sku-info")
def get_sku_info(
    week: str,
    forecast_type: str = "monthly",
    sku_id: str = ""
):
    """Get SKU info and band breaks."""
    table = f"walmart_ca_1_sku_final_{forecast_type}"
    week_d = to_date(week)
    window_days = 30 if forecast_type == 'monthly' else 90
    window_start = week_d - timedelta(days=window_days - 1)
    
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'''
                SELECT type_id, date, value, ci85_low, ci85_high, ci95_low, ci95_high
                FROM "{table}"
                WHERE date >= %s AND date <= %s 
                  AND sku_id = %s
                ORDER BY type_id, date
            ''', [window_start, week_d, sku_id])
            rows = cur.fetchall()
    
    type_rows = {'U': [], 'R': []}
    for row in rows:
        type_rows[row['type_id']].append(dict(row))
    
    units_breaks = get_band_breaks_from_rows(type_rows.get('U', []), week_d)
    revenue_breaks = get_band_breaks_from_rows(type_rows.get('R', []), week_d)
    
    return {
        'sku_id': sku_id,
        'units': units_breaks,
        'revenue': revenue_breaks
    }


# backend/routes/views.py
# Version: 2025-10-05 v6.0 — tsf_vw_full view with Month + Span UI
# - Correct query (no joins): selects ONLY the required columns from engine.tsf_vw_full
# - HTML form uses forecast_name, month (YYYY-MM from data), and span (1/2/3 months)
# - Removes exact date inputs
#
# Endpoints:
#   GET  /views/                 -> HTML page
#   GET  /views/forecasts        -> ["NO2_Georgia", ...]
#   GET  /views/months?forecast_name=NO2_Georgia -> ["2020-06","2020-07",...]
#   POST /views/query            -> { total, rows: [...] }
#   GET  /views/export           -> CSV download

from typing import Optional, List, Dict, Tuple
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import os, datetime as dt, calendar
import psycopg
from psycopg.rows import dict_row

router = APIRouter(prefix="/views", tags=["views"])

COLS = [
    "forecast_name","date","value","model_name",
    "fv","fv_mape","fv_mean_mape","fv_mean_mape_c",
    "ci85_low","ci85_high","ci90_low","ci90_high","ci95_low","ci95_high"
]

def _db_url() -> str:
    return (
        os.getenv("ENGINE_DATABASE_URL_DIRECT")
        or os.getenv("ENGINE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    )

def _connect():
    dsn = _db_url()
    if not dsn:
        raise RuntimeError("Database URL not configured")
    return psycopg.connect(dsn, autocommit=True)

# ---------- helpers ----------

def _ym_first(ym: str) -> dt.date:
    # ym is "YYYY-MM"
    y, m = ym.split("-")
    return dt.date(int(y), int(m), 1)

def _add_months(d: dt.date, n: int) -> dt.date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    day = 1
    return dt.date(y, m, day)

def _range_from_month_span(ym: str, span: int) -> Tuple[dt.date, dt.date]:
    span = 1 if span not in (1,2,3) else span
    start = _ym_first(ym)
    stop = _add_months(start, span)  # exclusive
    return start, stop

def _select_clause() -> str:
    return ", ".join(COLS)

# ---------- HTML ----------

_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>engine.tsf_vw_full</title>
  <style>
    body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial; margin:24px;}
    .card{border:1px solid #e5e7eb; border-radius:12px; padding:16px; max-width:1100px;}
    .row{display:flex; gap:12px; align-items:flex-end; flex-wrap:wrap;}
    label{font-size:12px; color:#374151; display:block; margin-bottom:4px;}
    select,button{padding:8px 10px; border:1px solid #d1d5db; border-radius:8px;}
    table{border-collapse:collapse; width:100%; margin-top:12px; font-size:12px;}
    th,td{border-top:1px solid #e5e7eb; padding:6px 8px; white-space:nowrap; text-align:left;}
    th{background:#f9fafb; position:sticky; top:0;}
    .actions{display:flex; gap:8px;}
    .err{color:#b91c1c; margin-top:8px;}
    .muted{color:#6b7280;}
  </style>
</head>
<body>
  <div class="card">
    <h2>engine.tsf_vw_full</h2>
    <div class="row">
      <div>
        <label>Forecast (forecast_name)</label>
        <select id="forecast"></select>
      </div>
      <div>
        <label>Month</label>
        <select id="month"></select>
      </div>
      <div>
        <label>Span</label>
        <select id="span">
          <option value="1">1 month</option>
          <option value="2">2 months</option>
          <option value="3">3 months</option>
        </select>
      </div>
      <div class="actions">
        <button id="run">Run</button>
        <button id="csv">Download CSV</button>
      </div>
      <div id="status" class="muted"></div>
    </div>

    <div style="overflow:auto; max-height:65vh">
      <table>
        <thead id="thead"></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <div id="error" class="err"></div>
  </div>

<script>
const COLS = %(cols_json)s;

function el(id){ return document.getElementById(id); }

function setHeaders(){
  document.getElementById('thead').innerHTML = '<tr>' + COLS.map(h => `<th>${h}</th>`).join('') + '</tr>';
}

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

async function postJSON(url, body){
  const r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

async function loadForecasts(){
  const data = await fetchJSON('/views/forecasts');
  const s = el('forecast');
  s.innerHTML = data.map(v => `<option value="${v}">${v}</option>`).join('');
  await loadMonths();
}

async function loadMonths(){
  const fid = el('forecast').value;
  const data = await fetchJSON('/views/months?forecast_name=' + encodeURIComponent(fid));
  const s = el('month');
  s.innerHTML = data.map(v => `<option value="${v}">${v}</option>`).join('');
}

function renderRows(rows){
  const tb = el('tbody');
  tb.innerHTML = rows.map(r => '<tr>' + COLS.map(c => `<td>${(r[c] ?? '')}</td>`).join('') + '</tr>').join('');
}

async function run(){
  el('error').textContent = '';
  el('status').textContent = 'Running...';
  const payload = {
    forecast_name: el('forecast').value,
    month: el('month').value,
    span: parseInt(el('span').value)
  };
  const out = await postJSON('/views/query', payload);
  renderRows(out.rows);
  el('status').textContent = `${out.rows.length} rows`;
}

function downloadCSV(){
  const qs = new URLSearchParams({
    forecast_name: el('forecast').value,
    month: el('month').value,
    span: el('span').value
  });
  window.location = '/views/export?' + qs.toString();
}

document.addEventListener('DOMContentLoaded', async () => {
  setHeaders();
  await loadForecasts();
  el('forecast').addEventListener('change', loadMonths);
  el('run').addEventListener('click', run);
  el('csv').addEventListener('click', downloadCSV);
});
</script>
</body></html>
"""

@router.get("/", response_class=HTMLResponse)
def page():
    html = _HTML % {"cols_json": COLS}
    return HTMLResponse(html)

# API helpers for UI

@router.get("/forecasts")
def forecasts():
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT forecast_name FROM engine.tsf_vw_full ORDER BY 1")
        return [r[0] for r in cur.fetchall()]

@router.get("/months")
def months(forecast_name: str):
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT to_char(date_trunc('month', date), 'YYYY-MM') AS ym
            FROM engine.tsf_vw_full
            WHERE forecast_name = %s
            GROUP BY ym
            ORDER BY ym
        """, [forecast_name])
        return [r[0] for r in cur.fetchall()]

# Query + CSV

@router.post("/query")
def query(payload: Dict):
    forecast_name = payload.get("forecast_name") or ""
    month = payload.get("month") or ""
    span = int(payload.get("span") or 1)

    if not forecast_name or not month:
        raise HTTPException(status_code=400, detail="forecast_name and month are required")

    start, stop = _range_from_month_span(month, span)

    sql = f"""
        SELECT { _select_clause() }
        FROM engine.tsf_vw_full
        WHERE forecast_name = %s
          AND date >= %s AND date < %s
        ORDER BY date ASC
    """

    with _connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, [forecast_name, start, stop])
        rows = [dict(r) for r in cur.fetchall()]
    return {"rows": rows, "total": len(rows)}

@router.get("/export")
def export(forecast_name: str, month: str, span: int = 1):
    start, stop = _range_from_month_span(month, int(span))

    sql = f"""
        SELECT { _select_clause() }
        FROM engine.tsf_vw_full
        WHERE forecast_name = %s
          AND date >= %s AND date < %s
        ORDER BY date ASC
    """

    def row_iter():
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql, [forecast_name, start, stop])
            headers = [d.name for d in cur.description]
            yield (",".join(headers) + "\\n").encode("utf-8")
            for rec in cur:
                out = []
                for val in rec:
                    if val is None:
                        out.append("")
                    elif isinstance(val, (dt.date, dt.datetime)):
                        out.append(val.isoformat())
                    else:
                        s = str(val)
                        if any(ch in s for ch in [",","\\n",'"']):
                            s = '"' + s.replace('"','""') + '"'
                        out.append(s)
                yield (",".join(out) + "\\n").encode("utf-8")

    fname = f"tsf_vw_full_{forecast_name}_{month}_x{span}.csv".replace(" ","_")
    return StreamingResponse(row_iter(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{fname}"'})

"""
Fetch urea (HS 310210) import/export data for African countries.
Uses the official comtradeapicall package with authenticated endpoint.

Usage:
    python scripts/fetch_urea_data.py

Environment variables:
    COMTRADE_API_KEY  - Required (get free key at comtradedeveloper.un.org)
    ANTHROPIC_API_KEY - Used by analyze_with_claude.py (not needed here)
"""

import os
import sys
import json
import time
import csv
from datetime import datetime, timezone
from pathlib import Path

try:
    import comtradeapicall
except ImportError:
    print("Installing comtradeapicall...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "comtradeapicall", "-q"])
    import comtradeapicall

try:
    import pandas as pd
except ImportError:
    print("Installing pandas...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "-q"])
    import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUBSCRIPTION_KEY = os.environ.get("COMTRADE_API_KEY", "")
if not SUBSCRIPTION_KEY:
    print("ERROR: COMTRADE_API_KEY environment variable not set.")
    print("Get a free key at https://comtradedeveloper.un.org")
    sys.exit(1)

PRODUCT_CODE = "310210"  # HS6 code for Urea
YEARS = list(range(2010, 2025))  # 2010-2024
BASE_DIR = Path(__file__).parent.parent / "data"

# African country UN numeric codes
AFRICAN_COUNTRIES = {
    "12": "Algeria", "24": "Angola", "204": "Benin",
    "72": "Botswana", "854": "Burkina Faso", "108": "Burundi",
    "132": "Cabo Verde", "120": "Cameroon",
    "140": "Central African Republic", "148": "Chad",
    "174": "Comoros", "178": "Congo Rep",
    "180": "Congo Dem Rep", "384": "Cote dIvoire",
    "262": "Djibouti", "818": "Egypt",
    "226": "Equatorial Guinea", "232": "Eritrea",
    "748": "Eswatini", "231": "Ethiopia",
    "266": "Gabon", "270": "Gambia",
    "288": "Ghana", "324": "Guinea",
    "624": "Guinea-Bissau", "404": "Kenya",
    "426": "Lesotho", "430": "Liberia",
    "434": "Libya", "450": "Madagascar",
    "454": "Malawi", "466": "Mali",
    "478": "Mauritania", "480": "Mauritius",
    "504": "Morocco", "508": "Mozambique",
    "516": "Namibia", "562": "Niger",
    "566": "Nigeria", "646": "Rwanda",
    "678": "Sao Tome and Principe", "686": "Senegal",
    "690": "Seychelles", "694": "Sierra Leone",
    "706": "Somalia", "710": "South Africa",
    "728": "South Sudan", "729": "Sudan",
    "834": "Tanzania", "768": "Togo",
    "788": "Tunisia", "800": "Uganda",
    "894": "Zambia", "716": "Zimbabwe"
}


def fetch_year_batch(year: int, flow_code: str) -> pd.DataFrame:
    """
    Fetch urea data for ALL African reporters in a single API call for one year/flow.
    This is much more efficient than one call per country.
    
    flow_code: 'M' = imports, 'X' = exports
    """
    reporter_codes = ",".join(AFRICAN_COUNTRIES.keys())
    flow_name = "Import" if flow_code == "M" else "Export"
    
    print(f"  Fetching {year} {flow_name}s for all African countries...", flush=True)
    
    try:
        df = comtradeapicall.getFinalData(
            SUBSCRIPTION_KEY,
            typeCode='C',
            freqCode='A',
            clCode='HS',
            period=str(year),
            reporterCode=reporter_codes,
            cmdCode=PRODUCT_CODE,
            flowCode=flow_code,
            partnerCode='0',  # World
            partner2Code=None,
            customsCode=None,
            motCode=None,
            maxRecords=2500,
            format_output='JSON',
            aggregateBy=None,
            breakdownMode='classic',
            countOnly=None,
            includeDesc=True
        )
        
        if df is not None and not df.empty:
            print(f"    Got {len(df)} records", flush=True)
            return df
        else:
            print(f"    No data", flush=True)
            return pd.DataFrame()
            
    except Exception as e:
        print(f"    ERROR: {e}", flush=True)
        return pd.DataFrame()


def fetch_all_data():
    """Fetch all urea trade data using batched API calls (one per year/flow)."""
    
    all_frames = []
    total_calls = len(YEARS) * 2
    completed = 0
    
    print(f"Fetching urea (HS {PRODUCT_CODE}) trade data")
    print(f"Countries: {len(AFRICAN_COUNTRIES)} African nations")
    print(f"Years: {YEARS[0]}-{YEARS[-1]}")
    print(f"API calls needed: {total_calls} (batched by year)")
    print(f"API: Authenticated Comtrade endpoint")
    print("=" * 60, flush=True)
    
    for year in YEARS:
        print(f"\n{year}:", flush=True)
        
        for flow_code in ["M", "X"]:
            completed += 1
            df = fetch_year_batch(year, flow_code)
            
            if not df.empty:
                all_frames.append(df)
            
            # Rate limiting: 1 call per 2 seconds to be safe
            time.sleep(2)
        
        print(f"  Progress: {completed}/{total_calls} calls done", flush=True)
    
    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        print(f"\nTotal records fetched: {len(combined)}", flush=True)
        return combined
    else:
        print("\nNo data fetched at all!", flush=True)
        return pd.DataFrame()


def process_and_save(df: pd.DataFrame):
    """Process the raw Comtrade data and save as clean CSV."""
    
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    
    if df.empty:
        print("No data to save.")
        return
    
    # Save raw data first
    raw_path = BASE_DIR / f"africa_urea_raw_{timestamp}.csv"
    df.to_csv(raw_path, index=False)
    print(f"\nRaw data saved: {raw_path}")
    
    # Build clean output
    rows = []
    for _, rec in df.iterrows():
        reporter_code = str(rec.get("reporterCode", ""))
        country_name = AFRICAN_COUNTRIES.get(reporter_code, rec.get("reporterDesc", "Unknown"))
        
        flow_desc = rec.get("flowDesc", "")
        if "import" in str(flow_desc).lower() or rec.get("flowCode") == "M":
            trade_flow = "Import"
        else:
            trade_flow = "Export"
        
        rows.append({
            "country_un_code": reporter_code,
            "country_name": country_name,
            "year": rec.get("period", ""),
            "trade_flow": trade_flow,
            "product_code": PRODUCT_CODE,
            "product_description": "Urea",
            "partner": "World",
            "trade_value_1000usd": round(rec.get("primaryValue", 0) / 1000, 2) if rec.get("primaryValue") else None,
            "quantity_kg": rec.get("netWgt"),
            "data_source": "Comtrade API (authenticated)",
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
        })
    
    # Save clean CSV
    csv_path = BASE_DIR / f"africa_urea_trade_{timestamp}.csv"
    fieldnames = rows[0].keys()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Clean data saved: {csv_path}")
    
    # Save JSON
    json_path = BASE_DIR / f"africa_urea_trade_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"JSON saved: {json_path}")
    
    # Latest copies
    import shutil
    shutil.copy2(csv_path, BASE_DIR / "africa_urea_trade_latest.csv")
    shutil.copy2(json_path, BASE_DIR / "africa_urea_trade_latest.json")
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    import_rows = [r for r in rows if r["trade_flow"] == "Import" and r["trade_value_1000usd"]]
    export_rows = [r for r in rows if r["trade_flow"] == "Export" and r["trade_value_1000usd"]]
    
    print(f"Import records: {len(import_rows)}")
    print(f"Export records: {len(export_rows)}")
    print(f"Countries with imports: {len(set(r['country_name'] for r in import_rows))}")
    print(f"Countries with exports: {len(set(r['country_name'] for r in export_rows))}")
    
    if import_rows:
        latest_year = max(r["year"] for r in import_rows)
        latest = sorted(
            [r for r in import_rows if r["year"] == latest_year],
            key=lambda x: x["trade_value_1000usd"] or 0,
            reverse=True
        )
        print(f"\nTop 10 African urea importers ({latest_year}):")
        for i, r in enumerate(latest[:10], 1):
            val = f"${r['trade_value_1000usd']:,.1f}K"
            qty = f"{r['quantity_kg']:,.0f} Kg" if r.get('quantity_kg') else "no qty"
            print(f"  {i}. {r['country_name']}: {val} ({qty})")


if __name__ == "__main__":
    df = fetch_all_data()
    process_and_save(df)

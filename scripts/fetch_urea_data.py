"""
Fetch urea (HS 310210) import/export data for African countries from UN Comtrade Plus API.

Uses the 'preview' endpoint which requires NO API key (max 500 records per call).
For urea by single country/year, this is more than enough.

Usage:
    python scripts/fetch_urea_data.py

Environment variables:
    COMTRADE_API_KEY  - Optional (for higher limits via getFinalData endpoint)
"""

import os
import json
import time
import csv
import requests
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# African country UN numeric codes (Comtrade uses these, not ISO3)
# ---------------------------------------------------------------------------
AFRICAN_COUNTRIES = {
    "12": ("DZA", "Algeria"), "24": ("AGO", "Angola"), "204": ("BEN", "Benin"),
    "72": ("BWA", "Botswana"), "854": ("BFA", "Burkina Faso"), "108": ("BDI", "Burundi"),
    "132": ("CPV", "Cabo Verde"), "120": ("CMR", "Cameroon"),
    "140": ("CAF", "Central African Republic"), "148": ("TCD", "Chad"),
    "174": ("COM", "Comoros"), "178": ("COG", "Congo, Rep."),
    "180": ("COD", "Congo, Dem. Rep."), "384": ("CIV", "Cote d'Ivoire"),
    "262": ("DJI", "Djibouti"), "818": ("EGY", "Egypt"),
    "226": ("GNQ", "Equatorial Guinea"), "232": ("ERI", "Eritrea"),
    "748": ("SWZ", "Eswatini"), "231": ("ETH", "Ethiopia"),
    "266": ("GAB", "Gabon"), "270": ("GMB", "Gambia"),
    "288": ("GHA", "Ghana"), "324": ("GIN", "Guinea"),
    "624": ("GNB", "Guinea-Bissau"), "404": ("KEN", "Kenya"),
    "426": ("LSO", "Lesotho"), "430": ("LBR", "Liberia"),
    "434": ("LBY", "Libya"), "450": ("MDG", "Madagascar"),
    "454": ("MWI", "Malawi"), "466": ("MLI", "Mali"),
    "478": ("MRT", "Mauritania"), "480": ("MUS", "Mauritius"),
    "504": ("MAR", "Morocco"), "508": ("MOZ", "Mozambique"),
    "516": ("NAM", "Namibia"), "562": ("NER", "Niger"),
    "566": ("NGA", "Nigeria"), "646": ("RWA", "Rwanda"),
    "678": ("STP", "Sao Tome and Principe"), "686": ("SEN", "Senegal"),
    "690": ("SYC", "Seychelles"), "694": ("SLE", "Sierra Leone"),
    "706": ("SOM", "Somalia"), "710": ("ZAF", "South Africa"),
    "728": ("SSD", "South Sudan"), "729": ("SDN", "Sudan"),
    "834": ("TZA", "Tanzania"), "768": ("TGO", "Togo"),
    "788": ("TUN", "Tunisia"), "800": ("UGA", "Uganda"),
    "894": ("ZMB", "Zambia"), "716": ("ZWE", "Zimbabwe")
}

PRODUCT_CODE = "310210"  # HS6 code for Urea
YEARS = list(range(2010, 2025))  # 2010-2024
BASE_DIR = Path(__file__).parent.parent / "data"

# New Comtrade Plus API endpoints
COMTRADE_PREVIEW_URL = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
COMTRADE_FINAL_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"


def fetch_comtrade_preview(reporter_code: str, year: int, flow_code: str = "M") -> list:
    """
    Fetch trade data using Comtrade Plus preview API (NO key required).
    Limited to 500 records per call.
    
    flow_code: 'M' = imports, 'X' = exports
    Returns list of matching records.
    """
    params = {
        "reporterCode": reporter_code,
        "period": str(year),
        "partnerCode": "0",       # 0 = World
        "cmdCode": PRODUCT_CODE,
        "flowCode": flow_code,
        "maxRecords": 500,
        "format": "JSON",
        "breakdownMode": "classic",
        "includeDesc": "true",
    }
    
    try:
        resp = requests.get(COMTRADE_PREVIEW_URL, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("data") and len(data["data"]) > 0:
                return data["data"]
            else:
                return []
        elif resp.status_code == 429:
            print(f"    Rate limited, waiting 5s...")
            time.sleep(5)
            return fetch_comtrade_preview(reporter_code, year, flow_code)
        else:
            print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
    except requests.exceptions.Timeout:
        print(f"    Timeout for {reporter_code}/{year}")
    except json.JSONDecodeError:
        print(f"    Invalid JSON response for {reporter_code}/{year}")
    except Exception as e:
        print(f"    Error for {reporter_code}/{year}: {e}")
    
    return []


def fetch_all_african_urea_data():
    """
    Main function: iterate over all African countries and years,
    fetching import and export data for urea (310210).
    """
    
    results = []
    countries = sorted(AFRICAN_COUNTRIES.items(), key=lambda x: x[1][1])  # Sort by name
    total_requests = len(countries) * len(YEARS) * 2
    completed = 0
    
    print(f"Fetching urea trade data for {len(countries)} African countries")
    print(f"Years: {YEARS[0]}-{YEARS[-1]}")
    print(f"Product: {PRODUCT_CODE} (Urea)")
    print(f"API: Comtrade Plus preview (no key required)")
    print(f"Total API calls needed: ~{total_requests}")
    print("=" * 60)
    
    for un_code, (iso3, country_name) in countries:
        print(f"\n{country_name} ({iso3}, UN:{un_code})...")
        
        for year in YEARS:
            for flow, flow_code in [("Import", "M"), ("Export", "X")]:
                completed += 1
                
                row = {
                    "country_iso3": iso3,
                    "country_un_code": un_code,
                    "country_name": country_name,
                    "year": year,
                    "trade_flow": flow,
                    "product_code": PRODUCT_CODE,
                    "product_description": "Urea",
                    "partner": "World",
                    "trade_value_1000usd": None,
                    "quantity_kg": None,
                    "data_source": None,
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                }
                
                records = fetch_comtrade_preview(un_code, year, flow_code)
                
                if records:
                    rec = records[0]
                    trade_val = rec.get("primaryValue")
                    if trade_val is not None:
                        row["trade_value_1000usd"] = round(trade_val / 1000, 2)
                    row["quantity_kg"] = rec.get("netWgt")
                    row["data_source"] = "Comtrade Plus API"
                    
                    val_str = f"${row['trade_value_1000usd']:,.1f}K" if row['trade_value_1000usd'] else "N/A"
                    qty_str = f"{row['quantity_kg']:,.0f} Kg" if row.get('quantity_kg') else "no qty"
                    print(f"  {year} {flow}: {val_str} ({qty_str})")
                else:
                    row["data_source"] = "no_data"
                
                results.append(row)
                
                # Rate limiting
                time.sleep(1.2)
                
                if completed % 100 == 0:
                    print(f"  ... Progress: {completed}/{total_requests} requests")
    
    return results


def save_results(results: list):
    """Save results to CSV and JSON."""
    
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    
    csv_path = BASE_DIR / f"africa_urea_trade_{timestamp}.csv"
    if results:
        fieldnames = results[0].keys()
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nSaved CSV: {csv_path}")
    
    json_path = BASE_DIR / f"africa_urea_trade_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved JSON: {json_path}")
    
    import shutil
    shutil.copy2(csv_path, BASE_DIR / "africa_urea_trade_latest.csv")
    shutil.copy2(json_path, BASE_DIR / "africa_urea_trade_latest.json")
    
    return csv_path, json_path


def print_summary(results: list):
    """Print a quick summary of what was fetched."""
    
    imports = [r for r in results if r["trade_flow"] == "Import" and r["trade_value_1000usd"]]
    exports = [r for r in results if r["trade_flow"] == "Export" and r["trade_value_1000usd"]]
    no_data = [r for r in results if r["data_source"] == "no_data"]
    
    print("\n" + "=" * 60)
    print("FETCH SUMMARY")
    print("=" * 60)
    print(f"Total records: {len(results)}")
    print(f"Import records with data: {len(imports)}")
    print(f"Export records with data: {len(exports)}")
    print(f"Records with no data: {len(no_data)}")
    
    if imports:
        countries_with_imports = set(r["country_name"] for r in imports)
        print(f"\nCountries with import data: {len(countries_with_imports)}")
        
        latest_year = max(r["year"] for r in imports)
        latest_imports = [r for r in imports if r["year"] == latest_year]
        latest_imports.sort(key=lambda x: x["trade_value_1000usd"] or 0, reverse=True)
        
        print(f"\nTop 10 African urea importers ({latest_year}):")
        for i, r in enumerate(latest_imports[:10], 1):
            val = f"${r['trade_value_1000usd']:,.1f}K" if r['trade_value_1000usd'] else "N/A"
            qty = f"{r['quantity_kg']:,.0f} Kg" if r.get('quantity_kg') else "no qty"
            print(f"  {i}. {r['country_name']}: {val} ({qty})")


if __name__ == "__main__":
    results = fetch_all_african_urea_data()
    csv_path, json_path = save_results(results)
    print_summary(results)

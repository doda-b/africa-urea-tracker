"""
Fetch urea (HS 310210) import data for African countries from UN Comtrade API.
Also fetches export data to approximate domestic production/re-export patterns.

Data sources:
- UN Comtrade API (via WITS Comtrade pages as fallback)
- Trade value in 1000 USD
- Quantity in Kg (where reported)

Usage:
    python scripts/fetch_urea_data.py
    
Environment variables:
    ANTHROPIC_API_KEY  - Required for Claude analysis step
    COMTRADE_API_KEY   - Optional, for UN Comtrade direct API (higher rate limits)
"""

import os
import json
import time
import csv
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# African country ISO3 codes
# ---------------------------------------------------------------------------
AFRICAN_COUNTRIES = {
    "DZA": "Algeria", "AGO": "Angola", "BEN": "Benin", "BWA": "Botswana",
    "BFA": "Burkina Faso", "BDI": "Burundi", "CPV": "Cabo Verde", "CMR": "Cameroon",
    "CAF": "Central African Republic", "TCD": "Chad", "COM": "Comoros",
    "COG": "Congo, Rep.", "COD": "Congo, Dem. Rep.", "CIV": "Cote d'Ivoire",
    "DJI": "Djibouti", "EGY": "Egypt", "GNQ": "Equatorial Guinea",
    "ERI": "Eritrea", "SWZ": "Eswatini", "ETH": "Ethiopia", "GAB": "Gabon",
    "GMB": "Gambia", "GHA": "Ghana", "GIN": "Guinea", "GNB": "Guinea-Bissau",
    "KEN": "Kenya", "LSO": "Lesotho", "LBR": "Liberia", "LBY": "Libya",
    "MDG": "Madagascar", "MWI": "Malawi", "MLI": "Mali", "MRT": "Mauritania",
    "MUS": "Mauritius", "MAR": "Morocco", "MOZ": "Mozambique", "NAM": "Namibia",
    "NER": "Niger", "NGA": "Nigeria", "RWA": "Rwanda", "STP": "Sao Tome and Principe",
    "SEN": "Senegal", "SYC": "Seychelles", "SLE": "Sierra Leone", "SOM": "Somalia",
    "ZAF": "South Africa", "SSD": "South Sudan", "SDN": "Sudan",
    "TZA": "Tanzania", "TGO": "Togo", "TUN": "Tunisia", "UGA": "Uganda",
    "ZMB": "Zambia", "ZWE": "Zimbabwe"
}

PRODUCT_CODE = "310210"  # HS6 code for Urea
YEARS = list(range(2010, 2025))  # 2010-2024
BASE_DIR = Path(__file__).parent.parent / "data"


def fetch_comtrade_data(reporter_iso3: str, year: int, trade_flow: str = "M") -> dict | None:
    """
    Fetch trade data from UN Comtrade API v1 (public, no key needed for basic access).
    
    trade_flow: 'M' = imports, 'X' = exports
    Returns dict with trade_value, quantity, quantity_unit or None if no data.
    """
    # UN Comtrade public API endpoint
    url = "https://comtrade.un.org/api/get"
    params = {
        "max": 500,
        "type": "C",        # Commodities
        "freq": "A",        # Annual
        "px": "HS",         # HS classification
        "ps": str(year),
        "r": reporter_iso3, # Reporter (ISO3 or UN numeric)
        "p": "0",           # Partner = World
        "rg": "1" if trade_flow == "M" else "2",  # 1=Import, 2=Export
        "cc": PRODUCT_CODE,
        "fmt": "json"
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("dataset") and len(data["dataset"]) > 0:
                record = data["dataset"][0]
                return {
                    "trade_value_usd": record.get("TradeValue"),
                    "quantity": record.get("NetWeight") or record.get("qty"),
                    "quantity_unit": record.get("qtDesc", "Kg"),
                }
    except Exception as e:
        print(f"  Comtrade API error for {reporter_iso3}/{year}: {e}")
    
    return None


def fetch_wits_tradestats(reporter_iso3: str, year: int, indicator: str) -> dict | None:
    """
    Fetch from WITS TradeStats API (URL-based structure).
    This works for aggregate product groups but we try it for completeness.
    
    indicator examples:
      MPRT-TRD-VL = Import Trade Value
      XPRT-TRD-VL = Export Trade Value  
    """
    url = (
        f"https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-trade"
        f"/reporter/{reporter_iso3.lower()}/year/{year}"
        f"/partner/wld/product/Chemicals/indicator/{indicator}"
        f"?format=JSON"
    )
    
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            # Parse SDMX JSON response
            if "dataSets" in data.get("structure", {}):
                return data
    except Exception:
        pass
    
    return None


def fetch_via_wits_comtrade_page(country_iso3: str, year: int, trade_flow: str = "Imports") -> dict | None:
    """
    Scrape the WITS Comtrade summary page for a specific country/year/product.
    This is the same data you see on the WITS website.
    
    URL pattern: 
    https://wits.worldbank.org/trade/comtrade/en/country/{ISO3}/year/{YEAR}/tradeflow/{Flow}/partner/WLD/product/310210
    """
    url = (
        f"https://wits.worldbank.org/trade/comtrade/en/country/{country_iso3}"
        f"/year/{year}/tradeflow/{trade_flow}/partner/ALL/product/{PRODUCT_CODE}"
    )
    
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            text = resp.text
            # Look for the data table in the HTML
            # The page contains a table with partner-level breakdowns
            # For now we extract the World total from the summary text
            
            # Quick parse: look for trade value patterns in the HTML
            import re
            
            # The page has data in a structured format, try to find value
            # Pattern: "Trade Value 1000USD" followed by numbers
            value_match = re.search(
                r'World.*?(\d[\d,]*\.?\d*)\s*(?:Kg|kg)',
                text, re.DOTALL
            )
            
            # Alternative: look for the summary paragraph
            summary_match = re.search(
                r'In \d{4}.*?(\$[\d,]+\.?\d*K)',
                text
            )
            
            if summary_match:
                return {"raw_html_summary": summary_match.group(0)[:500], "url": url}
    except Exception as e:
        print(f"  WITS page error for {country_iso3}/{year}: {e}")
    
    return None


def fetch_all_african_urea_data():
    """
    Main function: iterate over all African countries and years,
    fetching import and export data for urea (310210).
    
    Strategy:
    1. Try UN Comtrade API first (most reliable for HS6 data)
    2. Fall back to WITS Comtrade page scraping if needed
    3. Respect rate limits (1 request per second for free tier)
    """
    
    results = []
    total_requests = len(AFRICAN_COUNTRIES) * len(YEARS) * 2  # imports + exports
    completed = 0
    
    print(f"Fetching urea trade data for {len(AFRICAN_COUNTRIES)} African countries")
    print(f"Years: {YEARS[0]}-{YEARS[-1]}")
    print(f"Product: {PRODUCT_CODE} (Urea)")
    print(f"Total API calls needed: ~{total_requests}")
    print("=" * 60)
    
    for iso3, country_name in sorted(AFRICAN_COUNTRIES.items()):
        print(f"\n{country_name} ({iso3})...")
        
        for year in YEARS:
            for flow, flow_code in [("Import", "M"), ("Export", "X")]:
                completed += 1
                
                row = {
                    "country_iso3": iso3,
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
                
                # Try UN Comtrade API
                data = fetch_comtrade_data(iso3, year, flow_code)
                if data and data.get("trade_value_usd") is not None:
                    row["trade_value_1000usd"] = round(data["trade_value_usd"] / 1000, 2)
                    if data.get("quantity"):
                        row["quantity_kg"] = data["quantity"]
                    row["data_source"] = "UN Comtrade API"
                    print(f"  {year} {flow}: ${row['trade_value_1000usd']}K", end="")
                    if row["quantity_kg"]:
                        print(f" | {row['quantity_kg']:,.0f} Kg", end="")
                    print()
                else:
                    row["data_source"] = "no_data"
                    
                results.append(row)
                
                # Rate limiting: Comtrade free tier = ~1 req/sec
                time.sleep(1.1)
                
                # Progress update every 50 requests
                if completed % 50 == 0:
                    print(f"  ... Progress: {completed}/{total_requests} requests")
    
    return results


def save_results(results: list):
    """Save results to CSV and JSON."""
    
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    
    # Save as CSV
    csv_path = BASE_DIR / f"africa_urea_trade_{timestamp}.csv"
    if results:
        fieldnames = results[0].keys()
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nSaved CSV: {csv_path}")
    
    # Save as JSON
    json_path = BASE_DIR / f"africa_urea_trade_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved JSON: {json_path}")
    
    # Also save a "latest" symlink-style copy
    latest_csv = BASE_DIR / "africa_urea_trade_latest.csv"
    latest_json = BASE_DIR / "africa_urea_trade_latest.json"
    
    import shutil
    shutil.copy2(csv_path, latest_csv)
    shutil.copy2(json_path, latest_json)
    
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
        
        # Top importers by most recent year
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

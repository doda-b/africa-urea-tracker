"""
Fetch bilateral fertiliser trade flows for African countries — for the import-flows maps.

Pulls imports of all three core nutrients (urea/N, potash/K, phosphates/P) for every
African country, BROKEN DOWN BY PARTNER (origin country), so we can see WHERE the
fertiliser comes from — not just how much. Aggregates across all African importers to
produce an "arrows table": source country -> Africa, by nutrient, in USD and Kg.

Difference vs the old urea script:
  - partnerCode=None (all partners) instead of '0' (World) -> gives bilateral origins
  - three HS codes instead of one
  - 2023 only, imports only (this is for the flow map, not the time series)
  - adds an aggregation step that sums by partner -> the arrow weights

Usage:
    python scripts/fetch_fertiliser_flows.py

Environment variables:
    COMTRADE_API_KEY  - Required (free key at comtradedeveloper.un.org)
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

# HS codes for the three nutrients.
# HS4 for potash & phosphates (captures all sub-types in one call);
# HS6 for urea (the specific nitrogen product, matching the old tracker).
PRODUCT_CODES = {
    "310210": "Urea (N)",
    "3104":   "Potassic fertilisers (K)",
    "3103":   "Phosphatic fertilisers (P)",
}

YEAR = 2023            # single year for the flow map (matches FAOSTAT bubble year)
FLOW_CODE = "M"        # imports only — we want what flows INTO Africa
BASE_DIR = Path(__file__).parent.parent / "data"

# African country UN numeric codes (reporters)
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

AFRICAN_CODE_SET = set(AFRICAN_COUNTRIES.keys())


def fetch_nutrient(product_code: str, product_label: str) -> pd.DataFrame:
    """
    Fetch one nutrient's imports for ALL African reporters, broken down by partner.
    partnerCode=None returns every trading partner (origin), which is what gives us arrows.
    """
    reporter_codes = ",".join(AFRICAN_COUNTRIES.keys())
    print(f"\n  Fetching {product_label} (HS {product_code}) imports, all partners, {YEAR}...", flush=True)

    try:
        df = comtradeapicall.getFinalData(
            SUBSCRIPTION_KEY,
            typeCode='C',
            freqCode='A',
            clCode='HS',
            period=str(YEAR),
            reporterCode=reporter_codes,
            cmdCode=product_code,
            flowCode=FLOW_CODE,
            partnerCode=None,      # <<< all partners — the bilateral breakdown
            partner2Code=None,
            customsCode=None,
            motCode=None,
            maxRecords=100000,     # large — many reporter x partner rows
            format_output='JSON',
            aggregateBy=None,
            breakdownMode='classic',
            countOnly=None,
            includeDesc=True
        )

        if df is not None and not df.empty:
            df["nutrient"] = product_label
            df["hs_code"] = product_code
            print(f"    Got {len(df)} reporter x partner records", flush=True)
            return df
        else:
            print(f"    No data", flush=True)
            return pd.DataFrame()

    except Exception as e:
        print(f"    ERROR: {e}", flush=True)
        return pd.DataFrame()


def fetch_all():
    """Fetch all three nutrients (one call each)."""
    print("Fetching bilateral fertiliser import flows into Africa")
    print(f"Reporters: {len(AFRICAN_COUNTRIES)} African nations")
    print(f"Year: {YEAR} | Flow: imports | Partners: ALL (bilateral)")
    print(f"Nutrients: {', '.join(PRODUCT_CODES.values())}")
    print("=" * 60, flush=True)

    frames = []
    for code, label in PRODUCT_CODES.items():
        df = fetch_nutrient(code, label)
        if not df.empty:
            frames.append(df)
        time.sleep(2)  # rate limiting between nutrient calls

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        print(f"\nTotal records: {len(combined)}", flush=True)
        return combined
    print("\nNo data fetched.", flush=True)
    return pd.DataFrame()


def process_and_save(df: pd.DataFrame):
    """Save raw rows, then build the aggregated ARROWS table (source -> Africa)."""
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")

    if df.empty:
        print("No data to save.")
        return

    # --- 1. Save raw reporter x partner rows ---
    raw_path = BASE_DIR / f"africa_fert_flows_raw_{ts}.csv"
    df.to_csv(raw_path, index=False)
    print(f"\nRaw data saved: {raw_path}")

    # --- 2. Build clean per-flow rows ---
    rows = []
    for _, rec in df.iterrows():
        rep_code = str(rec.get("reporterCode", ""))
        par_code = str(rec.get("partnerCode", ""))
        par_name = rec.get("partnerDesc", "")

        # Skip the "World" partner aggregate (code 0) — we only want named origins,
        # otherwise we'd double count.
        if par_code == "0":
            continue
        # Skip intra-reporter / "areas nes" noise if partner unknown
        rows.append({
            "nutrient": rec.get("nutrient", ""),
            "hs_code": rec.get("hs_code", ""),
            "importer_code": rep_code,
            "importer": AFRICAN_COUNTRIES.get(rep_code, rec.get("reporterDesc", "Unknown")),
            "source_code": par_code,
            "source": par_name,
            "year": rec.get("period", ""),
            "value_usd": rec.get("primaryValue", 0) or 0,
            "qty_kg": rec.get("netWgt", None),
            "source_is_african": par_code in AFRICAN_CODE_SET,
        })

    clean_path = BASE_DIR / f"africa_fert_flows_clean_{ts}.csv"
    with open(clean_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"Clean per-flow data saved: {clean_path}")

    # --- 3. THE ARROWS TABLE: aggregate by (nutrient, source) across all African importers ---
    cdf = pd.DataFrame(rows)
    arrows = (cdf.groupby(["nutrient", "source", "source_code", "source_is_african"], as_index=False)
                 .agg(total_value_usd=("value_usd", "sum"),
                      total_qty_kg=("qty_kg", "sum"),
                      n_importers=("importer", "nunique")))
    # rank within each nutrient by value
    arrows = arrows.sort_values(["nutrient", "total_value_usd"], ascending=[True, False])

    arrows_path = BASE_DIR / f"africa_fert_ARROWS_{ts}.csv"
    arrows.to_csv(arrows_path, index=False)
    print(f"ARROWS table saved: {arrows_path}")

    import shutil
    shutil.copy2(arrows_path, BASE_DIR / "africa_fert_ARROWS_latest.csv")
    shutil.copy2(clean_path, BASE_DIR / "africa_fert_flows_clean_latest.csv")

    # --- 4. Print the headline arrows per nutrient ---
    print("\n" + "=" * 60)
    print("TOP SOURCES -> AFRICA (the arrows), 2023")
    print("=" * 60)
    for nutrient in arrows["nutrient"].unique():
        sub = arrows[arrows["nutrient"] == nutrient].head(8)
        total = arrows[arrows["nutrient"] == nutrient]["total_value_usd"].sum()
        print(f"\n{nutrient} — total imports ${total/1e6:,.0f}M:")
        for _, r in sub.iterrows():
            share = 100 * r["total_value_usd"] / total if total else 0
            flag = " (African)" if r["source_is_african"] else ""
            kg = f"{r['total_qty_kg']/1e6:,.0f}k t" if pd.notna(r['total_qty_kg']) else "n/a"
            print(f"  {r['source'][:28]:28s} ${r['total_value_usd']/1e6:7,.1f}M  ({share:4.1f}%)  {kg}{flag}")


if __name__ == "__main__":
    df = fetch_all()
    process_and_save(df)

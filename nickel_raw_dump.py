#!/usr/bin/env python3
"""
Raw UN Comtrade dump — Indonesia nickel exports, 2012-2024. No aggregation.

One row per (year, HS code): value (USD), net weight (kg) and quantity exactly as
reported. Pulls every nickel 6-digit line PLUS the 4-digit and chapter aggregates,
so you can check the hierarchy yourself (filter on the aggrLevel column):
  aggrLevel 6 = atomic lines   |   4 = headings   |   2 = chapter total (75)
The 6-digit chapter-75 lines sum to the 4-digit, which sum to 75. 720260 is in
chapter 72 (not 75), so it never overlaps the chapter-75 codes.

Requires env var COMTRADE_KEY.  Deps: requests pandas
Run:  COMTRADE_KEY=xxxx python nickel_raw_dump.py   ->  indonesia_nickel_raw.csv
"""
import os, time, requests
import pandas as pd

KEY = os.environ["COMTRADE_KEY"]
BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
REPORTER = "360"                       # Indonesia
YEARS = list(range(2012, 2025))

CODES = [
    # ---- chapter 72 (ferro-alloys) ----
    "720260",                                       # ferro-nickel / NPI
    # ---- chapter 75, 6-digit atomic lines ----
    "750110", "750120",                             # mattes / intermediate products
    "750210", "750220",                             # unwrought: unalloyed / alloyed
    "750300", "750400",                             # waste & scrap / powders & flakes
    "750511", "750512", "750521", "750522",         # bars, rods, profiles, wire
    "750610", "750620",                             # plates, sheets, strip, foil
    "750711", "750712", "750720",                   # tubes, pipes, fittings
    "750800",                                       # other articles of nickel
    # ---- chapter 28: battery-grade nickel chemicals ----
    "283324", "282540",                             # nickel sulphates / oxides & hydroxides
    # ---- aggregates, for cross-checking the roll-up (do NOT add to the above) ----
    "7501", "7502", "7503", "7504", "7505", "7506", "7507", "7508", "75",
]

# Preferred column order; any other fields Comtrade returns are appended untouched.
LEAD = ["period", "reporterCode", "reporterDesc", "flowCode", "flowDesc",
        "cmdCode", "cmdDesc", "aggrLevel", "partnerCode", "partnerDesc",
        "primaryValue", "fobvalue", "netWgt", "grossWgt",
        "qty", "qtyUnitAbbr", "altQty", "altQtyUnitAbbr"]


def comtrade(cmds, periods):
    params = {"reporterCode": REPORTER, "flowCode": "X",
              "period": ",".join(map(str, periods)), "cmdCode": ",".join(cmds),
              "partnerCode": "0", "partner2Code": "0", "customsCode": "C00", "motCode": "0"}
    for a in range(6):
        r = requests.get(BASE, params=params,
                         headers={"Ocp-Apim-Subscription-Key": KEY}, timeout=120)
        if r.status_code == 429:
            w = int(r.headers.get("Retry-After", 0)) or 6 * (a + 1)
            print(f"[429] rate-limited; waiting {w}s ({a + 1}/6)"); time.sleep(w); continue
        r.raise_for_status()
        return r.json().get("data") or []
    raise RuntimeError("Comtrade kept returning 429 after 6 retries")


records = []
for i in range(0, len(YEARS), 12):              # <=12 periods per call
    records += comtrade(CODES, YEARS[i:i + 12]); time.sleep(2)

df = pd.DataFrame(records)
cols = [c for c in LEAD if c in df.columns] + [c for c in df.columns if c not in LEAD]
df = df[cols].sort_values(["period", "aggrLevel", "cmdCode"] if "aggrLevel" in df.columns
                          else ["period", "cmdCode"])
df.to_csv("indonesia_nickel_raw.csv", index=False)
print(f"wrote indonesia_nickel_raw.csv - {len(df)} rows, "
      f"{df['cmdCode'].nunique()} codes, {df['period'].min()}-{df['period'].max()}")
print(df[[c for c in ['period','cmdCode','cmdDesc','primaryValue','netWgt','qty','qtyUnitAbbr']
          if c in df.columns]].head(25).to_string(index=False))

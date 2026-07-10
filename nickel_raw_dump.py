#!/usr/bin/env python3
"""
Raw UN Comtrade dump — Indonesia nickel exports, 2008-2024. No aggregation.

One row per (year, HS code): value (USD), net weight (kg) and quantity exactly as
reported. Includes the raw-ORE line (260400) so you can show the substitution story:
ore exports climbing into 2014, collapsing at the ban, ferro-nickel/NPI surging to
fill the gap. Also pulls the 4-digit and chapter aggregates so you can verify the
roll-up yourself (filter on aggrLevel):
  aggrLevel 6 = atomic lines   |   4 = headings   |   2 = chapter total (75)
The 6-digit chapter-75 lines sum to the 4-digit, which sum to 75. 260400 (ch.26)
and 720260 (ch.72) are their own chapters and never overlap the chapter-75 codes.

Requires env var COMTRADE_KEY.  Deps: requests pandas
Run:  COMTRADE_KEY=xxxx python nickel_raw_dump.py   ->  indonesia_nickel_raw.csv
"""
import os, time, requests
import pandas as pd

KEY = os.environ["COMTRADE_KEY"]
BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
REPORTER = "360"                       # Indonesia
YEARS = list(range(2008, 2025))        # 2008..2024 — back far enough to catch the ore ramp

CODES = [
    # ---- chapter 26 (ores) ----
    "260400",                                       # nickel ores & concentrates (the raw-ore export)
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

# quick peek at the substitution pair: ore (260400) vs ferro-nickel/NPI (720260)
sub = df[df["cmdCode"].astype(str).isin(["260400", "720260"])]
show = [c for c in ["period", "cmdCode", "cmdDesc", "primaryValue", "netWgt"] if c in sub.columns]
print(sub[show].to_string(index=False))

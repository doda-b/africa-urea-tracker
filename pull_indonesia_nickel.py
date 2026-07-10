#!/usr/bin/env python3
"""
Rebuild AfDB's "Indonesia's Exports of Nickel Products" chart from UN Comtrade.

Pulls Indonesia (reporter 360) annual EXPORTS, 2012-2024, maps HS codes to the
AfDB legend, and writes a tidy CSV + an AFC-themed stacked-bar + share-line PNG.

Rate-limit safe: batches all years into ~2 calls (not 13) and retries on HTTP 429.
Requires env var COMTRADE_KEY  (set in the workflow from your repo secret).
Run locally:   COMTRADE_KEY=xxxx python pull_indonesia_nickel.py
"""

import os
import time
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

KEY = os.environ["COMTRADE_KEY"]
BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"   # commodities / annual / HS
REPORTER = "360"                       # Indonesia
YEARS = list(range(2012, 2025))        # 2012..2024

# HS codes to pull (mixed 2/4/6-digit levels are fine in one request):
#   720260  ferro-nickel / NPI          -> green   (also the numerator for the share line)
#   750110  nickel matte                -> dark green
#   750120  high-purity intermediates   -> yellow  (nickel oxide sinters, MHP, etc.)
#   7502    unwrought nickel (4-digit)   -> orange  (= 750210 + 750220)
#   75      chapter-75 total            -> used to derive "Other articles" as a residual
PULL_CODES = "720260,750110,750120,7502,75"


def comtrade(reporter, cmd, periods):
    """One Comtrade call for a *list* of periods. Retries on 429 with backoff."""
    params = {
        "reporterCode": reporter,
        "flowCode": "X",                                  # exports
        "period": ",".join(str(p) for p in periods),      # all years in ONE call
        "cmdCode": cmd,
        "partnerCode": "0",                               # World = all partners
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
    }
    for attempt in range(6):
        r = requests.get(BASE, params=params,
                         headers={"Ocp-Apim-Subscription-Key": KEY}, timeout=120)
        if r.status_code == 429:                           # rate-limited -> wait & retry
            wait = int(r.headers.get("Retry-After", 0)) or 6 * (attempt + 1)
            print(f"[429] rate-limited; waiting {wait}s (attempt {attempt + 1}/6)")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json().get("data") or []
    raise RuntimeError("Comtrade kept returning 429 after 6 retries")


def chunks(seq, n=12):
    """Comtrade caps periods per call, so pull in groups of <=12 years."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def pull(reporter, cmd):
    data = []
    for grp in chunks(YEARS, 12):
        data += comtrade(reporter, cmd, grp)
        time.sleep(2)                                      # gentle gap between chunks
    return data


# --- 1. Indonesia exports by product (the bars) -----------------------------
rows = [{"year": int(d["period"]), "cmd": str(d["cmdCode"]), "usd": d.get("primaryValue") or 0}
        for d in pull(REPORTER, PULL_CODES)]

piv = (pd.DataFrame(rows)
       .pivot_table(index="year", columns="cmd", values="usd", aggfunc="sum")
       .reindex(YEARS).fillna(0))


def col(c):
    return piv[c] if c in piv.columns else pd.Series(0.0, index=piv.index)


out = pd.DataFrame(index=piv.index)
out["Ferro-nickel & NPI"]        = col("720260")
out["Nickel matte"]              = col("750110")
out["High-purity intermediates"] = col("750120")
out["Unwrought nickel"]          = col("7502")
out["Other articles of nickel"]  = (col("75") - col("750110")
                                    - col("750120") - col("7502")).clip(lower=0)
out = out / 1e6   # USD -> US$ million, to match the chart

# --- 2. Share of global FeNi (720260) exports (optional line) ---------------
# World total = sum of every reporter's 720260 exports. Fails soft: if reporterCode="all"
# is rejected or rate-limited, the bars are still produced with no share line.
share = pd.Series(index=piv.index, dtype=float)
try:
    wr = {}
    for d in pull("all", "720260"):
        y = int(d["period"])
        wr[y] = wr.get(y, 0) + (d.get("primaryValue") or 0)
    for y in YEARS:
        share[y] = (col("720260")[y] / wr[y] * 100) if wr.get(y) else float("nan")
except Exception as e:                       # noqa: BLE001
    print(f"[warn] share line skipped ({e}); bars still produced")
    share[:] = float("nan")

out.assign(share_global_feni_pct=share).to_csv("indonesia_nickel_exports.csv")
print(out.assign(share_global_feni_pct=share).round(0))

# --- 3. AFC-themed chart ----------------------------------------------------
AFC = ["#192A67", "#0071B9", "#0096D6", "#00B0AD", "#005B82"]   # AFC series order
PURPLE = "#4D2379"
plt.rcParams["font.family"] = "Poppins"      # silently falls back if not installed

fig, ax1 = plt.subplots(figsize=(10, 6))
bottom = pd.Series(0.0, index=out.index)
for cat, c in zip(out.columns, AFC):
    ax1.bar(out.index, out[cat], bottom=bottom, color=c, label=cat, width=0.7)
    bottom += out[cat]
ax1.set_ylabel("Exports (US$ million)")
ax1.set_xticks(YEARS)

ax2 = ax1.twinx()
if share.notna().any():
    ax2.plot(out.index, share, color=PURPLE, lw=2.5, label="Share of global FeNi exports")
ax2.set_ylabel("Share of global FeNi exports (%)")
ax2.set_ylim(0, 40)

h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8, frameon=False)
ax1.set_title("Indonesia's Exports of Nickel Products, 2012-2024 (US$ million)",
              loc="left", fontweight="medium")
fig.tight_layout()
fig.savefig("indonesia_nickel_exports.png", dpi=200, bbox_inches="tight")
print("wrote indonesia_nickel_exports.csv and indonesia_nickel_exports.png")

"""
Analyze urea trade data using Claude API.
Reads the fetched CSV data and sends it to Claude for intelligent analysis.

Usage:
    python scripts/analyze_with_claude.py

Environment variables:
    ANTHROPIC_API_KEY - Required
"""

import os
import json
import csv
import requests
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "data"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def load_latest_data() -> str:
    """Load the latest CSV data and return as a formatted string for Claude."""
    csv_path = DATA_DIR / "africa_urea_trade_latest.csv"
    
    if not csv_path.exists():
        raise FileNotFoundError(f"No data file found at {csv_path}. Run fetch_urea_data.py first.")
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Create a summary for Claude (full CSV could be too large)
    # Group by country and year, focus on imports
    imports = [r for r in rows if r["trade_flow"] == "Import" and r["trade_value_1000usd"]]
    exports = [r for r in rows if r["trade_flow"] == "Export" and r["trade_value_1000usd"]]
    
    summary_lines = ["AFRICAN UREA (HS 310210) TRADE DATA", "=" * 50, ""]
    
    # Imports table
    summary_lines.append("IMPORTS (Trade Value in $1000 USD):")
    summary_lines.append(f"{'Country':<25} {'Year':<6} {'Value ($K)':<15} {'Quantity (Kg)':<20}")
    summary_lines.append("-" * 70)
    for r in sorted(imports, key=lambda x: (x["country_name"], x["year"])):
        qty = r.get("quantity_kg", "N/A") or "N/A"
        summary_lines.append(
            f"{r['country_name']:<25} {r['year']:<6} {r['trade_value_1000usd']:<15} {qty:<20}"
        )
    
    summary_lines.append("")
    summary_lines.append("EXPORTS (Trade Value in $1000 USD):")
    summary_lines.append(f"{'Country':<25} {'Year':<6} {'Value ($K)':<15} {'Quantity (Kg)':<20}")
    summary_lines.append("-" * 70)
    for r in sorted(exports, key=lambda x: (x["country_name"], x["year"])):
        qty = r.get("quantity_kg", "N/A") or "N/A"
        summary_lines.append(
            f"{r['country_name']:<25} {r['year']:<6} {r['trade_value_1000usd']:<15} {qty:<20}"
        )
    
    return "\n".join(summary_lines)


def analyze_with_claude(data_summary: str) -> str:
    """Send data to Claude API for analysis."""
    
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    system_prompt = """You are an expert trade data analyst specializing in African agricultural 
inputs markets, particularly fertilizers. You have deep knowledge of:
- African agriculture and fertilizer demand patterns
- Urea production facilities in Africa (e.g., Nigeria's Dangote/Indorama plants, 
  Egypt's facilities, Algeria's Fertial/AOA)
- Trade routes and logistics for fertilizer in Africa
- The impact of global urea price cycles on African importers

When analyzing data, you should:
1. Identify the top importing countries and explain WHY they import so much
2. Note countries with both imports AND exports (likely domestic production)
3. Flag any interesting trends (price spikes, volume changes, new trade patterns)
4. Highlight data gaps and what they might mean
5. Connect findings to real-world events (e.g., gas price impacts on urea production,
   Russia-Ukraine war effects, Indian tender cycles)
6. Note which countries have domestic urea production capacity

Format your output as a clean markdown report."""

    user_prompt = f"""Analyze the following African urea trade data and produce a comprehensive report.

The data covers urea (HS code 310210) imports and exports for African countries.
Trade values are in thousands of USD. Quantities are in Kg where reported.

DATA:
{data_summary}

Please produce:
1. Executive Summary (2-3 paragraphs)
2. Top Importers Analysis (table + commentary)
3. African Urea Producers (countries with significant exports)
4. Trend Analysis (year-over-year changes, price/volume dynamics)
5. Data Quality Notes (missing data, gaps, reliability)
6. Key Insights & Recommendations for further research

Focus especially on practical insights about the African urea market."""

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 4000,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=120,
    )
    
    if response.status_code != 200:
        raise Exception(f"Claude API error {response.status_code}: {response.text}")
    
    result = response.json()
    return result["content"][0]["text"]


def save_report(report: str):
    """Save the Claude-generated report."""
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    
    report_path = DATA_DIR / f"urea_analysis_report_{timestamp}.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# African Urea Trade Analysis\n")
        f.write(f"*Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}*\n")
        f.write(f"*Data source: UN Comtrade via WITS*\n")
        f.write(f"*Analysis: Claude API (claude-sonnet-4-20250514)*\n\n")
        f.write(report)
    
    # Also save as latest
    latest = DATA_DIR / "urea_analysis_report_latest.md"
    import shutil
    shutil.copy2(report_path, latest)
    
    print(f"Report saved: {report_path}")
    return report_path


if __name__ == "__main__":
    print("Loading trade data...")
    data_summary = load_latest_data()
    
    print(f"Data loaded ({len(data_summary)} chars)")
    print("Sending to Claude for analysis...")
    
    report = analyze_with_claude(data_summary)
    
    print("\n" + "=" * 60)
    print(report)
    print("=" * 60)
    
    report_path = save_report(report)
    print(f"\nDone! Report saved to {report_path}")

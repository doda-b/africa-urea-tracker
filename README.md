# africa-urea-tracker

Automated pipeline that fetches urea (HS 310210) import and export data for all African countries using the UN Comtrade API, then analyzes results with Claude AI.

## What it does

1. **Fetches** urea trade data (imports + exports) for 54 African countries across 15 years (2010–2024)
2. **Saves** raw data as CSV and JSON in the `data/` folder  
3. **Analyzes** the data using Claude API to produce an insights report
4. **Commits** everything back to this repo automatically

Runs on-demand via GitHub Actions (you trigger it manually).

## Quick setup (5 minutes)

### 1. Create a new GitHub repo

Go to [github.com/new](https://github.com/new) and create a **public** repo (free Actions minutes for public repos).

### 2. Clone and copy files

```bash
git clone https://github.com/YOUR_USERNAME/africa-urea-tracker.git
cd africa-urea-tracker
```

Copy the following files from this template into your repo:
```
.github/workflows/fetch_urea_data.yml
scripts/fetch_urea_data.py
scripts/analyze_with_claude.py
data/.gitkeep
README.md
```

### 3. Add your API keys as GitHub Secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these secrets:

| Secret Name | Required | Where to get it |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | [console.anthropic.com](https://console.anthropic.com/) → API Keys |
| `COMTRADE_API_KEY` | No (optional) | [comtradedeveloper.un.org](https://comtradedeveloper.un.org/) — free tier gives higher rate limits |

### 4. Push and trigger

```bash
git add .
git commit -m "Initial setup"
git push
```

Then go to **Actions** tab → **Fetch African Urea Trade Data** → **Run workflow** to test it.

## How it works

### Data source

The UN Comtrade database (same source WITS uses) provides HS6-level trade data including:
- **Trade value** in USD
- **Quantity** in Kg (where reported by the country)
- **Trade flow** (Import/Export)

Product code `310210` = Urea (whether or not in aqueous solution).

### Rate limits

- UN Comtrade free API: ~1 request/second, 100 requests/hour
- Full fetch for all African countries: ~1620 requests ≈ 27 minutes
- GitHub Actions free tier: 2000 minutes/month (public repos = unlimited)

### File outputs

| File | Description |
|---|---|
| `data/africa_urea_trade_YYYYMMDD.csv` | Raw trade data, timestamped |
| `data/africa_urea_trade_latest.csv` | Always points to most recent data |
| `data/urea_analysis_report_YYYYMMDD.md` | Claude's analysis report |
| `data/urea_analysis_report_latest.md` | Always points to most recent report |

### CSV columns

| Column | Description |
|---|---|
| `country_iso3` | ISO3 country code (e.g., GHA, NGA, EGY) |
| `country_name` | Full country name |
| `year` | Year of data |
| `trade_flow` | Import or Export |
| `product_code` | 310210 |
| `trade_value_1000usd` | Trade value in thousands of USD |
| `quantity_kg` | Net weight in Kg (may be null) |
| `data_source` | Where the data came from |

## Customization

### Change the product
Edit `PRODUCT_CODE` in `scripts/fetch_urea_data.py`. Common fertilizer codes:
- `310210` — Urea
- `310230` — Ammonium nitrate
- `310240` — Ammonium nitrate + calcium carbonate mix
- `310420` — Potassium chloride (MOP)
- `310520` — Fertilizers with NPK

### Change the countries
Edit the `AFRICAN_COUNTRIES` dict in `scripts/fetch_urea_data.py`.

### Change the schedule
The workflow is set to manual-only by default. To add a schedule, edit `.github/workflows/fetch_urea_data.yml` and add a `schedule` trigger:
```yaml
on:
  schedule:
    - cron: '0 6 1 * *'         # Monthly on the 1st at 6 AM UTC
  workflow_dispatch:              # Keep manual trigger too
```

### Add more data sources
The pipeline is designed to be extended. You could add:
- IFDC Africa Fertilizer data
- FAO FAOSTAT production data
- IFA (International Fertilizer Association) statistics

## Cost

- **GitHub Actions**: Free for public repos
- **Claude API**: ~$0.01–0.05 per analysis run (one Sonnet call)
- **UN Comtrade API**: Free (optional paid tier for bulk access)

## Notes

- Some African countries don't report to UN Comtrade, so gaps are expected
- Quantity data (Kg) is less consistently reported than trade values
- WITS data typically lags by 1–2 years for some countries
- The Comtrade free API may occasionally be slow or return errors; the script handles retries

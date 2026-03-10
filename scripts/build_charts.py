"""
Build Excel charts from urea trade data.
Reads africa_urea_trade_latest.csv and produces africa_urea_analysis.xlsx

Usage:
    python scripts/build_charts.py
"""

import sys
import csv
from pathlib import Path
from datetime import datetime, timezone

try:
    import pandas as pd
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "-q"])
    import pandas as pd

try:
    from openpyxl import Workbook
    from openpyxl.chart import BarChart, LineChart, Reference
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
    from openpyxl import Workbook
    from openpyxl.chart import BarChart, LineChart, Reference
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter

DATA_DIR = Path(__file__).parent.parent / "data"

df = pd.read_csv(DATA_DIR / "africa_urea_trade_latest.csv")
df['trade_value_1000usd'] = pd.to_numeric(df['trade_value_1000usd'], errors='coerce')
df['quantity_kg'] = pd.to_numeric(df['quantity_kg'], errors='coerce')
df['year'] = df['year'].astype(int)

imports = df[df['trade_flow'] == 'Import'].copy()
exports = df[df['trade_flow'] == 'Export'].copy()
years = sorted(imports['year'].unique())
latest_year = imports['year'].max()

wb = Workbook()

HDR_FILL = PatternFill('solid', fgColor='1F4E79')
HDR_FONT = Font(bold=True, color='FFFFFF', size=10)
TITLE_FONT = Font(bold=True, size=14, color='1F4E79')
SUBTITLE_FONT = Font(italic=True, size=10, color='808080')
ALT_ROW = PatternFill('solid', fgColor='D6E4F0')

def style_header(ws, row, cols, headers):
    for i, h in enumerate(headers, 1):
        cell = ws.cell(row=row, column=i, value=h)
        cell.font = HDR_FONT
        cell.fill = HDR_FILL
        cell.alignment = Alignment(horizontal='center', wrap_text=True)

# ============================================================
# SHEET 1: Top 10 Importers
# ============================================================
ws1 = wb.active
ws1.title = "Top Importers"
ws1.sheet_properties.tabColor = "1F4E79"

latest_imp = imports[imports['year'] == latest_year].sort_values('trade_value_1000usd', ascending=False).head(10)

ws1['A1'] = f'Top 10 African Urea Importers ({latest_year})'
ws1['A1'].font = TITLE_FONT
ws1.merge_cells('A1:E1')

style_header(ws1, 3, 5, ['Country', 'Value ($K USD)', 'Quantity (MT)', 'Implied Price ($/MT)', 'Data Note'])

for idx, (_, row) in enumerate(latest_imp.iterrows(), 4):
    ws1.cell(row=idx, column=1, value=row['country_name'])
    ws1.cell(row=idx, column=2, value=round(row['trade_value_1000usd'], 1)).number_format = '#,##0.0'
    qty_mt = round(row['quantity_kg'] / 1000, 0) if pd.notna(row['quantity_kg']) and row['quantity_kg'] > 0 else None
    ws1.cell(row=idx, column=3, value=qty_mt).number_format = '#,##0'
    if qty_mt and qty_mt > 0:
        ws1.cell(row=idx, column=4, value=f'=B{idx}*1000/C{idx}').number_format = '$#,##0'
    else:
        ws1.cell(row=idx, column=4, value='N/A')
        ws1.cell(row=idx, column=5, value='Qty not reported')
        ws1.cell(row=idx, column=5).font = Font(italic=True, color='808080', size=9)
    if idx % 2 == 0:
        for c in range(1, 6):
            ws1.cell(row=idx, column=c).fill = ALT_ROW

for col, w in [(1, 22), (2, 18), (3, 16), (4, 20), (5, 18)]:
    ws1.column_dimensions[get_column_letter(col)].width = w

chart1 = BarChart()
chart1.type = "bar"
chart1.style = 10
chart1.title = f"Top 10 African Urea Importers ({latest_year})"
chart1.y_axis.title = "Country"
chart1.x_axis.title = "Trade Value ($K USD)"
chart1.height = 15
chart1.width = 25
data1 = Reference(ws1, min_col=2, min_row=3, max_row=3 + len(latest_imp))
cats1 = Reference(ws1, min_col=1, min_row=4, max_row=3 + len(latest_imp))
chart1.add_data(data1, titles_from_data=True)
chart1.set_categories(cats1)
chart1.series[0].graphicalProperties.solidFill = "1F4E79"
chart1.legend = None
ws1.add_chart(chart1, "G3")

# ============================================================
# SHEET 2: Import Trends (top 5)
# ============================================================
ws2 = wb.create_sheet("Import Trends")
ws2.sheet_properties.tabColor = "2E75B6"

total_by_country = imports.groupby('country_name')['trade_value_1000usd'].sum().sort_values(ascending=False)
top5 = total_by_country.head(5).index.tolist()

ws2['A1'] = 'Urea Import Trends — Top 5 African Importers (2010-2024)'
ws2['A1'].font = TITLE_FONT
ws2.merge_cells('A1:G1')

ws2.cell(row=3, column=1, value='Year')
ws2.cell(row=3, column=1).font = HDR_FONT
ws2.cell(row=3, column=1).fill = HDR_FILL
for j, country in enumerate(top5, 2):
    cell = ws2.cell(row=3, column=j, value=country)
    cell.font = Font(bold=True, color='FFFFFF', size=9)
    cell.fill = HDR_FILL
    cell.alignment = Alignment(horizontal='center')

for i, year in enumerate(years, 4):
    ws2.cell(row=i, column=1, value=year)
    for j, country in enumerate(top5, 2):
        val = imports[(imports['country_name'] == country) & (imports['year'] == year)]['trade_value_1000usd']
        ws2.cell(row=i, column=j, value=round(val.values[0], 1) if len(val) > 0 and pd.notna(val.values[0]) else None)
        ws2.cell(row=i, column=j).number_format = '#,##0.0'

ws2.column_dimensions['A'].width = 8
for c in range(2, len(top5) + 2):
    ws2.column_dimensions[get_column_letter(c)].width = 16

chart2 = LineChart()
chart2.style = 10
chart2.title = "Urea Import Value ($K USD) — Top 5 African Importers"
chart2.y_axis.title = "Trade Value ($K USD)"
chart2.x_axis.title = "Year"
chart2.y_axis.numFmt = '#,##0'
chart2.height = 18
chart2.width = 28

data2 = Reference(ws2, min_col=2, max_col=len(top5) + 1, min_row=3, max_row=3 + len(years))
cats2 = Reference(ws2, min_col=1, min_row=4, max_row=3 + len(years))
chart2.add_data(data2, titles_from_data=True)
chart2.set_categories(cats2)

colors = ['1F4E79', '2E75B6', 'ED7D31', '70AD47', 'A5A5A5']
for idx, s in enumerate(chart2.series):
    s.graphicalProperties.line.width = 25000
    s.graphicalProperties.line.solidFill = colors[idx % len(colors)]

ws2.add_chart(chart2, "A" + str(4 + len(years) + 2))

# ============================================================
# SHEET 3: Net Urea Trade Position
# ============================================================
ws3 = wb.create_sheet("Net Urea Trade Position")
ws3.sheet_properties.tabColor = "ED7D31"

ws3['A1'] = 'Net Urea Trade Position — African Countries (Cumulative 2010-2024)'
ws3['A1'].font = TITLE_FONT
ws3.merge_cells('A1:F1')
ws3['A2'] = 'Long = net importer (buys urea). Short = net exporter (produces/sells urea).'
ws3['A2'].font = SUBTITLE_FONT
ws3.merge_cells('A2:F2')

style_header(ws3, 4, 6, ['Country', 'Total Imports ($K)', 'Total Exports ($K)', 'Net Long/Short ($K)', 'Position', 'Producer Flag'])

imp_totals = imports.groupby('country_name')['trade_value_1000usd'].sum()
exp_totals = exports.groupby('country_name')['trade_value_1000usd'].sum()
all_countries = sorted(set(imp_totals.index) | set(exp_totals.index))

country_data = []
for c in all_countries:
    imp_val = imp_totals.get(c, 0)
    exp_val = exp_totals.get(c, 0)
    net = imp_val - exp_val
    country_data.append((c, imp_val, exp_val, net))

country_data.sort(key=lambda x: x[3])

SHORT_FILL = PatternFill('solid', fgColor='C6EFCE')
MAYBE_FILL = PatternFill('solid', fgColor='FFEB9C')

for idx, (country, imp_val, exp_val, net) in enumerate(country_data, 5):
    ws3.cell(row=idx, column=1, value=country)
    ws3.cell(row=idx, column=2, value=round(imp_val, 1)).number_format = '#,##0.0'
    ws3.cell(row=idx, column=3, value=round(exp_val, 1)).number_format = '#,##0.0'
    ws3.cell(row=idx, column=4, value=f'=B{idx}-C{idx}').number_format = '#,##0.0'

    if net < 0:
        ws3.cell(row=idx, column=5, value='SHORT')
        ws3.cell(row=idx, column=5).font = Font(bold=True, color='006100')
        ws3.cell(row=idx, column=5).fill = SHORT_FILL
    else:
        ws3.cell(row=idx, column=5, value='LONG')
        ws3.cell(row=idx, column=5).font = Font(color='9C0006')

    ws3.cell(row=idx, column=5).alignment = Alignment(horizontal='center')

    if exp_val > 100000:
        ws3.cell(row=idx, column=6, value='Major Producer')
        ws3.cell(row=idx, column=6).font = Font(bold=True, color='006100')
        ws3.cell(row=idx, column=6).fill = SHORT_FILL
    elif exp_val > 10000:
        ws3.cell(row=idx, column=6, value='Significant Exporter')
        ws3.cell(row=idx, column=6).font = Font(bold=True, color='006100')
        ws3.cell(row=idx, column=6).fill = SHORT_FILL
    elif exp_val > 1000:
        ws3.cell(row=idx, column=6, value='Possible Producer / Re-exporter')
        ws3.cell(row=idx, column=6).font = Font(color='9C6500')
        ws3.cell(row=idx, column=6).fill = MAYBE_FILL
    elif exp_val > 100:
        ws3.cell(row=idx, column=6, value='Minor Exporter')
        ws3.cell(row=idx, column=6).font = Font(color='9C6500')
        ws3.cell(row=idx, column=6).fill = MAYBE_FILL
    else:
        ws3.cell(row=idx, column=6, value='Pure Importer')

for col, w in [(1, 22), (2, 20), (3, 20), (4, 22), (5, 12), (6, 28)]:
    ws3.column_dimensions[get_column_letter(col)].width = w

chart3 = BarChart()
chart3.type = "col"
chart3.style = 10
chart3.title = "Net Urea Trade Position ($K USD, 2010-2024) — Top 20 by Volume"
chart3.y_axis.title = "Net Long/Short ($K USD)"
chart3.y_axis.numFmt = '#,##0'
chart3.height = 18
chart3.width = 30
data3 = Reference(ws3, min_col=4, min_row=4, max_row=4 + min(20, len(country_data)))
cats3 = Reference(ws3, min_col=1, min_row=5, max_row=4 + min(20, len(country_data)))
chart3.add_data(data3, titles_from_data=True)
chart3.set_categories(cats3)
chart3.series[0].graphicalProperties.solidFill = "2E75B6"
chart3.legend = None
ws3.add_chart(chart3, "H4")

# ============================================================
# SHEET 4: Heatmap by Value
# ============================================================
ws4 = wb.create_sheet("Heatmap (Value)")
ws4.sheet_properties.tabColor = "70AD47"

ws4['A1'] = 'African Urea Imports — Trade Value ($K USD)'
ws4['A1'].font = TITLE_FONT
ws4.merge_cells('A1:P1')
ws4['A2'] = 'Blank = no data reported. Darker green = higher value.'
ws4['A2'].font = SUBTITLE_FONT

imp_countries_val = imports.groupby('country_name')['trade_value_1000usd'].sum().sort_values(ascending=False).index.tolist()
max_val = imports['trade_value_1000usd'].max()

def heatmap_green(val, max_v):
    if pd.isna(val) or val is None: return 'F2F2F2'
    ratio = min(val / max_v, 1.0) if max_v > 0 else 0
    if ratio > 0.3: return '1F7A1F'
    elif ratio > 0.15: return '2D9B2D'
    elif ratio > 0.08: return '70AD47'
    elif ratio > 0.03: return '92D050'
    elif ratio > 0.01: return 'C6EFCE'
    elif ratio > 0: return 'E2EFDA'
    else: return 'F2F2F2'

ws4.cell(row=4, column=1, value='Country').font = HDR_FONT
ws4.cell(row=4, column=1).fill = HDR_FILL
for j, year in enumerate(years, 2):
    cell = ws4.cell(row=4, column=j, value=year)
    cell.font = Font(bold=True, color='FFFFFF', size=9)
    cell.fill = HDR_FILL
    cell.alignment = Alignment(horizontal='center')

for i, country in enumerate(imp_countries_val, 5):
    ws4.cell(row=i, column=1, value=country).font = Font(size=9)
    for j, year in enumerate(years, 2):
        val_s = imports[(imports['country_name'] == country) & (imports['year'] == year)]['trade_value_1000usd']
        val = val_s.values[0] if len(val_s) > 0 and pd.notna(val_s.values[0]) else None
        cell = ws4.cell(row=i, column=j, value=round(val, 0) if val else None)
        cell.number_format = '#,##0'
        cell.font = Font(size=8)
        cell.alignment = Alignment(horizontal='center')
        color = heatmap_green(val, max_val)
        cell.fill = PatternFill('solid', fgColor=color)
        if val and val / max_val > 0.08:
            cell.font = Font(size=8, color='FFFFFF', bold=True)

ws4.column_dimensions['A'].width = 22
for j in range(2, len(years) + 2):
    ws4.column_dimensions[get_column_letter(j)].width = 10

# ============================================================
# SHEET 5: Heatmap by Weight
# ============================================================
ws5 = wb.create_sheet("Heatmap (Weight)")
ws5.sheet_properties.tabColor = "2E75B6"

ws5['A1'] = 'African Urea Imports — Quantity (Metric Tonnes)'
ws5['A1'].font = TITLE_FONT
ws5.merge_cells('A1:P1')
ws5['A2'] = 'Blank = no data reported. Darker blue = higher volume. Values in MT.'
ws5['A2'].font = SUBTITLE_FONT

imp_weight = imports.copy()
imp_weight['quantity_mt'] = imp_weight['quantity_kg'] / 1000
wt_countries = imp_weight.groupby('country_name')['quantity_mt'].sum().sort_values(ascending=False).index.tolist()
max_wt = imp_weight['quantity_mt'].max()

def heatmap_blue(val, max_v):
    if pd.isna(val) or val is None or val == 0: return 'F2F2F2'
    ratio = min(val / max_v, 1.0) if max_v > 0 else 0
    if ratio > 0.3: return '1F4E79'
    elif ratio > 0.15: return '2E75B6'
    elif ratio > 0.08: return '5B9BD5'
    elif ratio > 0.03: return '9DC3E6'
    elif ratio > 0.01: return 'BDD7EE'
    elif ratio > 0: return 'DEEBF7'
    else: return 'F2F2F2'

ws5.cell(row=4, column=1, value='Country').font = HDR_FONT
ws5.cell(row=4, column=1).fill = HDR_FILL
for j, year in enumerate(years, 2):
    cell = ws5.cell(row=4, column=j, value=year)
    cell.font = Font(bold=True, color='FFFFFF', size=9)
    cell.fill = HDR_FILL
    cell.alignment = Alignment(horizontal='center')

for i, country in enumerate(wt_countries, 5):
    ws5.cell(row=i, column=1, value=country).font = Font(size=9)
    for j, year in enumerate(years, 2):
        val_s = imp_weight[(imp_weight['country_name'] == country) & (imp_weight['year'] == year)]['quantity_mt']
        val = val_s.values[0] if len(val_s) > 0 and pd.notna(val_s.values[0]) and val_s.values[0] > 0 else None
        cell = ws5.cell(row=i, column=j, value=round(val, 0) if val else None)
        cell.number_format = '#,##0'
        cell.font = Font(size=8)
        cell.alignment = Alignment(horizontal='center')
        color = heatmap_blue(val, max_wt)
        cell.fill = PatternFill('solid', fgColor=color)
        if val and val / max_wt > 0.08:
            cell.font = Font(size=8, color='FFFFFF', bold=True)

ws5.column_dimensions['A'].width = 22
for j in range(2, len(years) + 2):
    ws5.column_dimensions[get_column_letter(j)].width = 10

# ============================================================
# SHEET 6: Raw Data
# ============================================================
ws6 = wb.create_sheet("Raw Data")
ws6.sheet_properties.tabColor = "A5A5A5"
for j, col in enumerate(df.columns, 1):
    cell = ws6.cell(row=1, column=j, value=col)
    cell.font = HDR_FONT
    cell.fill = PatternFill('solid', fgColor='404040')
for i, (_, row) in enumerate(df.iterrows(), 2):
    for j, col in enumerate(df.columns, 1):
        ws6.cell(row=i, column=j, value=row[col])

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
outpath = DATA_DIR / f"africa_urea_analysis_{timestamp}.xlsx"
wb.save(outpath)

import shutil
shutil.copy2(outpath, DATA_DIR / "africa_urea_analysis_latest.xlsx")

print(f"Saved: {outpath}")
print(f"Sheets: {wb.sheetnames}")

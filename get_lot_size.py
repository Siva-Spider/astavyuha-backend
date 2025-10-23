
import pandas as pd
from datetime import datetime
import calendar

instruments = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")

def lot_size(name):


    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce').dt.date
    indices = {"Nifty 50": "NIFTY","Nifty Bank": "BANKNIFTY", "Nifty Fin Service": "FINNIFTY", "NIFTY MID SELECT": "MIDCPNIFTY"}


    if name in indices:
        name = indices[name]
        instrument_type = "OPTIDX"
    else:
        instrument_type = "OPTSTK"

    filtered = instruments[
        (instruments['instrument_type'] == instrument_type) &
        (instruments['name'] == name)
        ]

    if filtered.empty:
        print("❌ No matching option instrument found")
        return

    if not filtered.empty:
        lot_size = filtered.iloc[0]['lot_size']
        tick_size = filtered.iloc[0]['tick_size']
        print(lot_size)
        print(tick_size)
        return lot_size, tick_size
    else:
        print("❌ No matching option instrument found")
        return

def commodity_lot_size(name, symbol):
    # Load instrument data
    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce').dt.date

    # Filter only MCX FUTCOM contracts
    filtered = instruments[
        (instruments['instrument_type'] == "FUTCOM") &
        (instruments['name'] == name) &
        (instruments['exchange'] == "MCX_FO")
    ].copy()

    if filtered.empty:
        print(f"❌ No FUTCOM contracts found for {name}")
        return pd.DataFrame()

    # --- Generate current & next month expiry codes ---
    today = datetime.now()
    year = today.year
    month = today.month

    # Function to format YYMONFUT string
    def make_symbol(y, m):
        yy = str(y)[-2:]
        mon = calendar.month_abbr[m].upper()
        return f"{symbol}{yy}{mon}FUT"

    # Current month symbol
    curr_symbol = make_symbol(year, month)

    # Next month symbol (handle December → January transition)
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    next_symbol = make_symbol(next_year, next_month)

    next_2_month = next_month + 1 if next_month < 12 else 1
    next_year = year if month < 12 else year + 1
    next_2_symbol = make_symbol(next_year, next_2_month, )

    # Both target symbols
    targets = [curr_symbol.upper(), next_symbol.upper(), next_2_symbol.upper()]
    print(f"🎯 Target tradingsymbols: {targets}")

    # Filter matches
    matched = filtered[filtered['tradingsymbol'].str.upper().isin(targets)]

    if matched.empty:
        print("❌ No matching tradingsymbol found.")
        print("🧾 Available tradingsymbols for reference:")
        print(filtered['tradingsymbol'].unique()[:10])
        return pd.DataFrame()

        # Sort by expiry (latest first)
    matched.sort_values(by="expiry", ascending=False, inplace=True)
    latest_row = matched.iloc[0]

    print("✅ Matching contract found:")
    print(latest_row.to_dict())

    # Return lot size from latest expiry
    lot_size = latest_row['lot_size']
    tick_size = latest_row['tick_size']
    print(lot_size)
    print(tick_size)
    return lot_size, tick_size



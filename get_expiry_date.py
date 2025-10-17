
import datetime
import pytz
import pandas as pd

def get_expiry_date(spot_value, stock):

    now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
    # Switch to next weekly expiry after 3:15 PM on Wednesday
    if now.weekday() == 2 and now.time() >= datetime.time(15, 15):
        weekly_expiry = (now + datetime.timedelta(days=7)).date()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 0)
    pd.set_option('display.max_colwidth', None)

    # Load instruments data
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    instruments = pd.read_csv(url)

    # Parse expiry dates
    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce')

    # Get today's date
    today = datetime.datetime.today()

    # -- Monthly Expiry Logic --
    target_month = today.month + 1 if today.day > 20 else today.month
    target_year = today.year if target_month <= 12 else today.year + 1
    target_month = target_month if target_month <= 12 else 1

    monthly_filtered = instruments[
        (instruments['exchange'] == 'NSE_FO') &
        (instruments['instrument_type'] == 'FUTIDX') &
        (instruments['option_type'] == "FF") &
        (instruments['name'] == stock) &
        (instruments['expiry'].dt.month == target_month) &
        (instruments['expiry'].dt.year == target_year)
        ]

    if not monthly_filtered.empty:
        last_expiry = monthly_filtered['expiry'].max()
        print(f"📅 Last monthly expiry for NIFTY  in {last_expiry.strftime('%B %Y')}: {last_expiry.date()}")
    else:
        print("❌ No monthly expiry found for NIFTY 24800.")

    # -- Weekly Expiry Logic --
    weekly_filtered = instruments[
        (instruments['exchange'] == 'NSE_FO') &
        (instruments['instrument_type'] == 'OPTIDX') &
        (instruments['option_type'] == "CE") &
        (instruments['name'] == stock) &
        (instruments['expiry'] >= today)
        ]

    if not weekly_filtered.empty:
        next_weekly_expiry = weekly_filtered['expiry'].min()
        print(f"🗓️ Next weekly expiry for NIFTY  {next_weekly_expiry.date()}")
    else:
        print("❌ No upcoming weekly expiry found for NIFTY 24800.")

    if monthly_filtered.empty or weekly_filtered.empty:
        print(f"❌ No matching option instrument found options or hedge")
        return

    # Get monthly instrument key for the specific option type
    monthly_key = monthly_filtered[
        (monthly_filtered['expiry'] == last_expiry) &
        (monthly_filtered['option_type'] == "FF")
        ]['instrument_key'].values
    monthly_instrument_key = monthly_key[0]

    # Get weekly instrument key for the specific option type
    weekly_key = weekly_filtered[
        (weekly_filtered['expiry'] == next_weekly_expiry) &
        (weekly_filtered['option_type'] == "CE")
        ]['instrument_key'].values
    weekly_instrument_key = weekly_key[0]

    expiries = [last_expiry, next_weekly_expiry]

    return expiries

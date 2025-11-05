# trading_core.py
import gc
import datetime
import time
from zoneinfo import ZoneInfo

# Import shared helpers (keep logger_util as-is)
from logger_util import push_log, push_payload, get_log_buffer
import Next_Now_intervals as nni
import combinding_dataframes as cdf
import indicators as ind
import get_lot_size as ls

# Import broker wrappers (they are currently modules in your repo)
import Upstox as us
import Zerodha as zr
import AngelOne as ar
import Groww as gr
import Fivepaisa as fp
from kiteconnect import KiteConnect

# Maps (if you want reuse) - you can import from app if you keep them there or copy them here.
broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

reverse_stock_map = {}  # optionally set it or import from app.py if declared there

active_trades = {}  # local active trades state; you may pass externally if preferred
broker_sessions = {}

def run_trading_logic_for_all(trading_parameters, selected_brokers, logger=None):
    """
    Long-running trading loop extracted from your Flask app.
    trading_parameters: list of stock dicts (same format your frontend sends)
    selected_brokers: list of broker dicts (same as before)
    logger: optional logger - uses logger_util.push_log by default
    """
    push_log("✅ Trading loop started for all selected stocks")

    # set active flags
    for stock in trading_parameters:
        active_trades[stock['symbol_value']] = True

    # fetch instrument keys
    for stock in trading_parameters:
        if not active_trades.get(stock['symbol_value']):
            continue

        broker_key = stock.get('broker')
        broker_name = broker_map.get(broker_key)
        symbol = stock.get('symbol_value')
        name = stock.get('symbol_key')
        exchange_type = stock.get('type')

        push_log(f"🔑 Fetching instrument key for {symbol} via {broker_name}...")

        instrument_key = None
        try:
            if exchange_type == "EQUITY":
                if broker_name and broker_name.lower() == "upstox":
                    instrument_key = us.upstox_equity_instrument_key(name)
                elif broker_name and broker_name.lower() == "zerodha":
                    # find credentials for this broker in selected_brokers
                    broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                    if broker_info:
                        api_key = broker_info['credentials'].get("api_key")
                        access_token = broker_info['credentials'].get("access_token")
                        instrument_key = zr.zerodha_instruments_token(api_key, access_token, symbol)
                elif broker_name and broker_name.lower() == "angelone":
                    instrument_key = ar.angelone_get_token_by_name(symbol)
                elif broker_name and broker_name.lower() == "5paisa":
                    instrument_key = fp.fivepaisa_scripcode_fetch(symbol)

            elif exchange_type == "COMMODITY" and broker_name.lower() == "upstox":
                matched = us.upstox_commodity_instrument_key(name, symbol)
                instrument_key = matched['instrument_key'].iloc[0]

            if instrument_key:
                stock['instrument_key'] = instrument_key
                push_log(f"✅ Found instrument key {instrument_key} for {symbol}")
            else:
                push_log(f"⚠️ No instrument key found for {symbol}, skipping this stock.")
                active_trades[stock['symbol_value']] = False

        except Exception as e:
            push_log(f"❌ Error fetching instrument key for {symbol}: {e}", "error")
            active_trades[stock['symbol_value']] = False

    if not trading_parameters:
        push_log("No trading parameters provided - exiting.")
        return

    interval = trading_parameters[0].get("interval", "1minute")
    now_interval, next_interval = nni.round_to_next_interval(interval)
    push_log(f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}")

    # main trading loop
    while any(active_trades.values()):
        # remove any stocks that were disconnected
        for stock in list(trading_parameters):
            symbol = stock.get('symbol_value')
            if not active_trades.get(symbol):
                trading_parameters.remove(stock)
                push_log(f"Removed inactive stock {symbol} from trading list")

        # Get current time string in IST
        now = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
        if now >= next_interval:
            now_interval, next_interval = nni.round_to_next_interval(interval)
            push_log(f"New interval: {now_interval} Next: {next_interval}")

            # STEP 2: fetch data and run indicators per symbol
            for stock in list(trading_parameters):
                symbol = stock.get('symbol_value')
                if symbol not in active_trades:
                    continue

                broker_key = stock.get('broker')
                broker_name = broker_map.get(broker_key)
                company = stock.get('symbol_key')
                interval = stock.get('interval')
                instrument_key = stock.get('instrument_key')
                strategy = stock.get('strategy')
                exchange_type = stock.get('type')

                push_log(f"🕯 Fetching candles for {symbol}-{company} from {broker_name}")

                combined_df = None
                try:
                    if broker_name.lower() == "upstox":
                        access_token = next(
                            (b['credentials']['access_token'] for b in selected_brokers if b['name'] == broker_key),
                            None
                        )
                        if access_token:
                            hdf = us.upstox_fetch_historical_data_with_retry(access_token, instrument_key, interval)
                            idf = us.upstox_fetch_intraday_data(access_token, instrument_key, interval)
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)

                    elif broker_name.lower() == "zerodha":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            kite = KiteConnect(broker_info['credentials'].get("api_key"))
                            kite.set_access_token(broker_info['credentials'].get("access_token"))
                            if interval == "1":
                                interval = ""
                            hdf = zr.zerodha_historical_data(kite, instrument_key, interval)
                            idf = zr.zerodha_intraday_data(kite, instrument_key, interval)
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)

                    elif broker_name.lower() == "angelone":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            api_key = broker_info['credentials'].get("api_key")
                            user_id = broker_info['credentials'].get("user_id")
                            session = broker_sessions.get(broker_name)
                            if not session:
                                push_log("AngelOne session not found", "warning")
                                continue
                            obj = session["obj"]
                            auth_token = session["auth_token"]
                            interval = ar.number_to_interval(interval)
                            combined_df = ar.angelone_get_historical_data(api_key, auth_token, obj, "NSE", instrument_key, interval)

                    elif broker_name.lower() == "5paisa":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            app_key = broker_info['credentials'].get("app_key")
                            access_token = broker_info['credentials'].get("access_token")
                            combined_df = fp.fivepaisa_historical_data_fetch(access_token, instrument_key, interval, 25)

                except Exception as e:
                    push_log(f"❌ Error fetching data for {symbol}: {e}", "error")
                    continue

                if combined_df is None or combined_df.empty:
                    push_log(f"❌ No data for {symbol}, skipping.", "warning")
                    continue

                push_log(f"✅ Data ready for {symbol}")
                time.sleep(0.5)

                # indicators & signals
                indicators_df = ind.all_indicators(combined_df, strategy)
                try:
                    # call respective broker trade check functions
                    creds = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)
                    if broker_name.lower() == "upstox":
                        us.upstox_trade_conditions_check(stock.get("lots"), stock.get("target_percentage"), indicators_df.tail(1), creds, company, symbol, exchange_type, strategy)
                    elif broker_name.lower() == "zerodha":
                        zr.zerodha_trade_conditions_check(stock.get("lots"), stock.get("target_percentage"), indicators_df.tail(1), creds, symbol, strategy)
                    elif broker_name.lower() == "angelone":
                        session = broker_sessions.get(broker_name)
                        if not session:
                            push_log("AngelOne session missing during trade_check", "warning")
                            continue
                        obj = session["obj"]
                        auth_token = session["auth_token"]
                        interval_conv = ar.number_to_interval(interval)
                        ar.angelone_trade_conditions_check(obj, auth_token, stock.get("lots"), stock.get("target_percentage"), indicators_df, creds, symbol, strategy)
                    elif broker_name.lower() == "5paisa":
                        fp.fivepaisa_trade_conditions_check(stock.get("lots"), stock.get("target_percentage"), indicators_df, creds, stock, strategy)
                except Exception as e:
                    push_log(f"❌ Error running strategy for {symbol}: {e}", "error")

                # cleanup per symbol
                try:
                    del combined_df
                    del indicators_df
                except Exception:
                    pass
                gc.collect()
                time.sleep(0)

            push_log("✅ Trading cycle complete")
            push_log(f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}")
            push_log("Waiting for next interval beginning .....")
            time.sleep(1)

    push_log("All active trades ended. Exiting trading loop.")
    gc.collect()

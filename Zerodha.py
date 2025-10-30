from kiteconnect import KiteConnect
import requests
import sys
import pandas as pd
import datetime
from logger_util import logger, push_log, push_payload, get_log_buffer

def zerodha_get_equity_balance(api_key, access_token):

    kite = KiteConnect(api_key)
    kite.set_access_token(access_token)
    margins = kite.margins()  # fetch all margins
    equity_data = margins.get("equity", {})

    result = {
        "total_balance": equity_data.get("net", 0.0),
        "margin_used": equity_data.get("utilised", {}).get("debits", 0.0),
        "Available Margin": equity_data.get("available", {}).get("cash", 0.0)
    }
    return result

def zerodha_get_profile(api_key, access_token):
    url = "https://api.kite.trade/user/profile"
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", {})
            return {
                "user_id": data.get("user_id"),
                "User Name": data.get("user_name"),
                "email": data.get("email")
            }
        else:
            return {
                "error": "Failed to fetch profile",
                "status_code": response.status_code,
                "response": response.text
            }
    except Exception as e:
        return {"error": str(e)}

def zerodha_instruments_token(api_key, access_token, tradingsymbol):
    exchange ="NSE"
    kite = KiteConnect(api_key)
    kite.set_access_token(access_token)
    indices = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
               "MIDCPNIFTY": "NIFTY MID SELECT"}

    if tradingsymbol in indices:
        tradingsymbol = indices[tradingsymbol]
    # Fetch all instruments
    instruments = kite.instruments()
    df = pd.DataFrame(instruments)

    # Filter by tradingsymbol and exchange
    result = df[(df['tradingsymbol'] == tradingsymbol) & (df['exchange'] == exchange)]

    if not result.empty:
        return int(result.iloc[0]['instrument_token'])  # Return the first matching token
    else:
        return None  # Not found

def zerodha_historical_data(kite, instrument_token, interval):
    """
    Fetch historical OHLC data for given instrument.
    """
    today = datetime.date.today()
    end_date = (today - datetime.timedelta(days=1))
    start_date = today - datetime.timedelta(days=25)
    # ✅ map correctly
    if str(interval) == "1":
        interval_str = "minute"
    else:
        interval_str = f"{interval}minute"
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=start_date,
        to_date=end_date,
        interval=interval_str,
        continuous=False
    )
    df = pd.DataFrame(data)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df.set_index("timestamp", inplace=True)
    return df[['open', 'high', 'low', 'close']]

def zerodha_intraday_data(kite, instrument_token, interval):
    """
    Fetch today's intraday OHLC data.
    """
    today = datetime.date.today()
    # ✅ map correctly
    if interval == "1":
        interval_str = "minute"
    else:
        interval_str = f"{interval}minute"
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=today,
        to_date=today,
        interval=interval_str,
        continuous=False
    )
    df = pd.DataFrame(data)
    if df.empty:
        logger.info(f"⚠️ No intraday data found for {today} (maybe holiday or before market hours).")
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close'])
    else:
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df.set_index("timestamp", inplace=True)
        return df[['open', 'high', 'low', 'close']]

def zerodha_last_candle_data(kite, instrument_token, interval):
    """
    Fetch today's intraday OHLC data.
    """
    today = datetime.date.today()
    if interval == "1":
        interval = ""
    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=today,
        to_date=today,
        interval=f"{interval}minute",
        continuous=False
    )
    df = pd.DataFrame(data)
    if df.empty:
        logger.info(f"⚠️ No intraday data found for {today} (maybe holiday or before market hours).")
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close'])
    else:
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df.rename(columns={'date': 'timestamp'}, inplace=True)
        df.set_index("timestamp", inplace=True)
        return df[['open', 'high', 'low', 'close']].tail(1)

def fetch_positions(api_key, access_token):
    url = "https://api.kite.trade/portfolio/positions"

    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get("data",{})
        net_positions = data.get("net", [])
        day_positions = data.get("day",[])
        all_positions = net_positions+day_positions
        return all_positions
    else:
        logger.error(f"❌ Failed to fetch positions: {response.status_code} {response.text}")
        return None

def zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity):
    kite = KiteConnect(zerodha_api_key)
    kite.set_access_token(zerodha_access_token)
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,  # Regular order
            exchange="NSE",
            tradingsymbol=tradingsymbol,
            transaction_type="SELL",
            quantity=quantity,
            product="MIS",
            order_type="MARKET",
            price= None,
        )
        logger.info(f"✅ Order placed successfully! Order ID: {order_id}")
        return order_id
    except Exception as e:
        logger.error(f"❌ Order placement failed: {e}")
        return None

def zerodha_oco_order(kite, symbol, quantity, entry_price, stoploss_price, target_price):
    try:
        gtt = kite.place_gtt(
            trigger_type=kite.GTT_TYPE_OCO,
            tradingsymbol=symbol,
            exchange="NFO",
            trigger_values=[stoploss_price, target_price],  # stoploss & target
            last_price=entry_price,
            transaction_type="BUY",
            orders=[
                 {
                    "transaction_type": kite.TRANSACTION_TYPE_SELL,
                    "quantity": quantity,
                    "price": stoploss_price,
                    "order_type": kite.ORDER_TYPE_LIMIT,
                    "product": kite.PRODUCT_NRML
                },
                {
                    "transaction_type": kite.TRANSACTION_TYPE_SELL,
                    "quantity": quantity,
                    "price": target_price,
                    "order_type": kite.ORDER_TYPE_LIMIT,
                    "product": kite.PRODUCT_NRML
                }
            ]
        )
        logger.info(f"GTT OCO order placed: {gtt}")
        return gtt

    except Exception as e:
        logger.error(f"Error placing GTT order: {e}")

def zerodha_close_position(credentials, pos):
    zerodha_api_key = credentials['api_key']
    zerodha_access_token = credentials["access_token"]
    tradingsymbol = pos.get['tradingsymbol']
    quantity = pos.get['quantity']

    kite = KiteConnect(zerodha_api_key)
    kite.set_access_token(zerodha_access_token)
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,  # Regular order
            exchange="NSE",
            tradingsymbol=tradingsymbol,
            transaction_type="SELL",
            quantity=quantity,
            product="MIS",
            order_type="MARKET",
            price=None,
        )
        logger.info(f"✅ Order placed successfully! Order ID: {order_id}")
        return order_id
    except Exception as e:
        logger.error(f"❌ Order placement failed: {e}")
        return None

def zerodha_fetch_option_data(api_key, access_token, stock, close_price, tgt, lots,option_type):
    kite = KiteConnect(api_key)
    kite.set_access_token(access_token)

    # Fetch all instruments
    instruments = kite.instruments()
    df = pd.DataFrame(instruments)

    # Filter by stock name and exchange
    options_df = df[(df['name'] == stock) & (df['exchange'] == "NFO")]

    if options_df.empty:
        return None  # No instruments found for stock

    # Pick nearest expiry
    nearest_expiry = options_df['expiry'].min()
    options_df = options_df[options_df['expiry'] == nearest_expiry]

    # Find nearest strike below and above close_price
    nearest_strike = min(options_df['strike'], key=lambda x: abs(x - close_price))

    # Filter again for nearest strike and expiry
    strike_df = options_df[
        (options_df['strike'] == nearest_strike) &
        (options_df['expiry'] == nearest_expiry)
        ]

    # Match required option type from tradingsymbol
    strike_df = strike_df[strike_df['tradingsymbol'].str.endswith(option_type)]

    if strike_df.empty:
        return None

    # Return row as dict
    option_details = strike_df.iloc[0].to_dict()
    option_instrument_token = option_details['instrument_token']
    option_tradingsymbol = option_details['tradingsymbol']
    option_lot_size = option_details['lot_size']
    option_values = zerodha_last_candle_data(kite, option_instrument_token, "1")
    entry = float(option_values['close'].iloc[-1])
    option_tick_size = option_details['tick_size']
    target = (int(entry)*(100+int(tgt)))/100
    target_price = round(round(target/option_tick_size)*option_tick_size,2)
    quantity = lots * option_lot_size
    zerodha_oco_order(kite, option_tradingsymbol, quantity, entry, 0, target_price)


def zerodha_trade_conditions_check(lots, tgt, indicators_df, credentials, stock,strategy):
    zerodha_api_key = credentials['api_key']
    zerodha_access_token = credentials['access_token']
    #stock = indicators_df['name']

    # ✅ Check for signal
    if strategy == "ADX_MACD_WillR_Supertrend":
        latest_adx = indicators_df["ADX"].iloc[-1]
        latest_adxema = indicators_df['ADX_EMA21'].iloc[-1]
        latest_willr = indicators_df['WillR_14'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        latest_macd = indicators_df['MACD'].iloc[-1]
        latest_macd_signal = indicators_df['MACD_signal'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])

        positions1 = fetch_positions(zerodha_api_key, zerodha_access_token)

        if positions1:
            for pos in positions1:
                quantity = pos.get("quantity", 0)
                if quantity > 0:
                    tradingsymbol = pos.get("tradingsymbol")
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and ((latest_willr < -70 and latest_supertrend > close_price) or (
                            latest_willr < -70 and latest_macd < latest_macd_signal) or (
                                                        latest_supertrend > close_price and latest_macd < latest_macd_signal)):
                        zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                        logger.info(f"The existing position is type CE with symbol {tradingsymbol}. CE exit condition met, closing existing CE position.")
                    elif option_type == "PE" and ((latest_willr > -30 and latest_supertrend < close_price) or (
                            latest_willr > -30 and latest_macd > latest_macd_signal) or (
                                                          latest_supertrend < close_price and latest_macd < latest_macd_signal)):
                        zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                        logger.info(f"The existing position is type PE with symbol {tradingsymbol}. PE exit condition met, closing existing PE position.")

        positions = fetch_positions(zerodha_api_key, zerodha_access_token)
        if latest_adx > latest_adxema and latest_willr > -30 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
            logger.info("🔼 BUY SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos.get("quantity", 0)
                    if quantity > 0:
                        count +=1
                        tradingsymbol = pos.get("tradingsymbol")
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            logger.info(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")

                if count == 0:
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "CE")
                    logger.info(f"There are no live positions and BUY signal generated. Placing a new CE order")
            else:
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price,  tgt, lots,"CE")
                logger.info(f"There are no positions and BUY signal generated. Placing a new CE order")

        elif latest_adx > latest_adxema and latest_willr < -70 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
            logger.info("🔽 SELL SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos.get("quantity", 0)
                    if quantity > 0:
                        count +=1
                        tradingsymbol = pos.get("tradingsymbol")
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            logger.info(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                if count == 0:
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price,  tgt, lots,"PE")
                    logger.info(f"There are no live positions and SELL signal generated. Placing a new PE order")
            else:
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "PE")
                logger.info(f"There are no positions and SELL signal generated. Placing a new PE order")
        else:
            logger.info("⏸️ NO TRADE SIGNAL GENERATED")
            sys.stdout.flush()

    elif strategy == "Ema10_Ema20_Supertrend":
        latest_Ema10 = indicators_df["ema10"].iloc[-1]
        latest_Ema20 = indicators_df['ema20'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])

        positions1 = fetch_positions(zerodha_api_key, zerodha_access_token)
        if positions1:
            for pos in positions1:
                quantity = pos.get("quantity", 0)
                if quantity > 0:
                    tradingsymbol = pos.get("tradingsymbol")
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and (latest_Ema10 < latest_Ema20 or latest_supertrend > close_price):
                        zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                        logger.info(
                            f"The existing position is type CE with symbol {tradingsymbol}. CE exit condition met, closing existing CE position ")
                    elif option_type == "PE" and (latest_Ema10 > latest_Ema20 or latest_supertrend < close_price):
                        zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                        logger.info(
                            f"The existing position is type PE with symbol {tradingsymbol}. PE exit condition met, closing existing PE position ")

        positions = fetch_positions(zerodha_api_key, zerodha_access_token)
        if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price:
            logger.info("🔼 BUY SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos.get("quantity", 0)
                    if quantity > 0:
                        count += 1
                        tradingsymbol = pos.get("tradingsymbol")
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            logger.info(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")

                if count == 0:
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt,lots, "CE")
                    logger.info(f"There are no live positions and BUY signal generated. Placing a new CE order")
            else:
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "CE")
                logger.info(f"There are no positions and BUY signal generated. Placing a new CE order")

        elif latest_Ema10 < latest_Ema20 and latest_supertrend > close_price:
            logger.info("🔽 SELL SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos.get("quantity", 0)
                    if quantity > 0:
                        count +=1
                        tradingsymbol = pos.get("tradingsymbol")
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            logger.info(
                                f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                if count == 0:
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots,"PE")
                    logger.info(f"There are no live positions and SELL signal generated. Placing a new PE order")
            else:
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "PE")
                logger.info(f"There are no positions and SELL signal generated. Placing a new PE order")
        else:
            logger.info("⏸️ NO TRADE SIGNAL GENERATED")
            sys.stdout.flush()
    elif strategy == "Ema10_Ema20_MACD_Supertrend":
        latest_Ema10 = indicators_df["ema10"].iloc[-1]
        latest_Ema20 = indicators_df['ema20'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        latest_macd = indicators_df['MACD'].iloc[-1]
        latest_macd_signal = indicators_df['MACD'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])

        positions = fetch_positions(zerodha_api_key, zerodha_access_token)
        if positions:
            count = 0
            for pos in positions:
                quantity = pos['quantity']
                if quantity > 0:
                    count += 1
                    instrument_token = pos['instrument_token']
                    tradingsymbol = pos['tradingsymbol']
                    option_type = tradingsymbol[-2:]
                    if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
                        if option_type == "CE":
                            logger.info("BUY SIGNAL GENERATED. You have existing CALL position. No new order placed")
                        elif option_type == "PE":
                            logger.info("BUY SIGNAL GENERATED.  Closing existing PUT Position and place new CALL order")
                            zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                            zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "CE")
                    elif latest_Ema10 < latest_Ema20 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
                        if option_type == "PE":
                            logger.info("SELL SIGNAL GENERATED. You have existing PUT position. No new order placed")
                        elif option_type == "CE":
                            logger.info("SELL SIGNAL GENERATED.  Closing existing CALL Position and place new CALL order")
                            zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                            zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "PE")
                    elif option_type == "CE":
                        if latest_Ema10 < latest_Ema20 or latest_supertrend > close_price or latest_macd < latest_macd_signal:
                            logger.info("NO Trade Signal Generated .CALL position exit condition met. Closing existing CALL position")
                            zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
                    elif option_type == "PE":
                        if latest_Ema10 > latest_Ema20 or latest_supertrend < close_price or latest_macd > latest_macd_signal:
                            logger.info("NO Trade Signal Generated. PUT position exit condition met. Closing existing PUT position")
                            zerodha_place_order(zerodha_api_key, zerodha_access_token, tradingsymbol, quantity)
            if count == 0:
                if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
                    logger.info("BUY SIGNAL GENERATED. No live position exist. Placing new CALL order")
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "CE")
                elif latest_Ema10 < latest_Ema20 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
                    logger.info("SELL SIGNAL GENERATED. No live position exist. Placing new PUT order")
                    zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "PE")
                else:
                    logger.info("NO Trade Signal Generated")
        else:
            if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
                logger.info("BUY SIGNAL GENERATED. Placing new CALL order")
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "CE")
            elif latest_Ema10 < latest_Ema20 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
                logger.info("SELL SIGNAL GENERATED. Placing new PUT order")
                zerodha_fetch_option_data(zerodha_api_key, zerodha_access_token, stock, close_price, tgt, lots, "PE")
            else:
                logger.info("NO Trade Signal Generated")

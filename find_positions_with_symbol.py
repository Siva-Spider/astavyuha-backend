import requests
from kiteconnect import KiteConnect
import Upstox as us
import AngelOne as ar

def find_positions_for_symbol(broker, symbol, credentials):
    """
    Fetch positions for the given broker and return only those matching the symbol.
    """
    positions = []

    try:
        # --- Upstox ---
        if broker.lower() == "upstox":
            access_token = credentials.get("access_token")
            positions_data = us.upstox_positions(access_token)  # should return list/dict of positions
            positions = positions_data.get("data", {}).get("net", [])

        # --- Zerodha ---
        elif broker.lower() == "zerodha":
            api_key = credentials.get("api_key")
            access_token = credentials.get("access_token")
            kite = KiteConnect(api_key)
            kite.set_access_token(access_token)
            positions_data = kite.positions()
            positions = positions_data.get("net", [])

        # --- AngelOne ---
        elif broker.lower() == "angelone":
            api_key = credentials.get("api_key")
            user_id = credentials.get("user_id")
            pin = credentials.get("pin")
            totp_secret = credentials.get("totp_secret")
            positions_data = ar.angel_positions(api_key, user_id, pin, totp_secret)
            positions = positions_data.get("data", [])

        # --- Groww / 5paisa (dummy example here) ---
        elif broker.lower() in ["groww", "5paisa"]:
            # Assume you have functions like gr.groww_positions(), fp.fivepaisa_positions()
            positions = []  # placeholder

        # --- Filter matching positions ---
        matching = []
        for pos in positions:
            trading_symbol = pos.get("tradingsymbol", "")
            if trading_symbol.startswith(symbol):  # match beginning
                matching.append(pos)

        return matching

    except Exception as e:
        print(f"❌ Error fetching positions for {broker}, {symbol}: {e}")
        return []

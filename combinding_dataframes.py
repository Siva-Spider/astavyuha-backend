from collections import deque
import pandas as pd

def combinding_dataframes(hdf, idf):
    candle_buffer = deque(maxlen=500)
    if hdf is not None and not hdf.empty:
        for dt, row in hdf.iterrows():
            candle1 = {
                'datetime': dt,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close']
            }
            candle_buffer.append(candle1)
    if idf is not None and not idf.empty:
        for dt, row in idf.iterrows():
            candle2 = {
                'datetime': dt,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close']
            }
            candle_buffer.append(candle2)

    # Convert candle_buffer to DataFrame
    df = pd.DataFrame(candle_buffer)
    return df
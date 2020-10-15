import logging
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import func

from bdata_db import *
from bdata_model import ExchangeMarket, Trade


def agg_ohlcv(x):
    arr = x['price'].values
    names = {
        'low': min(arr) if len(arr) > 0 else np.nan,
        'high': max(arr) if len(arr) > 0 else np.nan,
        'open': arr[0] if len(arr) > 0 else np.nan,
        'close': arr[-1] if len(arr) > 0 else np.nan,
        'volume': sum(x['amount'].values) if len(x['amount'].values) > 0 else 0,
    }
    return pd.Series(names)


def mydf():
    return pd.read_sql_query(
        "select dts, side, price, amount, total from vemt where e_symbol='binance' and bt='EDO' and qt='BTC'",
        con=engine)


def ohlcv(df: pd.DataFrame, slice):
    return df.resample(slice).apply(agg_ohlcv)


def agg():
    session = Session()
    for em in session.query(ExchangeMarket).all():
        max_ts = session.query(func.max(Trade.ts)). \
            filter(Trade.exchange_market_id == em.exchange_market_id).first()
        max_ts = max_ts and max_ts[0]
        if max_ts:
            logging.info('{} {}'.format(em, datetime.utcfromtimestamp(max_ts / 1000)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    agg()

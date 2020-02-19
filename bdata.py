import concurrent.futures
import datetime
import json
import logging
import sys
import time
from argparse import ArgumentParser
from copy import deepcopy
from decimal import Decimal
from typing import Tuple

import ccxt
from sqlalchemy import and_, func

from bdata_db import Session
from bdata_model import Token, ExchangeMarket, Exchange, BookSnap, BookSnapBid, BookSnapAsk, Trade

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

args = []
base_list = []
quote_list = []

BOOK_LIMIT = 50
TRADES_LIMIT = 100000

TOP_EXCHANGES = ['hitbtc', 'bitfinex', 'binance', 'huobipro', 'kraken', 'zb', 'coinbasepro', 'okex', 'bittrex',
                 'bitstamp',
                 'poloniex', 'bitbay']

DISABLED_EXCHANGES = ['bitfinex2', 'anxpro', 'bcex', 'vaultoro', 'coss', 'coolcoin', 'btctradeim', 'cobinhood',
                      'coingi', 'flowbtc', 'stronghold', 'xbtce', 'stex', 'zb', 'rightbtc', 'hitbtc2',
                      'braziliex', 'bitforex',
                      'adara',  # TypeError '>' not supported between instances of 'NoneType' and 'NoneType'
                      'tidex',  # ExchangeNotAvailable tidex {"success":0,"error":"not available"}
                      'liquid',  # DDoSProtection liquid
                      ]


def decimalize(book):
    return {'bids': [(Decimal(str(p)), Decimal(str(a))) for p, a in book['bids']],
            'asks': [(Decimal(str(p)), Decimal(str(a))) for p, a in book['asks']]
            }


def book_limit(e):
    if e.id == 'kucoin':
        return 100
    elif e.id == 'fcoin' or e.id == 'fcoinjp':
        return 150
    else:
        return BOOK_LIMIT


def ensure_exchange_market(session, exchange, base, quote) -> Tuple[Exchange, ExchangeMarket, Token, Token]:
    e = session.query(Exchange).filter(Exchange.symbol == exchange.id).first()
    if not e:
        e = Exchange(symbol=exchange.id)
        session.add(e)
        session.commit()

    tokens = session.query(Token).all()
    bt = next((t for t in tokens if t.symbol == base), None)
    if not bt:
        bt = Token(symbol=base)
        session.add(bt)
        session.commit()

    qt = next((t for t in tokens if t.symbol == quote), None)
    if not qt:
        qt = Token(symbol=quote)
        session.add(qt)
        session.commit()

    em = session.query(ExchangeMarket).filter(
        and_(ExchangeMarket.exchange_id == e.exchange_id, ExchangeMarket.base_token_id == bt.token_id,
             ExchangeMarket.quote_token_id == qt.token_id, )).first()
    if not em:
        em = ExchangeMarket(exchange_id=e.exchange_id, base_token_id=bt.token_id,
                            quote_token_id=qt.token_id)
        session.add(em)
        session.commit()

    return (e, em, bt, qt)


def snap_market_book(ts: datetime.datetime, exchange: ccxt.Exchange, base: str, quote: str):
    logging.info('{}::{} snap book'.format(exchange.id, base + '/' + quote))
    session = Session()
    try:
        (e, em, bt, qt) = ensure_exchange_market(session, exchange, base, quote)

        bs = session.query(BookSnap).filter(
            and_(BookSnap.exchange_market_id == em.exchange_market_id, BookSnap.ts == ts)).first()
        if bs:
            return

        bs = BookSnap(exchange_market_id=em.exchange_market_id, ts=ts)
        session.add(bs)
        b = exchange.fetch_order_book(bt.symbol + '/' + qt.symbol, limit=book_limit(exchange))
        b = decimalize(b)
        for (p, a) in b['bids']:
            bs.bids.append(BookSnapBid(price=p, amount=a))
        for (p, a) in b['asks']:
            bs.asks.append(BookSnapAsk(price=p, amount=a))
        session.commit()

    finally:
        session.close()


def snap_trades(ts: datetime.datetime, exchange: ccxt.Exchange, base: str, quote: str):
    logging.info('{}::{} snap trades'.format(exchange.id, base + '/' + quote))
    session = Session()
    try:
        try:
            (e, em, bt, qt) = ensure_exchange_market(session, exchange, base, quote)

            max_trade_id = session.query(func.max(Trade.trade_id)). \
                filter(Trade.exchange_market_id == em.exchange_market_id).first()
            max_trade_id = max_trade_id and max_trade_id[0]

            if max_trade_id:
                last = session.query(Trade).filter(Trade.trade_id == max_trade_id).one()
                since = last.ts
            else:
                last = None
                since = exchange.milliseconds() - exchange.milliseconds() % 86400000 - datetime.datetime.now().timetuple().tm_yday * 86400000
            at = []
            market = base + '/' + quote
            if last and last.eid:
                last_id = last.eid
            else:
                last_id = ''
            while since < exchange.milliseconds() and len(at) < TRADES_LIMIT:
                trades = exchange.fetch_trades(market, since)
                if len(trades) > 0:
                    if last_id == trades[-1]['id']:
                        break
                    since = trades[-1]['timestamp']
                    last_id = trades[-1]['id']
                    at += trades
                else:
                    break

            # duplicates
            found = True
            while found:
                found = False
                for i in range(1, len(at)):
                    if at[i - 1]['id'] == at[i]['id']:
                        del at[i - 1]
                        found = True
                        break
            if last and last.eid:
                i = 0
                while (i < len(at)) and (at[i]['id'] != last.eid):
                    i += 1
                if i < len(at):
                    at = at[i + 1:]

            if len(at) > 0:
                logging.info('{}::{} trades {}'.format(exchange.id, market, len(at)))
                for e in at:
                    session.add(
                        Trade(exchange_market_id=em.exchange_market_id,
                              ts=e['timestamp'],
                              side='B' if e['side'] == 'buy' else 'S',
                              price=Decimal(str(e['price'])) if e['price'] else 0,
                              amount=Decimal(str(e['amount'])) if e['amount'] else 0,
                              eid=e['id']))

            session.commit()

        except:
            session.rollback()
            raise
    finally:
        session.close()


def snap(exchange, market, ts):
    try:
        snap_market_book(ts, exchange, market['base'], market['quote'])
        snap_trades(ts, exchange, market['base'], market['quote'])
    except Exception as e:
        logging.error(
            '{}::{} {} {}'.format(exchange.id, market['symbol'], sys.exc_info()[0].__name__, sys.exc_info()[1]))


def market_filter(market) -> bool:
    global base_list, quote_list
    return market['base'] and market['quote'] and \
           (market['base'] in base_list or base_list[0] == '*') and \
           (market['quote'] in quote_list or quote_list[0] == '*') and \
           '/' in market['symbol']


def exchange_filter(exchange) -> bool:
    return exchange.has['publicAPI'] and exchange.has['fetchOrderBook'] and exchange.has['fetchTrades']


def last_ts():
    now = datetime.datetime.now()
    ts = datetime.datetime(now.year, now.month, now.day)
    s = now.hour * 3600 + now.minute * 60 + now.second
    s = s - (s % args.interval)
    return ts + datetime.timedelta(seconds=s)


if __name__ == '__main__':
    cfg = json.loads(open('config.json').read())

    parser = ArgumentParser()
    parser.add_argument('--exchange', required=True)
    parser.add_argument('--interval', default=300, type=int)
    parser.add_argument('--base', required=True)
    parser.add_argument('--quote', required=True)
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()
    if args.exchange == '*':
        exchange_list = ccxt.exchanges
        for e in DISABLED_EXCHANGES:
            if e in exchange_list:
                exchange_list.remove(e)
    else:
        exchange_list = args.exchange.split(',')
    base_list = args.base.split(',')
    quote_list = args.quote.split(',')

    markets = []
    exchanges = []
    for name in exchange_list:
        exchange = getattr(ccxt, name)()
        if exchange_filter(exchange):
            try:
                if 'proxies' in cfg:
                    exchange.proxies = cfg['proxies']
                m = exchange.load_markets()
                exchanges.append(exchange)
                exchange.enableRateLimit = False
                exchange.timeout = 60000
                markets.append(m)
            except Exception as e:
                logging.error('{} error {}'.format(exchange.id, str(e)))
                pass

    if len(exchanges) == 0:
        logging.error('exchanges list is empty')
    else:
        ts = last_ts()
        while True:
            if datetime.datetime.now() > ts:
                current_ts = ts
                logging.info('start snap ts={}'.format(current_ts))
                threads = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                    tasks = []
                    for i in range(0, len(exchanges)):
                        for m in markets[i].keys():
                            if market_filter(markets[i][m]):
                                tasks.append(
                                    pool.submit(snap, deepcopy(exchanges[i]), deepcopy(markets[i][m]), current_ts))
                    ts = last_ts() + datetime.timedelta(seconds=args.interval)
                    concurrent.futures.wait(tasks, return_when=concurrent.futures.ALL_COMPLETED)
                    tasks = []
                    logging.info('end snap ts={}'.format(datetime.datetime.now()))
            else:
                time.sleep(0.2)

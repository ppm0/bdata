import datetime
import json
import logging
import sys
import time
from argparse import ArgumentParser, Namespace
from copy import deepcopy
from decimal import Decimal
from enum import Enum
from typing import Tuple, Optional

import ccxt
from sqlalchemy import and_, func

from bdata_db import Session
from bdata_model import Token, ExchangeMarket, Exchange, BookSnap, BookSnapBid, BookSnapAsk, Trade

KUCOIN = 'kucoin'
BINANCE = 'binance'
COINBASEPRO = 'coinbasepro'

args: Optional[Namespace] = None
base_list: list = []
quote_list: list = []

DEFAULT_BOOK_LIMIT = None
TRADES_LIMIT = 100000


class SnapTarget(Enum):
    ALL = "all"
    BOOK = "book"
    TRADE = "trade"


TOP_EXCHANGES = ['hitbtc', 'bitfinex', 'binance', 'huobipro', 'kraken', 'zb', 'coinbasepro', 'okex', 'bittrex',
                 'bitstamp',
                 'poloniex', 'bitbay']

DISABLED_EXCHANGES = ['bitfinex2', 'anxpro', 'bcex', 'vaultoro', 'coss', 'coolcoin', 'btctradeim', 'cobinhood',
                      'coingi', 'flowbtc', 'stronghold', 'xbtce', 'stex', 'zb', 'rightbtc', 'hitbtc2',
                      'braziliex', 'bitforex',
                      'adara',  # TypeError '>' not supported between instances of 'NoneType' and 'NoneType'
                      'tidex',  # ExchangeNotAvailable tidex {"success":0,"error":"not available"}
                      'liquid',  # DDoSProtection liquid
                      'okex3',  # use only okex
                      'fcoin', 'fcoinjp',  # 20200220 exit scam
                      'bitbay',  # fucked up
                      '_1btcxe',  # error - ddos protection
                      # ExchangeError bitget {"status":"error","ts":1612883459971,"err_code":"invalid-parameter","err_msg":"Failed to convert property value of type \u0027java.lang.String\u0027 to required type \u0027java.lang.Integer\u0027 for property \u0027size\u0027; nested exception is java.lang.NumberFormatException: For input string: \"1612828800000\""}
                      'bitget',
                      # DDoSProtection bytetrade 503 Service Unavailable {"code":1,"msg":"Please use websocket to get this data."}
                      'bytetrade',
                      # BTC/USDT BadSymbol eterbase {"message":"Invalid market !","_links":{"self":{"href":"/markets/3/trades","templated":false}}}
                      'eterbase',
                      ]


def decimalize(book):
    return {'bids': [(Decimal(str(l[0])), Decimal(str(l[1]))) for l in book['bids']],
            'asks': [(Decimal(str(l[0])), Decimal(str(l[1]))) for l in book['asks']]
            }


def book_limit(exchange: ccxt.Exchange):
    if exchange.id == KUCOIN:
        return 100
    elif exchange.id == BINANCE:
        return 5000
    else:
        return DEFAULT_BOOK_LIMIT


def book_params(exchange: ccxt.Exchange):
    if exchange.id == COINBASEPRO:
        return {'level': 3}
    else:
        return {}


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


def snap_book(mts: datetime.datetime, exchange: ccxt.Exchange, base: str, quote: str):
    logging.info('{}::{} snap book'.format(exchange.id, base + '/' + quote))
    session = Session()
    try:
        (e, em, bt, qt) = ensure_exchange_market(session, exchange, base, quote)

        bs = session.query(BookSnap).filter(
            and_(BookSnap.exchange_market_id == em.exchange_market_id, BookSnap.mts == mts)).first()
        if bs:
            return

        bs = BookSnap(exchange_market_id=em.exchange_market_id, mts=mts)
        session.add(bs)
        bo = exchange.fetch_order_book(bt.symbol + '/' + qt.symbol, limit=book_limit(exchange),
                                       params=book_params(exchange))
        b = decimalize(bo)
        for (p, a) in b['bids']:
            bs.bids.append(BookSnapBid(price=p, amount=a))
        for (p, a) in b['asks']:
            bs.asks.append(BookSnapAsk(price=p, amount=a))
        session.commit()

        bs.stat = False
        session.add(bs)
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
            else:
                last = None

            if last and last.eid:
                last_eid = last.eid
            else:
                last_eid = ''

            if em.trade_ts:
                since = em.trade_ts
            else:
                if max_trade_id:
                    since = session.query(Trade).filter(Trade.trade_id == max_trade_id).one().ts
                else:
                    since = exchange.milliseconds() - exchange.milliseconds() % 86400000

            trades_all = []
            market = base + '/' + quote

            trades_prior = None
            if exchange.id.startswith(BINANCE):
                while since < exchange.milliseconds() and len(trades_all) < TRADES_LIMIT:
                    trades_tmp = exchange.fetch_trades(symbol=market, since=since)
                    if json.dumps(trades_prior, sort_keys=True) == json.dumps(trades_tmp, sort_keys=True):
                        since += 55 * 60 * 1000
                    else:
                        if len(trades_tmp) > 0:
                            trades_all += trades_tmp
                            since = trades_tmp[-1]['timestamp']
                        else:
                            since += 55 * 60 * 1000
                    trades_prior = deepcopy(trades_tmp)
            else:
                while since < exchange.milliseconds() and len(trades_all) < TRADES_LIMIT:
                    trades_tmp = exchange.fetch_trades(market, since)
                    if len(trades_tmp) > 0:
                        if last_eid == trades_tmp[-1]['id']:
                            break
                        since = trades_tmp[-1]['timestamp']
                        last_eid = trades_tmp[-1]['id']
                        trades_all += trades_tmp
                    else:
                        break

            # duplicates
            found = True
            while found:
                found = False
                for i in range(1, len(trades_all)):
                    if trades_all[i - 1]['id'] == trades_all[i]['id']:
                        del trades_all[i - 1]
                        found = True
                        break
            if last and last.eid:
                i2 = 0
                while (i2 < len(trades_all)) and (trades_all[i2]['id'] != last.eid):
                    i2 += 1
                if i2 < len(trades_all):
                    trades_all = trades_all[i2 + 1:]

            logging.info('{}::{} len={}'.format(exchange.id, market, len(trades_all)))
            if len(trades_all) > 0:
                for e in trades_all:
                    session.add(
                        Trade(exchange_market_id=em.exchange_market_id,
                              ts=e['timestamp'],
                              side='B' if e['side'] == 'buy' else 'S',
                              price=Decimal(str(e['price'])) if e['price'] else 0,
                              amount=Decimal(str(e['amount'])) if e['amount'] else 0,
                              eid=e['id']))
                em.trade_ts = trades_all[-1]['timestamp']
                session.add(em)

            session.commit()

        except:
            session.rollback()
            raise
    finally:
        session.close()


def snap(exchange: ccxt.Exchange, market: dict, ts: datetime, snap_target: SnapTarget):
    try:
        if snap_target in [SnapTarget.ALL, SnapTarget.BOOK]:
            snap_book(ts, exchange, market['base'], market['quote'])
        if snap_target in [SnapTarget.ALL, SnapTarget.TRADE]:
            snap_trades(ts, exchange, market['base'], market['quote'])
    except Exception as e:
        logging.error(
            '{}::{} {} {}'.format(exchange.id, market['symbol'], sys.exc_info()[0].__name__, sys.exc_info()[1]))


def market_filter(market) -> bool:
    global base_list, quote_list
    return market['base'] and market['quote'] and \
           (market['base'] in base_list or base_list[0] == '*') and \
           (market['quote'] in quote_list or quote_list[0] == '*') and '/' in market['symbol']


def exchange_filter(exchange) -> bool:
    return exchange.has['publicAPI'] and exchange.has['fetchOrderBook'] and exchange.has['fetchTrades']


def last_ts():
    now = datetime.datetime.now()
    ts = datetime.datetime(now.year, now.month, now.day)
    s = now.hour * 3600 + now.minute * 60 + now.second
    s = s - (s % args.interval)
    return ts + datetime.timedelta(seconds=s)


def create_exchange(cls):
    exchange = cls()
    if exchange.id.startswith('bitfinex'):
        exchange.rateLimit = 5000
    return exchange


def bdata():
    cfg = json.loads(open('config.json').read())

    parser = ArgumentParser()
    parser.add_argument('--exchange', required=True)
    parser.add_argument('--interval', default=300, type=int)
    parser.add_argument('--snap_target', type=SnapTarget, choices=list(SnapTarget), default=SnapTarget.TRADE)
    parser.add_argument('--base', required=True)
    parser.add_argument('--quote', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--workers', default=16, type=int)
    global args
    args = parser.parse_args()
    if args.exchange == '*':
        exchange_list = ccxt.exchanges
        for e in DISABLED_EXCHANGES:
            if e in exchange_list:
                exchange_list.remove(e)
    else:
        exchange_list = args.exchange.split(',')

    global base_list, quote_list

    base_list = args.base.split(',')
    quote_list = args.quote.split(',')

    markets = []
    exchanges = []
    for name in exchange_list:
        exchange = create_exchange(getattr(ccxt, name))
        if exchange_filter(exchange):
            try:
                if 'proxies' in cfg:
                    exchange.proxies = cfg['proxies']
                    exchange.enableRateLimit = False
                else:
                    exchange.enableRateLimit = True
                m = exchange.load_markets()
                exchanges.append(exchange)
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
                # with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                #     tasks = []
                #     for i in range(0, len(exchanges)):
                #         for m in markets[i].keys():
                #             if market_filter(markets[i][m]):
                #                 tasks.append(
                #                     pool.submit(snap, deepcopy(exchanges[i]), deepcopy(markets[i][m]), current_ts,
                #                                 args.snap_target))
                #     ts = last_ts() + datetime.timedelta(seconds=args.interval)
                #     concurrent.futures.wait(tasks, return_when=concurrent.futures.ALL_COMPLETED)
                for i in range(0, len(exchanges)):
                    for m in markets[i].keys():
                        if market_filter(markets[i][m]):
                            snap(exchanges[i], markets[i][m], current_ts, args.snap_target)
                ts = last_ts() + datetime.timedelta(seconds=args.interval)
                logging.info('end snap ts={}'.format(current_ts))
            else:
                time.sleep(0.1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    bdata()

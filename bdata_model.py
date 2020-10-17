import datetime

from sqlalchemy import Column, String, BigInteger, DateTime, Integer, ForeignKey, UniqueConstraint, Index, Numeric, \
    Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Exchange(Base):
    __tablename__ = 'exchange'
    exchange_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), unique=True, nullable=False)
    exchange_markets = relationship('ExchangeMarket', backref='exchange', lazy="joined")


class Token(Base):
    __tablename__ = 'token'
    token_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50), unique=True, nullable=False)


class ExchangeMarket(Base):
    __tablename__ = 'exchange_market'
    exchange_market_id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchange.exchange_id'), nullable=False)
    base_token_id = Column(Integer, ForeignKey('token.token_id'))
    quote_token_id = Column(Integer, ForeignKey('token.token_id'))
    trade_ts = Column(BigInteger)
    base_token = relationship("Token", foreign_keys=[base_token_id], lazy="joined")
    quote_token = relationship("Token", foreign_keys=[quote_token_id], lazy="joined")
    __table_args__ = (UniqueConstraint('exchange_id', 'base_token_id', 'quote_token_id'),
                      Index('ixu1', 'exchange_id', 'base_token_id', 'quote_token_id', unique=True))

    def __repr__(self):
        return '{}({}/{})'.format(self.exchange.symbol, self.base_token.symbol, self.quote_token.symbol)


class BookSnap(Base):
    __tablename__ = 'book_snap'
    book_snap_id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    mts = Column(DateTime, nullable=False)
    exchange_market_id = Column(Integer, ForeignKey('exchange_market.exchange_market_id'), nullable=False)
    stat = Column(Boolean)
    asks = relationship('BookSnapAsk', backref='book_snap')
    bids = relationship('BookSnapBid', backref='book_snap')
    __table_args__ = (Index('ix_book_snap_1', 'ts', 'exchange_market_id'),)


class BookSnapStat(Base):
    __tablename__ = 'book_snap_stat'
    book_snap_stat_id = Column(BigInteger, primary_key=True, autoincrement=True)
    book_snap_id = Column(BigInteger, ForeignKey('book_snap.book_snap_id', ondelete='cascade'), nullable=False)
    # code - 0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 2, 3, 5, 8, 10, 20, 30, 50, 80, 100
    code = Column(String(10), nullable=False)
    data = Column(JSONB)
    __table_args__ = (Index('ix_book_snap_stat_book_snap_id_code', 'book_snap_id', 'code', unique=True),)


class BookSnapAsk(Base):
    __tablename__ = 'book_snap_ask'
    book_snap_ask_id = Column(BigInteger, primary_key=True, autoincrement=True)
    book_snap_id = Column(BigInteger, ForeignKey('book_snap.book_snap_id', ondelete='cascade'), nullable=False)
    price = Column(Numeric)
    amount = Column(Numeric)
    __table_args__ = (Index('ix_book_snap_ask_1', 'book_snap_id'),)


class BookSnapBid(Base):
    __tablename__ = 'book_snap_bid'
    book_snap_bid_id = Column(BigInteger, primary_key=True, autoincrement=True)
    book_snap_id = Column(BigInteger, ForeignKey('book_snap.book_snap_id', ondelete='cascade'), nullable=False)
    price = Column(Numeric)
    amount = Column(Numeric)
    __table_args__ = (Index('ix_book_snap_bid_1', 'book_snap_id'),)


class Trade(Base):
    __tablename__ = 'trade'
    trade_id = Column(BigInteger, primary_key=True, autoincrement=True)
    exchange_market_id = Column(Integer, ForeignKey('exchange_market.exchange_market_id'), nullable=False)
    ts = Column(BigInteger, nullable=False)
    side = Column(String(1), nullable=False)
    price = Column(Numeric, nullable=False)
    amount = Column(Numeric, nullable=False)
    fee = Column(Numeric, nullable=True)
    eid = Column(String(32), nullable=True)
    __table_args__ = (Index('ix_trade_ts_exchange_market_id', 'ts', 'exchange_market_id'),
                      Index('ix_trade_exchange_market_id_trade_id', 'exchange_market_id', 'trade_id'),
                      Index('ix_trade_exchange_market_id_ts', 'exchange_market_id', 'ts')
                      )

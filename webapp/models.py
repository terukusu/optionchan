from enum import Enum

import pytz
from sqlalchemy import Integer, UniqueConstraint, Index
from sqlalchemy.types import TypeDecorator

from webapp import db


class OptionType(Enum):
    CALL = 1
    PUT = 2


class AwareDateTime(TypeDecorator):

    impl = db.DateTime

    __tz_db = pytz.timezone('Asia/Tokyo')
    __tz_local = pytz.timezone('Asia/Tokyo')

    def process_bind_param(self, value, engine):
        return value

    def process_result_value(self, value, engine):
        if value is None:
            return None
        else:
            return AwareDateTime.__tz_db.localize(value).astimezone(AwareDateTime.__tz_local)


class EnumType(TypeDecorator):

    impl = Integer

    def __init__(self, *args, **kwargs):
        self.enum_class = kwargs.pop('enum_class')
        TypeDecorator.__init__(self, *args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, self.enum_class):
                raise TypeError("Value should %s type" % self.enum_class)
            return value.value

    def process_result_value(self, value, dialect):
        if value is not None:
            if not isinstance(value, int):
                raise TypeError("value should have int type")
            return self.enum_class(value)


class SpotPriceInfo(db.Model):
    __tablename__ = 'spot_price_info'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    price = db.Column(db.Float)
    price_time = db.Column(AwareDateTime)
    diff = db.Column(db.Float)
    diff_rate = db.Column(db.Float)
    hv = db.Column(db.Float)
    updated_at = db.Column(AwareDateTime, unique=True)

    def __init__(self, id, price, price_time, diff, diff_rate, hv, updated_at):
        self.id = id
        self.price = price
        self.price_time = price_time
        self.diff = diff
        self.diff_rate = diff_rate
        self.hv = hv
        self.updated_at = updated_at

    def __repr__(self):
        return '{}(id={}, price={}, price_time={}, diff={}, diff_rate={}, hv={}, updated_at={})'\
            .format(self.__class__.__name__, self.id, self.price, self.price_time, self.diff, self.diff_rate, self.hv, self.updated_at)


class FuturePriceInfo(db.Model):
    __tablename__ = 'future_price_info'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    price = db.Column(db.Integer)
    price_time = db.Column(AwareDateTime)
    diff = db.Column(db.Integer)
    diff_rate = db.Column(db.Float)
    hv = db.Column(db.Float)
    contract_month = db.Column(db.Date)
    updated_at = db.Column(AwareDateTime, unique=True)

    def __init__(self, id, price, price_time, diff, diff_rate, hv, contract_month, updated_at):
        self.id = id
        self.price = price
        self.price_time = price_time
        self.diff = diff
        self.diff_rate = diff_rate
        self.hv = hv
        self.contract_month = contract_month
        self.updated_at = updated_at

    def __repr__(self):
        return '{}(id={}, price={}, price_time={}, diff={}, diff_rate={}, hv={}, contract_month={}, updated_at={})'\
            .format(self.__class__.__name__, self.id, self.price, self.price_time, self.diff, self.diff_rate, self.hv, self.contract_month, self.updated_at)


class Option(db.Model):
    __tablename__ = 'option'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    type = db.Column(EnumType(enum_class=OptionType), nullable=False)
    target_price = db.Column(db.Integer, nullable=False)
    is_atm = db.Column(db.Boolean, index=True, nullable=False)
    price = db.Column(db.Integer)
    price_time = db.Column(AwareDateTime)
    diff = db.Column(db.Integer)
    diff_rate = db.Column(db.Float)
    iv = db.Column(db.Float)
    bid = db.Column(db.Integer)
    bid_volume = db.Column(db.Integer)
    bid_iv = db.Column(db.Float)
    ask = db.Column(db.Integer)
    ask_volume = db.Column(db.Integer)
    ask_iv = db.Column(db.Float)
    volume = db.Column(db.Integer)
    positions = db.Column(db.Integer)
    quotation = db.Column(db.Integer)
    quotation_date = db.Column(db.Date)
    delta = db.Column(db.Float)
    gamma = db.Column(db.Float)
    theta = db.Column(db.Float)
    vega = db.Column(db.Float)
    last_trading_day = db.Column(db.Date, nullable=False)
    updated_at = db.Column(AwareDateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint('type', 'target_price', 'last_trading_day', 'updated_at', name='unique_idx_option'),
    )

    def __init__(self,id, type, target_price, is_atm, price, price_time, diff, diff_rate, iv, bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions, quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, updated_at):
        self.id = id
        self.type = type
        self.target_price = target_price
        self.is_atm = is_atm
        self.price = price
        self.price_time = price_time
        self.diff = diff
        self.diff_rate = diff_rate
        self.iv = iv
        self.bid = bid
        self.bid_volume = bid_volume
        self.bid_iv = bid_iv
        self.ask = ask
        self.ask_volume = ask_volume
        self.ask_iv = ask_iv
        self.volume = volume
        self.positions = positions
        self.quotation = quotation
        self.quotation_date = quotation_date
        self.delta = delta
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        self.last_trading_day = last_trading_day
        self.updated_at = updated_at

    def __repr__(self):
        return '{}(id={}, type={}, target_price={}, is_atm={}, price={}, price_time={}, diff={}, diff_rate={}, iv={}, bid={}, bid_volume={}, bid_iv={}, ask={}, ask_volume={}, ask_iv={}, volume={}, positions={}, quotation={}, quotation_date={}, delta={}, gamma={}, theta={}, vega={}, last_trading_day={}, updated_at={})'\
            .format(self.__class__.__name__, self.id, self.type, self.target_price, self.is_atm, self.price, self.price_time, self.diff, self.diff_rate, self.iv, self.bid, self.bid_volume, self.bid_iv, self.ask, self.ask_volume, self.ask_iv, self.volume, self.positions, self.quotation, self.quotation_date, self.delta, self.gamma, self.theta, self.vega, self.last_trading_day, self.updated_at)
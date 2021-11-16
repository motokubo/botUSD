import websocket, json, pprint
import datetime
import time
import sqlite3
import logging
import requests

from binance.client import Client
from binance.enums import *
from decimal import Decimal
from datetime import date

from .database import Database
from .logger import Logger
from .binance_api_manager import BinanceAPIManager
from .config import Config
from .strategies import get_strategy
from .scheduler import SafeScheduler

SOCKET = "wss://stream.binance.com:9443/ws/ethbtc@kline_1m"

# TRADE_START_BTC_TO_ETH_VALUE = 0.62
# DIFFERENCE_PERCENTAGE = 0.009
# UP_PERCENTAGE = 1 + DIFFERENCE_PERCENTAGE
# DOWN_PERCENTAGE = 1 - DIFFERENCE_PERCENTAGE

# SIDE_BUY = 'BUY'
# SIDE_SELL = 'SELL'
# TRADE_LIMIT_BRL = 28000

# precisionBTC = 6
# precisionETH = 4

def main():
    logger = Logger()
    logger.info("Starting")
    logger.info("--------------------------------------------------------")

    config = Config()
    db = Database(logger, config)
    manager = BinanceAPIManager(config, db, logger)
    # check if we can access API feature that require valid config
    try:
        _ = manager.get_account()
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Couldn't access Binance API - API keys may be wrong or lack sufficient permissions")
        logger.error(e)
        return
    strategy = get_strategy(config.STRATEGY)
    if strategy is None:
        logger.error("Invalid strategy name")
        return
    trader = strategy(manager, db, logger, config)
    logger.info(f"Chosen strategy: {config.STRATEGY}")

    db.set_ticker_prices(manager.get_all_ticker_price())
    trader.refresh_db_balance()
    trader.getAmountTradedBRL()

    schedule = SafeScheduler(logger)
    schedule.every(1).hours.do(trader.refresh_database).tag("updating balance value")
    schedule.every(5).minutes.do(trader.printSomeInf).tag("print balance")
    schedule.every(20).seconds.do(trader.refresh_balance_tickers).tag("refresh balance tickers")
    #schedule.every(1).seconds.do(trader.refresh_balance).tag("updating balance value")
    #schedule.every(1).minutes.do(db.prune_scout_history).tag("pruning scout history")
    #schedule.every(1).hours.do(db.prune_value_history).tag("pruning value history")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
            #if not trader.is_limit_reached() and not schedule.get_jobs("createOrdersCheckOrders"):

            #else :
            #    schedule.clear("createOrdersCheckOrders")
    finally:
        manager.close()
        logger.info('Bot stopped')
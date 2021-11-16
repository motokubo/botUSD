import time
import requests
from typing import Dict, Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException
from cachetools import TTLCache, cached

from .config import Config
from .database import Database
from .logger import Logger


ORDER_TYPE_LIMIT = 'LIMIT'
TIME_IN_FORCE_GTC = 'GTC'

class BinanceAPIManager:
    def __init__(self, config: Config, db: Database, logger: Logger):
        # initializing the client class calls `ping` API endpoint, verifying the connection
        self.binance_client = Client(
            config.BINANCE_API_KEY,
            config.BINANCE_API_SECRET_KEY,
        )
        self.db = db
        self.logger = logger
        self.config = config

    def get_account(self):
        """
        Get account information
        """
        return self.binance_client.get_account()

    def get_ticker_price(self, ticker_symbol: str, force=False):
        """
        Get ticker price of a specific coin
        """
        all_tickers_prices = self.binance_client.get_symbol_ticker()
        ticker_price = next(ticker_price for ticker_price in all_tickers_prices if ticker_price["symbol"] == ticker_symbol)
        return ticker_price

    def get_all_ticker_price(self, force=False):
        """
        Get ticker price of all coins
        """
        return self.binance_client.get_symbol_ticker()
        
    def get_currency_balance(self, currency_symbol: str, force=False):
        """
        Get balance of a specific coin
        """
        return self.binance_client.get_asset_balance(currency_symbol)
        
    def get_all_currency_balance(self, force=False):
        """
        Get balance of all coin
        """
        return self.binance_client.get_account()["balances"]
            
    def close(self):
        """
        Get account information
        """
        self.cancelAllOrders(0)
        self.db.disable_bot()

    def get_open_orders(self, symbol):
        return self.binance_client.get_open_orders(symbol=symbol)

    def cancel_order(self, symbol, orderId):
        return self.binance_client.cancel_order(symbol=symbol)

    def cancelAllOrders(self, error_counter):
        self.logger.info('Closing orders')
        try:
            for symbol in self.config.SUPPORTED_SYMBOL_LIST:
                self.logger.info(symbol)
                orders = self.get_open_orders(symbol=symbol)
                for order in orders:
                    self.logger.info(order)
                    self.cancel_order(
                        symbol=symbol,
                        orderId=order["orderId"])
        except requests.exceptions.ConnectTimeout:
            if error_counter < 3:
                time.sleep(1)
                self.cancelAllOrders(error_counter + 1)
                    
    def orderLimit(self, side, quantity, symbol, price, order_type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC):
        try:
            self.logger.info("sending order")
            self.logger.debug("Quantity: %s", str(quantity))
            self.logger.debug("Price: %s", str(price))
            order = self.binance_client.create_test_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
            #order = self.binance_client.create_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
            self.logger.info(order)
        except Exception as e:
            self.logger.error("an exception occured - {}".format(e))
            return False

        return order
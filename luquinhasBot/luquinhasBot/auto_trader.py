import datetime
import time
from typing import Dict, List
from decimal import Decimal

from .binance_api_manager import BinanceAPIManager
from .config import Config
from .database import Database
from .logger import Logger


class AutoTrader:
    def __init__(self, binance_manager: BinanceAPIManager, database: Database, logger: Logger, config: Config):
        self.manager = binance_manager
        self.db = database
        self.logger = logger
        self.config = config

        self.orderBuy = None
        self.orderSell = None
        self.balances = None
        self.all_tickers_prices = None

    def start_balance_values(self):
        rows = self.db.select_table("SELECT * FROM ASSET_BALANCE WHERE user_id = ?", (self.config.USER_ID,))
        if not rows:
            self.refresh_balance()

    def getAmountTradedBRL(self):
        amount_traded_BRL = 0
        currentMonth = datetime.datetime.now().month
        currentYear = datetime.datetime.now().year

        rows = self.db.select_table("SELECT btc_amount, close_btc_to_brl FROM TRADE_HISTORIC WHERE user_id=? and strftime('%m', timestamp) = ? and strftime('%Y', timestamp) = ?", (self.config.USER_ID, str(currentMonth).zfill(2), str(currentYear).zfill(4)))
        for x in rows:
            self.logger.debug("rows: %s", x)
            amount_traded_BRL += x[0] * x[1]

        self.logger.info("Amount traded in BRL: " + str(amount_traded_BRL))
        self.AMOUNT_TRADED_BRL = amount_traded_BRL
        self.CURRENT_MONTH = currentMonth

        return amount_traded_BRL

    def printSomeInf(self):
        self.logger.info("-----------")

        for balance in self.balances:
            if balance["asset"] in self.config.SUPPORTED_COIN_LIST or balance["asset"]=="BTC":
                self.logger.info(balance["asset"], " balance:", Decimal(balance["free"]) + Decimal(balance["locked"]))
                self.logger.info("-----------")

    # def printAccountInformation(self, balanceBTC, balanceETH, close, side, amountBTC, amountSecondCoin, tax):
    #     global amountTradedInMonth
    #     symbolTicker = client.get_symbol_ticker(symbol=BTC_BRL_SYMBOL)
    #     self.logger.info("TimeStamp = " + str(datetime.datetime.now()))
    #     if close!=None:
    #         self.logger.info("ETHBTC price = " + str(close))
    #     self.logger.info("Balance BTC = " + str(balanceBTC))
    #     self.logger.info("Balance ETH = " + str(balanceETH))
    #     if side!=None:
    #         self.logger.info("Side = " + str(side))
    #     if amountBTC!=None:
    #         self.logger.info("Amount First Coin = " + str(amountBTC) + " BTC")
    #     if amountSecondCoin!=None:
    #         self.logger.info("Amount Second Coin = " + str(amountSecondCoin) + " ETH")
    #     if tax!=None:
    #         self.logger.info("Tax = " + str(tax) + "")
    #     if symbolTicker['price']!=None:
    #         self.logger.info("Preço de 1 BTC em reais = " + str(symbolTicker['price']) + "")
    #     if close!=None:
    #         totalBTC = Decimal(balanceBTC) + Decimal(balanceETH)*Decimal(close)
    #         self.logger.info("Total = " + str(totalBTC) + "")

    #     self.logger.info("--------------------------------------------------------")

    #     if side!=None and amountBTC!=None and amountSecondCoin!=None and tax!=None and symbolTicker['price']!=None :
    #         self.logger.debug("Try to insert")
    #         cur = conn.cursor()
    #         cur.execute("INSERT INTO TRADE_HISTORIC (user_id, close, side, btc_amount, second_coin, second_coin_amount, close_btc_to_real) VALUES (?,?,?,?,?,?,?)", (userID, close, side, amountBTC, "ETH", amountSecondCoin, float(symbolTicker['price'])))
    #         conn.commit()
    #         amountTradedInMonth += amountBTC * float(symbolTicker['price'])
    #         self.logger.info("Insert trade in database")

    # def orders_USD(self):
    #     try:
    #         self.refresh_balance_tickers()
    #         for balance in self.balances:
    #             if balance["asset"] in self.config.SUPPORTED_COIN_LIST:
    #                 total_asset = Decimal(balance["free"]) + Decimal(balance["locked"])
    #                 if total_asset

    #     except requests.exceptions.ConnectTimeout:
    #         print("timeout")
    #         pass

    def checkOrders(self):
        try:
            self.refresh_balance_tickers()
            for balance in self.balances:
                if balance["asset"] in self.config.SUPPORTED_COIN_LIST:
                    total_asset = Decimal(balance["free"]) + Decimal(balance["locked"])
                    if total_asset

            for supported_symbol in self.config.SUPPORTED_SYMBOL_LIST:
                open_orders = self.manager.get_open_orders(supported_symbol)
                if len(open_orders)!=2:


            #If orders do not exist
            if self.orderSell == None and self.orderBuy == None :
                self.logger.debug("Create orders")

                #Create limit orders
                if Decimal(float(balanceETH["free"])) > Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)):
                    #(side, quantity, symbol, price, order_type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC)
                    orderSell = orderLimit(SIDE_SELL, '{:0.0{}f}'.format(Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)), precisionETH), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(float(balanceBTC["free"])/TRADE_START_BTC_TO_ETH_VALUE*UP_PERCENTAGE), precisionBTC))
                    orderBuy = orderLimit(SIDE_BUY, '{:0.0{}f}'.format(Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)), precisionETH), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(float(balanceBTC["free"])/TRADE_START_BTC_TO_ETH_VALUE*DOWN_PERCENTAGE), precisionBTC))
                    balanceBTC = client.get_asset_balance(asset='BTC')
                    balanceETH = client.get_asset_balance(asset='ETH')
                #End program if do not have more side coin
                else :
                    self.logger.info("Don't have enough money to buy bitcoin")
            #If order exist
            else:
                #logging.debug("Check orders")
                #Check if sell order is filled
                currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderSell["orderId"])
                if currentOrder['status']=='FILLED':
                    self.logger.info("Sold")
                    self.logger.info(currentOrder)
                    cancelAllOrders()
                    balanceBTC = client.get_asset_balance(asset='BTC')
                    balanceETH = client.get_asset_balance(asset='ETH')
                    printAccountInformation(balanceBTC["free"], balanceETH["free"], close, currentOrder['side'], round(float(currentOrder['cummulativeQuoteQty']), 8), round(float(currentOrder['executedQty']), 8), round(float(currentOrder['executedQty'])*0.001, 8))
                    orderSell = None
                    orderBuy = None

                #Check if buy order is filled
                currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderBuy["orderId"])
                if currentOrder['status']=='FILLED':
                    self.logger.info("Bought")
                    self.logger.info(currentOrder)
                    cancelAllOrders()
                    balanceBTC = client.get_asset_balance(asset='BTC')
                    balanceETH = client.get_asset_balance(asset='ETH')
                    printAccountInformation(balanceBTC["free"], balanceETH["free"], close, currentOrder['side'], round(float(currentOrder['cummulativeQuoteQty']), 8), round(float(currentOrder['executedQty']), 8), round(float(currentOrder['cummulativeQuoteQty'])*0.001, 8))
                    orderSell = None
                    orderBuy = None
                time.sleep(1)
        except requests.exceptions.ConnectTimeout:
            print("timeout")
            pass

    def is_limit_reached(self):
        if self.AMOUNT_TRADED_BRL<self.config.TRADE_LIMIT_BRL:
            return False
        else:
            currentMonth = datetime.datetime.now().month
            if currentMonth!=self.CURRENT_MONTH:
                self.AMOUNT_TRADED_BRL = 0
                self.CURRENT_MONTH = currentMonth
            return True

    def refresh_db_balance(self):
        self.balances = self.manager.get_all_currency_balance()
        self.all_tickers_prices = self.manager.get_all_ticker_price()
        self.refresh_balance(self.balances, self.all_tickers_prices)

    def refresh_balance_tickers(self):
        self.balances = self.manager.get_all_currency_balance()
        self.all_tickers_prices = self.manager.get_all_ticker_price()

#Check profit function
    # def refresh_profit(self, balances, all_tickers_prices):
    #     rows = self.db.select_table("SELECT * FROM TRADE_HISTORIC WHERE user_id=?", (self.config.USER_ID,))
    #     for x in rows:
    #         self.logger.debug("rows: %s", x)
    #         if x[3] == SIDE_BUY and x[5] == "ETH" :
    #             #logging.debug("BUY ETH")
    #             profit -= float(x[4]) * 1.001
    #             ethAccumulation += float(x[6])
    #         elif x[3] == SIDE_SELL and x[5] == "ETH" :
    #             #logging.debug("SELL ETH")
    #             profit += float(x[4]) * 0.999
    #             ethAccumulation -= float(x[6])
    #         elif x[3] == SIDE_BUY and x[5] == "USDC" :
    #             #logging.debug("BUY BTC")
    #             profit += float(x[4]) * 0.999
    #             usdcAccumulation -= float(x[6])
    #         elif x[3] == SIDE_SELL and x[5] == "USDC" :
    #             #logging.debug("SELL BTC")
    #             profit -= float(x[4]) * 1.001
    #             usdcAccumulation += float(x[6])
        
    #     if ethAccumulation>0:
    #         profit += (ethAccumulation * symbolTickerEthBtc)
    #     if usdcAccumulation>0:
    #         profit += (usdcAccumulation / symbolTickerBtcUsdc)

    #     self.db.insert_update_table("UPDATE PROFIT SET " +
    #         "total_btc = ? " +
    #         "total_usd = ? " +
    #         "where user_id = ?", 
    #         (profit, self.config.USER_ID))

    #     return profit

    def refresh_payment_historic(self, profit):
        rows = self.db.select_table("SELECT total_btc, datetime(julianday(datetime('now'))), datetime(julianday(timestamp)), strftime('%d', julianday(datetime('now'))) - strftime('%d', datetime(julianday(timestamp))) as days_difference FROM PAYMENT_HISTORIC WHERE user_id=? ORDER BY timestamp DESC LIMIT 1", (self.config.USER_ID,))
        for x in rows:
            if x[3] > 0:
                self.logger.debug("Momento de cobrar")
                self.db.insert_update_table("INSERT INTO PAYMENT_HISTORIC (user_id, total_btc, paid) VALUES (?,?,?)", (self.config.USER_ID, profit - x[0], "pending"))

    def refresh_balance(self, balances, all_tickers_prices):
        """
        Log current value state of all altcoin balances against BTC and BUSD in DB.
        """
        btc_balance = 0

        for balance in balances:
            if balance["asset"] in self.config.SUPPORTED_COIN_LIST and balance["asset"]=="BUSD":
                ticker_price = next(ticker_price for ticker_price in all_tickers_prices if ticker_price["symbol"] == "BTC" + balance["asset"])
                btc_balance +=  (Decimal(balance["free"]) + Decimal(balance["locked"])) / Decimal(ticker_price["price"])
            elif balance["asset"] in self.config.SUPPORTED_COIN_LIST:
                ticker_price = next(ticker_price for ticker_price in all_tickers_prices if ticker_price["symbol"] == balance["asset"] + "BTC")
                btc_balance += Decimal(ticker_price["price"]) * (Decimal(balance["free"]) + Decimal(balance["locked"]))
            elif balance["asset"] == "BTC":
                btc_balance += Decimal(balance["free"]) + Decimal(balance["locked"])          
        ticker_price = next(ticker_price for ticker_price in all_tickers_prices if ticker_price["symbol"] == "BTCBUSD")
        busd_balance = btc_balance * Decimal(ticker_price["price"])
        ticker_price = next(ticker_price for ticker_price in all_tickers_prices if ticker_price["symbol"] == "BTCBRL")
        brl_balance = btc_balance * Decimal(ticker_price["price"])
        self.logger.info("total btc = " + str(btc_balance))
        self.logger.info("total busd = " + str(busd_balance))
        self.logger.info("total brl = " + str(brl_balance))
        
        self.db.insert_update_table("INSERT INTO ASSET_BALANCE (user_id, btc, usd, brl) VALUES (?,?,?,?)", (self.config.USER_ID, float(btc_balance), float(busd_balance), float(brl_balance)))

    def refresh_database(self):
        self.logger.info("VERIFY THE PROFIT FUNCTION")
        self.logger.debug("Refresh database")
        
        self.balances = self.manager.get_all_currency_balance()
        self.all_tickers_prices = self.manager.get_all_ticker_price()
        self.refresh_balance(self.balances, self.all_tickers_prices)
        ##profit = self.refresh_profit(balances, all_tickers_prices)
        ##self.refresh_payment_historic(profit)
        
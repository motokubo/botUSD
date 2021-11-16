import websocket, json, pprint
import datetime
import sched, time
import sqlite3
import logging
import logger
import requests

from config import API_KEY, API_SECRET, USER, TELEGRAM_ID
from binance.client import Client
from binance.enums import *
from decimal import Decimal
from datetime import date

SOCKET = "wss://stream.binance.com:9443/ws/ethbtc@kline_1m"

TRADE_START_BTC_TO_ETH_VALUE = 0.575
TRADE_SYMBOL = 'ETHBTC'
BTC_USDC_SYMBOL = 'BTCUSDC'
ETH_BTC_SYMBOL = 'ETHBTC'
BTC_BRL_SYMBOL = 'BTCBRL'
DIFFERENCE_PERCENTAGE = 0.009
UP_PERCENTAGE = 1 + DIFFERENCE_PERCENTAGE
DOWN_PERCENTAGE = 1 - DIFFERENCE_PERCENTAGE

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
ORDER_TYPE_MARKET = 'MARKET'
ORDER_TYPE_LIMIT = 'LIMIT'
TIME_IN_FORCE_GTC = 'GTC'
TRADE_LIMIT_BRL = 28000
DAYS_TO_PAYMENT = 7

client = Client(API_KEY, API_SECRET)
orderBuy = None
orderSell = None

precisionBTC = 6
precisionETH = 4
count = 0
error_counter = 0

balanceBTC = client.get_asset_balance(asset='BTC')
balanceETH = client.get_asset_balance(asset='ETH')
lastHour = None
lastMonth = datetime.datetime.now().month

userID = None

logger.setup_logger()

# logging.debug("The debug")
# logging.info("The info")
# logging.warning("The warn")
# logging.error("The error")
# logging.critical("The critical")
    
def orderLimit(side, quantity, symbol, price, order_type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC):
    try:
        logging.info("sending order")
        logging.debug("Quantity: %s", str(quantity))
        logging.debug("Price: %s", str(price))
        #order = client.create_test_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
        order = client.create_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
        logging.info(order)
    except Exception as e:
        logging.error("an exception occured - {}".format(e))
        return False

    return order

def connect_database():
    conn = sqlite3.connect('../database.db')
    logging.info("Opened database successfully")
    return conn
    
def create_tables(conn):
    cur = conn.cursor()
    logging.debug("Creating tables")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS USER(
            id integer PRIMARY KEY AUTOINCREMENT,
            name text NOT NULL,
            telegram_id integer,
            status text
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS PAYMENT_HISTORIC(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            total_btc real NOT NULL,
            paid text NOT NULL,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS TRADE_HISTORIC(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            close real NOT NULL,
            side text NOT NULL,
            btc_amount real NOT NULL,
            second_coin text NOT NULL,
            second_coin_amount real NOT NULL,
            close_btc_to_real real NOT NULL,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS ASSET_BALANCE(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            btc real NOT NULL,
            usdc real NOT NULL,
            eth real NOT NULL,
            total_amount_real real NOT NULL,
            total_amount_btc real NOT NULL,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS PROFIT(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            total_btc real NOT NULL,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""")
    
    conn.commit()
    
def initialize_user(conn):
    global userID
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM USER WHERE name=?", (USER,))
    conn.commit()

    rows = cur.fetchall()
    #Create user if do not exist
    if rows:
        logging.info("User already created")
        cur.execute("UPDATE USER SET status = ? WHERE name = ?", ("Active", USER))
    else:
        cur.execute("INSERT INTO USER (name, telegram_id, status) VALUES (?,?,?)", (USER,TELEGRAM_ID,"Active"))
        
    conn.commit()

    cur.execute("SELECT * FROM USER WHERE name=?", (USER,))
    conn.commit()
    rows = cur.fetchall()
    logging.debug(rows)
    userID = rows[0][0]

    cur.execute("SELECT * FROM PAYMENT_HISTORIC WHERE user_id=?", (userID,))
    conn.commit()
    rows = cur.fetchall()
    logging.debug(rows)
    if not rows:
        cur.execute("INSERT INTO PAYMENT_HISTORIC (user_id, total_btc, paid) VALUES (?,?,?)", (userID,0,"Paid"))
        conn.commit()
    else :
        logging.info("User already registered in PAYMENT_HISTORIC")
        
    cur.execute("SELECT * FROM ASSET_BALANCE WHERE user_id=?", (userID,))
    conn.commit()
    rows = cur.fetchall()
    logging.debug(rows)
    if not rows:
        cur.execute("INSERT INTO ASSET_BALANCE (user_id, btc, usdc, eth, total_amount_real, total_amount_btc) VALUES (?,?,?,?,?,?)", (userID,0,0,0,0,0))
        conn.commit()
    else :
        logging.info("User already registered in ASSET_BALANCE")
        
    cur.execute("SELECT * FROM PROFIT WHERE user_id=?", (userID,))
    conn.commit()
    rows = cur.fetchall()
    logging.debug(rows)
    if not rows:
        cur.execute("INSERT INTO PROFIT (user_id, total_btc) VALUES (?,?)", (userID,0))
        conn.commit()
    else :
        logging.info("User already registered in PROFIT")

def printSomeInf(close):
    global count
    count += 1
    if count>=10:
        count=0
        print("-----------")
        print("Amount in BTC converted to ETH:", str(round(Decimal(float(balanceBTC["free"]) + float(balanceBTC["locked"])),8)/Decimal(close)))
        print("-----------")
        print("When should buy ETH:", str(Decimal(TRADE_START_BTC_TO_ETH_VALUE) * Decimal(UP_PERCENTAGE)))
        print("When should sell ETH:", str(Decimal(TRADE_START_BTC_TO_ETH_VALUE) * Decimal(DOWN_PERCENTAGE)))
        print("-----------")

def printAccountInformation(balanceBTC, balanceETH, close, side, amountBTC, amountSecondCoin, tax):
    global amountTradedInMonth
    symbolTicker = client.get_symbol_ticker(symbol=BTC_BRL_SYMBOL)
    logging.info("TimeStamp = " + str(datetime.datetime.now()))
    if close!=None:
        logging.info("ETHBTC price = " + str(close))
    logging.info("Balance BTC = " + str(balanceBTC))
    logging.info("Balance ETH = " + str(balanceETH))
    if side!=None:
        logging.info("Side = " + str(side))
    if amountBTC!=None:
        logging.info("Amount First Coin = " + str(amountBTC) + " BTC")
    if amountSecondCoin!=None:
        logging.info("Amount Second Coin = " + str(amountSecondCoin) + " ETH")
    if tax!=None:
        logging.info("Tax = " + str(tax) + "")
    if symbolTicker['price']!=None:
        logging.info("Preço de 1 BTC em reais = " + str(symbolTicker['price']) + "")
    if close!=None:
        totalBTC = Decimal(balanceBTC) + Decimal(balanceETH)*Decimal(close)
        logging.info("Total = " + str(totalBTC) + "")

    logging.info("--------------------------------------------------------")

    if side!=None and amountBTC!=None and amountSecondCoin!=None and tax!=None and symbolTicker['price']!=None :
        logging.debug("Try to insert")
        cur = conn.cursor()
        cur.execute("INSERT INTO TRADE_HISTORIC (user_id, close, side, btc_amount, second_coin, second_coin_amount, close_btc_to_real) VALUES (?,?,?,?,?,?,?)", (userID, close, side, amountBTC, "ETH", amountSecondCoin, float(symbolTicker['price'])))
        conn.commit()
        amountTradedInMonth += amountBTC * float(symbolTicker['price'])
        logging.info("Insert trade in database")

def checkOrders(close):
    global orderSell, orderBuy, balanceBTC, balanceETH

    try:
        #If orders do not exist
        if orderSell == None and orderBuy == None :
            logging.debug("Create orders")

            #Create limit orders
            if Decimal(float(balanceETH["free"])) > Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)):
                #(side, quantity, symbol, price, order_type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC)
                orderSell = orderLimit(SIDE_SELL, '{:0.0{}f}'.format(Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)), precisionETH), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(float(balanceBTC["free"])/TRADE_START_BTC_TO_ETH_VALUE*UP_PERCENTAGE), precisionBTC))
                orderBuy = orderLimit(SIDE_BUY, '{:0.0{}f}'.format(Decimal((float(balanceBTC["free"])*DIFFERENCE_PERCENTAGE)/float(close)), precisionETH), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(float(balanceBTC["free"])/TRADE_START_BTC_TO_ETH_VALUE*DOWN_PERCENTAGE), precisionBTC))
                balanceBTC = client.get_asset_balance(asset='BTC')
                balanceETH = client.get_asset_balance(asset='ETH')
            #End program if do not have more side coin
            else :
                logging.info("Don't have enough money to buy bitcoin")
                on_close(None)
                ws.close()
        #If order exist
        else:
            #logging.debug("Check orders")
            #Check if sell order is filled
            currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderSell["orderId"])
            if currentOrder['status']=='FILLED':
                logging.info("Sold")
                logging.info(currentOrder)
                cancelAllOrders()
                balanceBTC = client.get_asset_balance(asset='BTC')
                balanceETH = client.get_asset_balance(asset='ETH')
                printAccountInformation(balanceBTC["free"], balanceETH["free"], close, currentOrder['side'], round(float(currentOrder['cummulativeQuoteQty']), 8), round(float(currentOrder['executedQty']), 8), round(float(currentOrder['executedQty'])*0.001, 8))
                orderSell = None
                orderBuy = None

            #Check if buy order is filled
            currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderBuy["orderId"])
            if currentOrder['status']=='FILLED':
                logging.info("Bought")
                logging.info(currentOrder)
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

def getAmountTradedBRL():
    actualtradeBRL = 0
    currentMonth = datetime.datetime.now().month
    currentYear = datetime.datetime.now().year

    cur = conn.cursor()
    
    cur.execute("SELECT * FROM TRADE_HISTORIC WHERE user_id=? and strftime('%m', timestamp) = ? and strftime('%Y', timestamp) = ?", (userID, str(currentMonth).zfill(2), str(currentYear).zfill(4)))
    conn.commit()

    rows = cur.fetchall()
    for x in rows:
        logging.debug("rows: %s", x)
        actualtradeBRL += x[4] * x[7]

    logging.info("Amount traded in BRL: %s", str(actualtradeBRL))

    return actualtradeBRL

def checkAmountTradedBRL():
    global amountTradedInMonth, lastMonth
    if amountTradedInMonth<TRADE_LIMIT_BRL:
        return True
    else:
        currentMonth = datetime.datetime.now().month
        if currentMonth!=lastMonth:
            amountTradedInMonth = 0
            lastMonth = currentMonth
        return False

def days_between(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

def refresh_database():
    global lastHour
    currentHour = datetime.datetime.now().hour
    cur = conn.cursor()
    if lastHour != currentHour:
        logging.debug("Refresh database")
        time.sleep(10)
        profit = float(0)
        ethAccumulation = float(0)
        usdcAccumulation = float(0)
        currentAssetBalanceBTC = client.get_asset_balance(asset='BTC')
        currentAssetBalanceETH = client.get_asset_balance(asset='ETH')
        currentAssetBalanceUSDC = client.get_asset_balance(asset='USDC')
        symbolTickerBtcBrl = float(client.get_symbol_ticker(symbol=BTC_BRL_SYMBOL)["price"])
        symbolTickerBtcUsdc = float(client.get_symbol_ticker(symbol=BTC_USDC_SYMBOL)["price"])
        symbolTickerEthBtc = float(client.get_symbol_ticker(symbol=ETH_BTC_SYMBOL)["price"])

        currentBalanceBTC = float(currentAssetBalanceBTC["free"]) + float(currentAssetBalanceBTC["locked"])
        currentBalanceETH = float(currentAssetBalanceETH["free"]) + float(currentAssetBalanceETH["locked"])
        currentBalanceUSDC = float(currentAssetBalanceUSDC["free"]) + float(currentAssetBalanceUSDC["locked"])

        logging.debug("Symbol ticker BTC/BRL: %f", symbolTickerBtcBrl)
        logging.debug("Symbol ticker BTC/USDC: %f", symbolTickerBtcUsdc)
        logging.debug("Symbol ticker ETH/BTC: %f", symbolTickerEthBtc)
        logging.debug("Current BTC balance: %f", currentBalanceBTC)
        logging.debug("Current ETH balance: %f", currentBalanceETH)
        logging.debug("Current USDC balance: %f", currentBalanceUSDC)
        logging.debug("USDC to BTC: %f", (currentBalanceUSDC / symbolTickerBtcUsdc))
        logging.debug("ETH to BTC: %f", (currentBalanceETH * symbolTickerEthBtc))

        convertedBalanceBTC = currentBalanceBTC + (currentBalanceETH * symbolTickerEthBtc) + (currentBalanceUSDC / symbolTickerBtcUsdc)

        logging.debug("BTC to BRL: %f", (convertedBalanceBTC * symbolTickerBtcBrl))
        currentBalanceBRL = convertedBalanceBTC * symbolTickerBtcBrl
        
        cur.execute("UPDATE ASSET_BALANCE SET " +
            "btc = ?, usdc = ?, eth= ?, total_amount_real = ?, total_amount_btc = ? " +
            "where user_id = ?", 
            (currentBalanceBTC, currentBalanceUSDC, currentBalanceETH, currentBalanceBRL, convertedBalanceBTC, userID))
        conn.commit()

        cur.execute("SELECT * FROM TRADE_HISTORIC WHERE user_id=?", (userID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            logging.debug("rows: %s", x)
            if x[3] == SIDE_BUY and x[5] == "ETH" :
                #logging.debug("BUY ETH")
                profit -= float(x[4]) * 1.001
                ethAccumulation += float(x[6])
            elif x[3] == SIDE_SELL and x[5] == "ETH" :
                #logging.debug("SELL ETH")
                profit += float(x[4]) * 0.999
                ethAccumulation -= float(x[6])
            elif x[3] == SIDE_BUY and x[5] == "USDC" :
                #logging.debug("BUY BTC")
                profit += float(x[4]) * 0.999
                usdcAccumulation -= float(x[6])
            elif x[3] == SIDE_SELL and x[5] == "USDC" :
                #logging.debug("SELL BTC")
                profit -= float(x[4]) * 1.001
                usdcAccumulation += float(x[6])
        
        if ethAccumulation>0:
            profit += (ethAccumulation * symbolTickerEthBtc)
        if usdcAccumulation>0:
            profit += (usdcAccumulation / symbolTickerBtcUsdc)

        cur.execute("UPDATE PROFIT SET " +
            "total_btc = ? " +
            "where user_id = ?", 
            (profit, userID))
        conn.commit()

        cur.execute("SELECT total_btc, datetime(julianday(datetime('now'))), datetime(julianday(timestamp)), strftime('%d', julianday(datetime('now'))) - strftime('%d', datetime(julianday(timestamp))) as days_difference FROM PAYMENT_HISTORIC WHERE user_id=? ORDER BY timestamp DESC LIMIT 1", (userID,))
        conn.commit()
        rows = cur.fetchall()
        for x in rows:
            if x[3] > 0:
                logging.debug("Momento de cobrar")
                cur.execute("INSERT INTO PAYMENT_HISTORIC (user_id, total_btc, paid) VALUES (?,?,?)", (userID, profit - x[0], "pending"))
                conn.commit()

        lastHour = currentHour

def cancelAllOrders():
    global error_counter
    logging.info('Closing orders')
    try:
        orders = client.get_open_orders(symbol=TRADE_SYMBOL)
        for x in orders:
            logging.info(x)
            client.cancel_order(
                symbol=TRADE_SYMBOL,
                orderId=x["orderId"])    
    except requests.exceptions.ConnectTimeout:
        if error_counter < 3:
            time.sleep(1)
            error_counter += 1
            cancelAllOrders()
        error_counter = 0

def on_open(ws):
    logging.info('Opened connection')

def on_close(ws):
    cancelAllOrders()
    cur = conn.cursor()
    cur.execute("UPDATE USER SET status = ? WHERE name = ?", ("Inactive", USER))
    conn.commit()
    logging.info('Closed connection')

def on_message(ws, message): 
    #logging.debug('received message')
    json_message = json.loads(message)
    #pprint.pprint(json_message)
    candle = json_message['k']
    close = candle['c']
    print("close:", str(round(Decimal(close), 6)))

    if checkAmountTradedBRL():
        checkOrders(close)
    
    printSomeInf(close)
    refresh_database()

conn = connect_database()
create_tables(conn)
initialize_user(conn)
amountTradedInMonth = getAmountTradedBRL()

logging.info("Start")
logging.info("--------------------------------------------------------")
printAccountInformation(balanceBTC["free"], balanceETH["free"], None, None, None, None, None)

ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
ws.run_forever()
import websocket, json, pprint, talib, numpy
import datetime
import time
import sqlite3
import signal
from config import API_KEY, API_SECRET, USER, TELEGRAM_ID
from binance.client import Client
from binance.enums import *
from decimal import Decimal

SOCKET = "wss://stream.binance.com:9443/ws/btcusdc@kline_1m"

TRADE_START_BTC_TO_USDC_VALUE = 2920
TRADE_SYMBOL = 'BTCUSDC'
BTC_BRL_SYMBOL = 'BTCBRL'
DIFFERENCE_PERCENTAGE = 0.011
UP_PERCENTAGE = 1 + DIFFERENCE_PERCENTAGE
DOWN_PERCENTAGE = 1 - DIFFERENCE_PERCENTAGE

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
ORDER_TYPE_MARKET = 'MARKET'
ORDER_TYPE_LIMIT = 'LIMIT'
TIME_IN_FORCE_GTC = 'GTC'
TRADE_LIMIT_BRL = 28000

client = Client(API_KEY, API_SECRET)
orderBuy = None
orderSell = None

precisionBTC = 5
precisionUSDC = 2
count = 0

balanceBTC = client.get_asset_balance(asset='BTC')["free"]
balanceUSDC = client.get_asset_balance(asset='USDC')["free"]
lastMonth = datetime.datetime.now().month

userID = None

firstTick = True
f = open("log.txt", "a")
    
def orderLimit(side, quantity, symbol, price, order_type=ORDER_TYPE_LIMIT, timeInForce=TIME_IN_FORCE_GTC):
    try:
        print("sending order")
        #order = client.create_test_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
        order = client.create_order(symbol=symbol, side=side, type=order_type, price=price, quantity=quantity, timeInForce=timeInForce)
        print(order)
    except Exception as e:
        print("an exception occured - {}".format(e))
        return False

    return order

def connect_database():
    conn = sqlite3.connect('../database.db')
    print("Opened database successfully")
    return conn
    
def create_tables(conn):
    global userID
    cur = conn.cursor()
    print("Creating tables")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS USER(
            id integer PRIMARY KEY AUTOINCREMENT,
            name text NOT NULL,
            telegram_id text,
            status text
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS PAYMENT_HISTORIC(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            total_btc real NOT NULL,
            timestamp DATETIME NOT NULL
            );""")

    cur.execute(
        """CREATE TABLE IF NOT EXISTS TRADE_HISTORIC(
            id integer PRIMARY KEY AUTOINCREMENT,
            user_id integer REFERENCES USER(id),
            close real NOT NULL,
            side text NOT NULL,
            first_coin text NOT NULL,
            amount_first_coin real NOT NULL,
            second_coin text NOT NULL,
            amount_second_coin real NOT NULL,
            tax real NOT NULL,
            btc_to_real real NOT NULL,
            timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""")
    
    cur.execute("SELECT * FROM USER WHERE name=?", (USER,))
    conn.commit()

    rows = cur.fetchall()
    #Create user if do not exist
    if rows:
        print("User already created")
        cur.execute("UPDATE USER SET status = ? WHERE name = ?", ("Active", USER))
    else:
        cur.execute("INSERT INTO USER (name, telegram_id, status) VALUES (?,?,?)", (USER,TELEGRAM_ID,"Active"))
        
    conn.commit()

    cur.execute("SELECT * FROM USER WHERE name=?", (USER,))
    conn.commit()
    rows = cur.fetchall()
    print(rows)
    userID = rows[0][0]


def printAccountInformation(balanceBTC, balanceUSDC, close, side, amountFirstCoin, amountSecondCoin, tax):
    global amountTradedInMonth
    symbolTicker = client.get_symbol_ticker(symbol=BTC_BRL_SYMBOL)
    print(symbolTicker['price'])
    f.write("TimeStamp = " + str(datetime.datetime.now()) + "\n")
    f.write("BTC price = " + close + "\n")
    f.write("BalanceBTC = " + balanceBTC + "\n")
    f.write("BalanceUSDC = " + balanceUSDC + "\n") 
    if side!=None:
        f.write("Side = " + side + "\n")
    if amountFirstCoin!=None:
        f.write("Amount First Coin = " + str(amountFirstCoin) + " BTC\n")
    if amountSecondCoin!=None:
        f.write("Amount Second Coin = " + str(amountSecondCoin) + " USDC\n")
    if tax!=None:
        f.write("Tax = " + str(tax) + "\n")

    totalBTC = Decimal(balanceBTC) + Decimal(balanceUSDC)/Decimal(close)
    print(totalBTC)
    f.write("Total = " + str(totalBTC) + "\n")
    f.write("--------------------------------------------------------\n")
    f.flush()

    if side!=None and amountFirstCoin!=None and amountSecondCoin!=None and tax!=None :
        print("Try to insert")
        cur = conn.cursor()
        cur.execute("INSERT INTO TRADE_HISTORIC (user_id, close, side, first_coin, amount_first_coin, second_coin, amount_second_coin, tax, btc_to_real) VALUES (?,?,?,?,?,?,?,?,?)", (userID, close, side, "BTC", amountFirstCoin, "USDC", amountSecondCoin, tax, float(symbolTicker['price'])))
        conn.commit()
        amountTradedInMonth += amountFirstCoin * float(symbolTicker['price'])
        print("Insert in trade history")

def getAmountTradedBRL():
    actualtradeBRL = 0
    currentMonth = datetime.datetime.now().month
    currentYear = datetime.datetime.now().year

    cur = conn.cursor()
    
    cur.execute("SELECT * FROM TRADE_HISTORIC WHERE user_id=? and strftime('%m', timestamp) = ? and strftime('%Y', timestamp) = ?", (userID, currentMonth, currentYear))
    conn.commit()

    rows = cur.fetchall()
    for x in rows:
        actualtradeBRL += x[4] * x[9]

    return actualtradeBRL
    
def cancelAllOrders():
    orders = client.get_open_orders(symbol=TRADE_SYMBOL)
    for x in orders:
        print(x)
        client.cancel_order(
            symbol=TRADE_SYMBOL,
            orderId=x["orderId"])

def on_open(ws):
    print('Opened connection')

def on_close(ws):
    print('Closing orders')
    cancelAllOrders()
    cur = conn.cursor()
    cur.execute("UPDATE USER SET status = ? WHERE name = ?", ("Inactive", USER))
    conn.commit()
    print('Closed connection')

def on_message(ws, message): 
    global firstTick, orderSell, orderBuy, balanceBTC, balanceUSDC, count, lastMonth, amountTradedInMonth
    count+=1
    #print('received message')
    json_message = json.loads(message)
    #pprint.pprint(json_message)
    candle = json_message['k']
    close = candle['c']
    print("close", round(Decimal(close), 2))

    if amountTradedInMonth<TRADE_LIMIT_BRL:
        if firstTick == True:
            f.write("Start\n")
            f.write("--------------------------------------------------------\n")
            firstTick = False
            printAccountInformation(balanceBTC, balanceUSDC, close, None, None, None, None)

        #print(Decimal(float(balanceBTC)*DIFFERENCE_PERCENTAGE) * Decimal(TRADE_START_BTC_TO_USDC_VALUE*DOWN_PERCENTAGE/float(balanceBTC)))
        if orderSell == None and orderBuy == None :
            #print("Create orders")
            if Decimal(float(balanceUSDC)) > Decimal(float(balanceBTC)*DIFFERENCE_PERCENTAGE) * Decimal(TRADE_START_BTC_TO_USDC_VALUE*DOWN_PERCENTAGE/float(balanceBTC)):
                orderSell = orderLimit(SIDE_SELL, '{:0.0{}f}'.format(Decimal(float(balanceBTC)*DIFFERENCE_PERCENTAGE/UP_PERCENTAGE), precisionBTC), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(TRADE_START_BTC_TO_USDC_VALUE*UP_PERCENTAGE/float(balanceBTC)), precisionUSDC))
                orderBuy = orderLimit(SIDE_BUY, '{:0.0{}f}'.format(Decimal(float(balanceBTC)*DIFFERENCE_PERCENTAGE/DOWN_PERCENTAGE), precisionBTC), TRADE_SYMBOL, '{:0.0{}f}'.format(Decimal(TRADE_START_BTC_TO_USDC_VALUE*DOWN_PERCENTAGE/float(balanceBTC)), precisionUSDC))
                balanceBTC = client.get_asset_balance(asset='BTC')["free"]
                balanceUSDC = client.get_asset_balance(asset='USDC')["free"]
            else :
                print("Don't have enough money to buy bitcoin")
                on_close(None)
                ws.close()
        else:
            #print("Check orders")
            currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderSell["orderId"])
            if currentOrder['status']=='FILLED':
                print("Sold")
                print(currentOrder)
                cancelAllOrders()
                balanceBTC = client.get_asset_balance(asset='BTC')["free"]
                print(balanceBTC)
                balanceUSDC = client.get_asset_balance(asset='USDC')["free"]
                print(balanceUSDC)
                printAccountInformation(balanceBTC, balanceUSDC, close, currentOrder['side'], round(float(currentOrder['executedQty']), 8), round(float(currentOrder['cummulativeQuoteQty']), 8), round(float(currentOrder['cummulativeQuoteQty'])*0.001, 8))
                orderSell = None
                orderBuy = None
            currentOrder = client.get_order(symbol=TRADE_SYMBOL,orderId=orderBuy["orderId"])
            if currentOrder['status']=='FILLED':
                print("Bought")
                print(currentOrder)
                cancelAllOrders()
                balanceBTC = client.get_asset_balance(asset='BTC')["free"]
                print(balanceBTC)
                balanceUSDC = client.get_asset_balance(asset='USDC')["free"]
                print(balanceUSDC)
                printAccountInformation(balanceBTC, balanceUSDC, close, currentOrder['side'], round(float(currentOrder['executedQty']), 8), round(float(currentOrder['cummulativeQuoteQty']), 8), round(float(currentOrder['executedQty'])*0.001, 8))
                orderSell = None
                orderBuy = None
            time.sleep(1)

        actualBalanceBTC = client.get_asset_balance(asset='BTC')

        # if round(Decimal(actualBalanceBTC["free"]),8) != round(Decimal(balanceBTC),8):
        #     print(round(Decimal(actualBalanceBTC["free"]),8))
        #     print(round(Decimal(balanceBTC),8))
        #     print("User added BTC to account")
        #     on_close(None)
        #     ws.close()

        if count>=10:
            count=0

            print("-----------")
            print("Amount in BTC converted to USD: ", round(Decimal(float(actualBalanceBTC["free"]) + float(actualBalanceBTC["locked"])),8) * Decimal(close))
            print("-----------")
            print("When should sell: ", Decimal(TRADE_START_BTC_TO_USDC_VALUE) * Decimal(UP_PERCENTAGE))
            print("When should buy: ", Decimal(TRADE_START_BTC_TO_USDC_VALUE) * Decimal(DOWN_PERCENTAGE))
            print("-----------")
    else:
        currentMonth = datetime.datetime.now().month
        if currentMonth!=lastMonth:
            amountTradedInMonth = 0
            lastMonth = currentMonth

conn = connect_database()
create_tables(conn)
amountTradedInMonth = getAmountTradedBRL()

ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
ws.run_forever()
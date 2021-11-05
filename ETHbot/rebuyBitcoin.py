from config import API_KEY, API_SECRET, USER, TELEGRAM_ID
from binance.client import Client
from binance.enums import *
import sqlite3
import logging

TRADE_SYMBOL1 = 'BTCUSDC'
TRADE_SYMBOL2 = 'ETHBTC'
BTC_BRL_SYMBOL = 'BTCBRL'
SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
ORDER_TYPE_MARKET = 'MARKET'
TIME_IN_FORCE_GTC = 'GTC'
client = Client(API_KEY, API_SECRET)

balanceETH = float(client.get_asset_balance(asset='ETH')['free'])
balanceUSDC = float(client.get_asset_balance(asset='USDC')['free'])
symbolTickerBtcUsdc = float(client.get_symbol_ticker(symbol=TRADE_SYMBOL1)["price"])
symbolTickerEthBtc = float(client.get_symbol_ticker(symbol=TRADE_SYMBOL2)["price"])
symbolTickerBtcBrl = float(client.get_symbol_ticker(symbol=BTC_BRL_SYMBOL)["price"])

def connect_database():
    conn = sqlite3.connect('../database.db')
    logging.info("Opened database successfully")
    return conn

conn = connect_database()
cur = conn.cursor()

cur.execute("SELECT * FROM USER WHERE name=?", (USER,))
conn.commit()
rows = cur.fetchall()
logging.debug(rows)
userID = rows[0][0]

if balanceUSDC > 11.0:
    order = client.create_order(symbol=TRADE_SYMBOL1, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=balanceUSDC/symbolTickerBtcUsdc)
    cur = conn.cursor()
    cur.execute("INSERT INTO TRADE_HISTORIC (user_id, close, side, btc_amount, second_coin, second_coin_amount, close_btc_to_real) VALUES (?,?,?,?,?,?,?)", (userID, symbolTickerBtcUsdc, SIDE_BUY, balanceUSDC/symbolTickerBtcUsdc, "USDC", balanceUSDC, symbolTickerBtcBrl))
    conn.commit()


if balanceETH * symbolTickerEthBtc > 0.00012:
    order = client.create_order(symbol=TRADE_SYMBOL2, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=balanceETH)
    cur = conn.cursor()
    cur.execute("INSERT INTO TRADE_HISTORIC (user_id, close, side, btc_amount, second_coin, second_coin_amount, close_btc_to_real) VALUES (?,?,?,?,?,?,?)", (userID, symbolTickerEthBtc, SIDE_SELL, balanceETH * symbolTickerEthBtc, "ETH", balanceETH, symbolTickerBtcBrl))
    conn.commit()

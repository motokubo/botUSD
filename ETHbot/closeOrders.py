from config import API_KEY, API_SECRET
from binance.client import Client
from binance.enums import *

TRADE_SYMBOL1 = 'BTCUSDC'
TRADE_SYMBOL2 = 'ETHBTC'
client = Client(API_KEY, API_SECRET)

orders = client.get_open_orders(symbol=TRADE_SYMBOL1)
for x in orders:
    print(x)
    client.cancel_order(
        symbol=TRADE_SYMBOL1,
        orderId=x["orderId"])

orders = client.get_open_orders(symbol=TRADE_SYMBOL2)
for x in orders:
    print(x)
    client.cancel_order(
        symbol=TRADE_SYMBOL2,
        orderId=x["orderId"])
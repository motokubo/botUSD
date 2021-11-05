import decimal
import logging
import time
import uuid
import copy
from datetime import datetime, timedelta


class BacktestClient():

    def __init__(self, initial_balance):
        logging.debug('BacktestClient: init...')

        self.orders = []
        self.cancels = []
        self.balance = [
            {
                "asset": "BTC",
                "free": self.round_floor(decimal.Decimal(initial_balance), 6),
                "locked": decimal.Decimal(0)
            },
            {
                "asset": "USDT",
                "free": decimal.Decimal(0),
                "locked": decimal.Decimal(0)
            },
            {
                "asset": "USDC",
                "free": decimal.Decimal(0),
                "locked": decimal.Decimal(0)
            }
        ]
        self.timestamps = []
        self.wins = 0
        self.losses = 0
        self.profit = 0
        self.processes = 0
        self.initial_balance = copy.deepcopy(self.balance)

    def get_historical_klines(self, _, __, ___):
        logging.debug('BacktestClient: get_historical_klines...')
        return []

    def get_asset_balance(self, asset='BTC'):
        logging.debug('BacktestClient: get_asset_balance...')
        return next(balance for balance in self.balance if balance["asset"] == asset)

    def get_open_orders(self, symbol):
        logging.debug('BacktestClient: get_open_orders...')
        return [
            order for order in self.orders
            if (
                order['symbol'] == symbol and
                self.is_open(order)
            )
        ]

    def get_order(self, symbol, orderId):
        logging.debug('BacktestClient: get_order...')
        return next(
            order for order in self.orders
            if order['symbol'] == symbol and order['orderId'] == orderId
        )

    def cancel_order(self, symbol, orderId):
        logging.debug('BacktestClient: cancel_order...')
        order = next(
            order for order in self.orders
            if order['symbol'] == symbol and order['orderId'] == orderId
        )
        cancel = {
            "symbol": order['symbol'],
            "origClientOrderId": order['clientOrderId'],
            "orderId": order['orderId'],
            "clientOrderId": f"cancel{order['clientOrderId']}"
        }
        order['status'] = 'CANCELED'
        self.update_balance(
            order['symbol'], order['side'], order['price'], order['origQty'], 'CANCEL')
        return cancel

    def create_order(
        self, symbol, side, type, timeInForce, quantity, price, stopPrice=None,
        newClientOrderId=None
    ):
        logging.debug('BacktestClient: create_order...')

        if type == 'LIMIT':
            status = 'NEW'
            self.update_balance(symbol, side, price, quantity, 'CREATE')
        elif type == 'MARKET':
            status = 'FILLED'
            self.update_balance(symbol, side, price, quantity, 'CREATE')
            self.update_balance(symbol, side, price, quantity, 'FILL')

        orderId = str(uuid.uuid4())
        order = {
            "symbol": symbol,  # BTCUSDC | BTCUSDT
            "orderId": orderId,
            "clientOrderId": newClientOrderId or orderId,
            "price": price,
            "origQty": quantity,
            "executedQty": 0,
            "status": status,  # FILLED | PARTIALLY_FILLED | CANCELED | PENDING_CANCEL | REJECTED | EXPIRED | NEW
            "timeInForce": timeInForce,  # GTC | IOC | FOK
            "type": type,  # LIMIT | MARKET | STOP_LOSS | STOP_LOSS_LIMIT | TAKE_PROFIT | TAKE_PROFIT_LIMIT | LIMIT_MAKER
            "side": side,  # BUY | SELL
            "stopPrice": stopPrice,
            "icebergQty": 0,
            "time": time.time()
        }
        self.orders.append(order)
        return order

    def order_market_buy(self, symbol, quoteOrderQty):
        logging.debug('BacktestClient: order_market_buy...')

        order = self.create_order(
            symbol=symbol, side='BUY', type='MARKET', timeInForce='GTC',
            quantity=self.round_floor(quoteOrderQty/self.last_candle['c'], 6),
            price=self.last_candle['c']
        )

        self.loss(self.last_candle['c'])

        return order

    def order_market_sell(self, symbol, quantity):
        logging.debug('BacktestClient: order_market_sell...')

        self.start_process(self.last_candle['c'])

        return self.create_order(
            symbol=symbol, side='SELL', type='MARKET', timeInForce='GTC',
            quantity=quantity, price=self.last_candle['c']
        )

    def process_orders(self, candle):
        logging.debug('BacktestClient: process_orders...')

        self.last_candle = candle
        self.last_candle['c'] = decimal.Decimal(self.last_candle['c'])

        high = decimal.Decimal(candle['h'])
        low = decimal.Decimal(candle['l'])

        buy_order_index = next(
            (
                index for (index, order) in enumerate(self.orders)
                if(
                    order['side'] == 'BUY' and
                    self.is_open(order)
                )
            ),
            None
        )
        sell_orders_index = [
            index for (index, order) in enumerate(self.orders)
            if (
                order['side'] == 'SELL' and
                self.is_open(order)
            )
        ]

        if buy_order_index and float(self.orders[buy_order_index]['price']) >= low:
            logging.debug('BacktestClient: buy order filled')

            self.orders[buy_order_index]['status'] = 'FILLED'
            buy_order = self.orders[buy_order_index]
            self.update_balance(
                buy_order['symbol'], 'BUY', buy_order['price'], buy_order['origQty'], 'FILL')

            self.win(buy_order['price'])
        else:
            for sell_order_index in sell_orders_index:
                sell_order = self.orders[sell_order_index]
                if sell_order['price'] <= high:
                    logging.debug('BacktestClient: sell order filled')
                    self.orders[sell_order_index]['status'] = 'FILLED'
                    self.update_balance(
                        sell_order['symbol'], 'SELL', sell_order['price'], sell_order['origQty'],
                        'FILL'
                    )

    def update_balance(self, symbol, side, price, qty, operation):
        logging.debug('BacktestClient: update_balance...')

        BTC = symbol[0:3]
        USD = symbol[3:7]

        balance = self.balance
        BTC_index = next(
            index for (index, balance) in enumerate(balance) if balance["asset"] == BTC)
        USD_index = next(
            index for (index, balance) in enumerate(balance) if balance["asset"] == USD)

        total = self.round_floor(price * qty, 2)
        if operation == 'CREATE':
            if side == 'BUY':
                balance[USD_index]['locked'] += total
                balance[USD_index]['free'] -= total
            elif side == 'SELL':
                balance[BTC_index]['locked'] += qty
                balance[BTC_index]['free'] -= qty
        elif operation == 'CANCEL':
            if side == 'BUY':
                balance[USD_index]['locked'] -= total
                balance[USD_index]['free'] += total
            elif side == 'SELL':
                balance[BTC_index]['locked'] -= qty
                balance[BTC_index]['free'] += qty
        elif operation == 'FILL':
            if side == 'BUY':
                balance[USD_index]['locked'] -= total
                balance[BTC_index]['free'] += qty
            elif side == 'SELL':
                balance[BTC_index]['locked'] -= qty
                balance[USD_index]['free'] += total

    def get_indicators(self):
        logging.debug('BacktestClient: get_indicators')
        total_orders = len(self.orders)
        buy_orders_executed = len([
            order for order in self.orders
            if (
                order['status'] == 'FILLED' and
                order['side'] == 'BUY'
            )
        ])
        sell_orders_executed = len([
            order for order in self.orders
            if (
                order['status'] == 'FILLED' and
                order['side'] == 'SELL'
            )
        ])
        canceled_orders = len([
            order for order in self.orders
            if (
                order['status'] == 'CANCELED'
            )
        ])

        avg_time_win = sum(
            [
                timestamp['duration'] for timestamp in self.timestamps 
                if timestamp['complete'] == 'WIN'
            ], timedelta()
        ) / self.wins if self.wins else 0
        avg_time_loss = sum(
            [
                timestamp['duration'] for timestamp in self.timestamps 
                if timestamp['complete'] == 'LOSS'
            ], timedelta()
        ) / self.losses if self.losses else 0

        timestamps_losses = []
        timestamps_wins = []
        for timestamp in self.timestamps:
            timestamp['start'] = timestamp['start'].strftime("%d/%m/%Y, %H:%M:%S")
            timestamp['end'] = timestamp['end'].strftime("%d/%m/%Y, %H:%M:%S")
            timestamp['duration'] = str(timestamp['duration'])
            if timestamp['complete'] == 'WIN':
                timestamps_wins.append(timestamp)
            elif timestamp['complete'] == 'LOSS':
                timestamps_losses.append(timestamp)

        avg_profit_win = sum(
            [t['profit'] for t in timestamps_wins]
        ) / len(timestamps_wins) if len(timestamps_wins) else 0
        avg_profit_loss = sum(
            [t['profit'] for t in timestamps_losses]
        ) / len(timestamps_losses) if len(timestamps_losses) else 0

        initial_balance = self.decimal_to_float(self.initial_balance)
        final_balance = self.decimal_to_float(self.balance)
        self.last_candle['c'] = str(self.last_candle['c'])

        equivalent_before = self.calculate_equivalent(initial_balance, 1)
        equivalent_after = self.calculate_equivalent(final_balance, float(self.last_candle['c']))
        total_profit = equivalent_after - equivalent_before

        return {
            'initial_balance': initial_balance,
            'final_balance': final_balance,
            'last_candle': self.last_candle,
            'equivalent_before': equivalent_before,
            'equivalent_after': equivalent_after,
            'total_profit': total_profit,
            'wins': self.wins,
            'losses': self.losses,
            'win_loss': self.wins + self.losses,
            'processes': self.processes,
            'timestamps_losses': timestamps_losses,
            'timestamps_wins': timestamps_wins,
            'avg_time_win': str(avg_time_win),
            'avg_time_loss': str(avg_time_loss),
            'avg_profit_win': avg_profit_win,
            'avg_profit_loss': avg_profit_loss,
            'total_orders': total_orders,
            'buy_orders_executed': buy_orders_executed,
            'sell_orders_executed': sell_orders_executed,
            'canceled_orders': canceled_orders
        }

    def win(self, price):
        self.wins += 1
        self.end_process('WIN', price)

    def loss(self, price):
        self.losses += 1
        self.end_process('LOSS', price)

    def end_process(self, complete, price):
        last_timestamp = self.timestamps[-1]
        last_timestamp['end'] = self.convert_timestamp(self.last_candle['T'])
        last_timestamp['duration'] = last_timestamp['end'] - last_timestamp['start']
        last_timestamp['complete'] = complete
        last_timestamp['balance_after'] = self.decimal_to_float(copy.deepcopy(self.balance))
        last_timestamp['final_price'] = str(price)
        last_timestamp['BTC_equivalent_after'] = self.calculate_equivalent(self.balance, float(price))
        last_timestamp['profit'] = last_timestamp['BTC_equivalent_after'] - last_timestamp['BTC_equivalent_before']

    def start_process(self, price):
        self.processes += 1
        self.timestamps.append({
            'start': self.convert_timestamp(self.last_candle['T']),
            'end': self.convert_timestamp('0'),
            'duration': 0,
            'complete': 'INCOMPLETE',
            'balance_before': self.decimal_to_float(copy.deepcopy(self.balance)),
            'initial_price': str(price),
            'BTC_equivalent_before': self.calculate_equivalent(self.balance, float(price))
        })

    def convert_timestamp(self, string):
        timestamp = int(string)
        timestamp /= 1000
        timestamp = datetime.fromtimestamp(timestamp)
        return timestamp

    def is_open(self, order):
        return order['status'] not in (
            'PENDING_CANCEL',
            'REJECTED',
            'EXPIRED',
            'FILLED',
            'CANCELED',
        )

    def round_floor(self, value, prec):
        if prec == 2:
            return decimal.Decimal(value).quantize(
                decimal.Decimal('.01'),
                rounding=decimal.ROUND_DOWN)
        elif prec == 6:
            return decimal.Decimal(value).quantize(
                decimal.Decimal('.000001'),
                rounding=decimal.ROUND_DOWN)

    def decimal_to_float(self, balance):
        for asset in balance:
            for item in asset:
                if type(asset[item]) == type(decimal.Decimal()):
                    asset[item] = float(asset[item])
        return balance

    def calculate_equivalent(self, balance, price):
        equivalent = 0
        BTC_balance = next(b for b in balance if b['asset'] == 'BTC')
        equivalent += float(BTC_balance['free'])
        equivalent += float(BTC_balance['locked'])
        USDT_balance = next(b for b in balance if b['asset'] == 'USDT')
        equivalent += float(USDT_balance['free']) / price
        equivalent += float(USDT_balance['locked']) / price
        return equivalent

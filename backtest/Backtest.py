import logging

from core.Core import Core


class Backtest(Core):

    def processOrders(self, candle):
        logging.debug('Backtest: processOrders...')
        self.client.process_orders(candle)

    def terminate(self):
        logging.info('Backtest: terminate...')

    def initiate(self, ws):
        logging.info('Backtest: initiate...')
        ws.send('ping')

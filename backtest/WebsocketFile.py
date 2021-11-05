import asyncio
import csv
import json
import logging
import os
import signal

import websockets


class WebsocketFile():
    def __init__(self, uri, port, filename, interval, symbol, pid):
        logging.debug('WebsocketFile: init')
        self.uri = uri
        self.port = port
        self.filename = filename
        self.interval = interval
        self.symbol = symbol
        self.pid = pid

        self.read_csv()

    def stream_backtest(self):
        logging.debug('WebsocketFile: stream_backtest')

        async def send_stream(websocket, path):
            await websocket.recv()
            logging.debug(f'WebsocketFile: sending stream {self.filename}...')
            for message in self.stream_list:
                await websocket.send(json.dumps(message))

        stream = websockets.serve(send_stream, self.uri, self.port, ping_interval=None)
        os.kill(int(self.pid), signal.SIGCONT)

        asyncio.get_event_loop().run_until_complete(stream)
        asyncio.get_event_loop().run_forever()
        

    def read_csv(self):
        logging.debug(f'WebsocketFile: reading csv "{self.filename}"')

        self.stream_list = []
        with open(f'./backtest/data/{self.filename}.csv') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                obj = self.build_obj(row)
                self.stream_list.append(obj)

    def build_obj(self, row):
        return {
            'E': '---',                 # event time
            'e': 'kline',               # event type
            'k': {
                'B': row[11],           # can be ignored
                'L': '---',             # last trade id
                'Q': row[10],           # quote volume of active buy
                'T': row[6],            # end time of this bar
                'V': row[9],            # volume of active buy
                'c': row[4],            # close
                'f': '---',             # first trade id
                'h': row[2],            # high
                'i': self.interval,     # interval
                'l': row[3],            # low
                'n': row[8],            # number of trades
                'o': row[1],            # open
                'q': row[7],            # quote volume
                's': self.symbol,       # symbol
                't': row[0],            # start time of this bar
                'v': row[5],            # volume
                'x': True               # whether this bar is final
            },
            's': self.symbol            # symbol
        }

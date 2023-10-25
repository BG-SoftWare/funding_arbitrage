import json
import time
from decimal import Decimal
from json import loads
from threading import Event, Thread, Lock

from libs.exchanges.ws.client import Client


class ByBitPublic(Client):
    def __init__(self, url: str, exchange: str, orderbook: dict, order_reports: dict, ticker: str, order_lock: Lock,
                 reports_lock: Lock, api_key: str, api_sec: str, balance_lock: Lock, balance_list: dict, http_url: str):
        self.__http_url = http_url
        self.orderbook = orderbook
        self.order_lock = order_lock
        self.updates = 0
        self.last_update = orderbook
        self.ticker = ticker
        self.__api_key = api_key
        self.__api_sec = api_sec
        self.order_reports = order_reports
        self.reports_lock = reports_lock
        self.balance_lock = balance_lock
        self.balance_list = balance_list
        self.url = url
        super().__init__(self.url, exchange)

    def handle_execution_report(self, data):
        with self.reports_lock:
            self.order_reports[data["c"]] = data["X"]

    def on_message(self, message):
        data = loads(message)
        if "topic" not in data:
            return

        if data["topic"] == f"tickers.{self.ticker}":
            with self.order_lock:
                self.orderbook["funding_rate"] = Decimal(data["data"]["fundingRate"])*100
        if data["topic"] == f"orderbook.50.{self.ticker}":
            if data["type"] == "snapshot":
                with self.order_lock:
                    self.orderbook["bids"] = [[Decimal(update[0]), Decimal(update[1])] for update
                                              in
                                              data["data"]["b"]]
                    self.orderbook["asks"] = [[Decimal(update[0]), Decimal(update[1])] for update
                                              in
                                              data["data"]["a"]]
                    self.orderbook["timestamp"] = int(time.time() * 1000)
            elif data["type"] == "delta":
                self.process_updates(data["data"])

    def process_updates(self, data):
        with self.order_lock:
            for update in data['b']:
                fix = [Decimal(update[0]), Decimal(update[1])]
                self.manage_orderbook('bids', fix)
            for update in data['a']:
                fix = [Decimal(update[0]), Decimal(update[1])]
                self.manage_orderbook('asks', fix)
            self.last_update['timestamp'] = int(time.time() * 1000)

    def manage_orderbook(self, side, update):
        price, qty = update
        if len(self.orderbook[side]) == 0 and qty != 0:
            self.orderbook[side].append(update)
        for x in range(0, len(self.orderbook[side])):
            if price == self.orderbook[side][x][0]:
                if qty == 0:
                    del self.orderbook[side][x]
                    break
                else:
                    self.orderbook[side][x] = update
                    break
            elif ((price > self.orderbook[side][x][0] and side == 'bids') or
                  (price < self.orderbook[side][x][0] and side == 'asks')):
                if qty != 0:
                    self.orderbook[side].insert(x, update)
                    break
                else:
                    break

    @staticmethod
    def call_repeatedly(interval, func, *args):
        stopped = Event()

        def loop():
            while not stopped.wait(interval):
                func(*args)

        Thread(target=loop).start()
        return stopped.set

    def on_close(self):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()
        super().on_close()

    def on_error(self, error):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()
        super().on_error(error)

    def on_open(self):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()

        self.ws.send(json.dumps(
            {
                "op": "subscribe",
                "args": [
                    f"orderbook.50.{self.ticker}"
                ],
                "req_id": "depthsub"
            }))
        self.ws.send(json.dumps(
            {
                "op": "subscribe",
                "args": [
                    f"tickers.{self.ticker}"
                ],
                "req_id": "tickersub"
            }))

        self.call_repeatedly(20, lambda: self.ws.send('{"req_id": "100001", "op": "ping"}'))
        super().on_open()

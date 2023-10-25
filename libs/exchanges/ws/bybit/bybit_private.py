import hashlib
import hmac
import json
import time
from json import loads
from threading import Event, Thread, Lock

from libs.exchanges.ws.client import Client


class ByBitPrivate(Client):
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

    def handle_execution_report(self, data: dict):
        with self.reports_lock:
            self.order_reports[data["c"]] = data["X"]

    def on_message(self, message):
        data = loads(message)
        open("bybit_uds.txt", "a").write(json.dumps(data) + "\n")
        if "topic" not in data:
            return

        if data["topic"] == "user.execution.contractAccount":
            if len(data) > 0:
                if "execType" in data["data"][0]:
                    if data["data"][0]["execType"] == "Funding":
                        with self.reports_lock:
                            self.order_reports["funding_collected"] = True
                    if data["data"][0]["execType"] == "BustTrade":
                        with self.reports_lock:
                            self.order_reports["liquidated"] = True

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
        ts = int((time.time() * 1000) + 10000)
        sign = str(
            hmac.new(bytes(self.__api_sec, "utf-8"), bytes(f"GET/realtime{ts}", "utf-8"), hashlib.sha256).hexdigest())
        self.ws.send(json.dumps(
            {
                "op": "auth",
                "args":
                    [
                        self.__api_key,
                        ts,
                        sign
                    ]
            }
        ))
        self.ws.send(json.dumps(
            {
                "op": "subscribe",
                "args": [
                    "user.wallet.contractAccount",
                    "user.order.contractAccount",
                    "user.execution.contractAccount",
                    "user.position.contractAccount"
                ],
                "req_id": "udssub"
            }))

        self.call_repeatedly(20, lambda: self.ws.send('{"req_id": "100001", "op": "ping"}'))
        super().on_open()

import json
import logging
import time
from decimal import Decimal
from json import loads
from threading import Event, Thread, Lock

import requests

from libs.exchanges.ws.client import Client


class Binance(Client):
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
        self.__listen_key = self.create_listen_key()
        self.periodically = self.call_repeatedly(30 * 60, self.update_listen_key, (self.__listen_key,))
        self.order_reports = order_reports
        self.reports_lock = reports_lock
        self.balance_lock = balance_lock
        self.balance_list = balance_list
        if self.__listen_key != "":
            self.url = url.replace("LISTENKEY", self.__listen_key)
        else:
            self.url = url
        super().__init__(self.url, exchange)

    def create_listen_key(self) -> str:
        if self.__api_key == "":
            return ""
        key = requests.post(f"https://{self.__http_url}/fapi/v1/listenKey", headers={"X-MBX-APIKEY": self.__api_key})
        return key.json()["listenKey"]

    def update_listen_key(self, key: str) -> None:
        if self.__api_key == "":
            return None
        params = {
            "listenKey": key,
        }
        requests.put(f"https://{self.__http_url}/fapi/v1/listenKey", params=params,
                     headers={"X-MBX-APIKEY": self.__api_key})

    def handle_execution_report(self, data: dict):
        with self.reports_lock:
            self.order_reports[data["c"]] = data["X"]

    def on_message(self, message):
        data = loads(message)
        if data["stream"] == self.__listen_key:
            open("binance_uds.txt", "a").write(json.dumps(data) + "\n")
            logging.debug(f"{self.exchange} {data}")
            with self.reports_lock:
                if "user_data_stream" not in self.order_reports:
                    self.order_reports["user_data_stream"] = [data["data"]]
                else:
                    self.order_reports["user_data_stream"].append(data["data"])
                if "a" not in data["data"]:
                    return
                if data["data"]["a"]["m"] == "FUNDING_FEE":
                    self.order_reports["funding_collected"] = True
                if data["e"] == "MARGIN_CALL":
                    self.order_reports["liquidated"] = True

        if data["data"]["e"] == "markPriceUpdate":
            with self.order_lock:
                self.orderbook["funding_rate"] = Decimal(data["data"]["r"]) * 100

        if data["data"]["e"] == "depthUpdate":
            data = data["data"]
            if 'lastUpdateId' not in self.orderbook:
                for key, value in self.get_snapshot().items():
                    self.orderbook[key] = value
            last_update_id = self.orderbook['lastUpdateId']

            if self.updates == 0:
                if data['U'] <= last_update_id <= data['u']:
                    self.updates = 1
                    self.orderbook['lastUpdateId'] = data['u']
                    self.process_updates(data)

            elif data['pu'] == last_update_id:
                self.orderbook['lastUpdateId'] = data['u']
                self.process_updates(data)
            else:
                self.updates = 0

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

        for x in range(0, len(self.orderbook[side])):
            if price == self.orderbook[side][x][0]:
                if qty == 0:
                    del self.orderbook[side][x]
                    break
                self.orderbook[side][x] = update
                break
            elif ((price > self.orderbook[side][x][0] and side == 'bids') or
                  (price < self.orderbook[side][x][0] and side == 'asks')):
                if qty != 0:
                    self.orderbook[side].insert(x, update)
                    break
                break

    def get_snapshot(self) -> dict:
        r = requests.get(f'https://{self.__http_url}/fapi/v1/depth?symbol=' + self.ticker + '&limit=1000')
        data = loads(r.content.decode())
        data["bids"] = [[Decimal(x[0]), Decimal(x[1])] for x in data["bids"]]
        data["asks"] = [[Decimal(x[0]), Decimal(x[1])] for x in data["asks"]]
        data["lastUpdateId"] = data["lastUpdateId"]
        data["timestamp"] = int(time.time() * 1000)
        return data

    @staticmethod
    def call_repeatedly(interval, func, *args):
        stopped = Event()

        def loop():
            while not stopped.wait(interval):
                logging.debug("exec repeat")
                func(*args)

        Thread(target=loop).start()
        return stopped.set

    def on_close(self):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()
        try:
            self.periodically()
        except:
            pass
        super().on_close()

    def on_error(self, error):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()
        try:
            self.periodically()
        except:
            pass
        super().on_error(error)

    def on_open(self):
        with self.order_lock:
            self.orderbook.clear()
        with self.reports_lock:
            self.order_reports.clear()
        self.__listen_key = self.create_listen_key()
        self.periodically = self.call_repeatedly(20 * 60, self.update_listen_key, (self.__listen_key,))
        super().on_open()

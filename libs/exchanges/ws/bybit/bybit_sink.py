from libs.exchanges.ws.bybit.bybit_private import ByBitPrivate
from libs.exchanges.ws.bybit.bybit_public import ByBitPublic
from threading import Lock


class ByBit:
    daemon = True

    def __init__(self, url: str, exchange: str, orderbook: dict, order_reports: dict, ticker: str, order_lock: Lock,
                 reports_lock: Lock, api_key: str, api_sec: str, balance_lock: Lock, balance_list: dict, http_url: str):
        self.bybit_depth = ByBitPublic(url + "/contract/usdt/public/v3", exchange, orderbook, order_reports, ticker,
                                       order_lock, reports_lock, api_key, api_sec, balance_lock, balance_list, http_url)
        self.bybit_depth.daemon = self.daemon
        self.bybit_private_and_funding_rate = ByBitPrivate(url + "/contract/private/v3", exchange + "Private",
                                                           orderbook, order_reports, ticker, order_lock, reports_lock,
                                                           api_key, api_sec, balance_lock, balance_list, http_url)
        self.bybit_private_and_funding_rate.daemon = self.daemon

    def start(self):
        self.bybit_depth.daemon = self.daemon
        self.bybit_private_and_funding_rate.daemon = self.daemon
        self.bybit_depth.start()
        self.bybit_private_and_funding_rate.start()

import datetime
from decimal import Decimal
from urllib.parse import urlencode

import requests


class ByBit:
    exchange_name = "ByBit"
    maker_fee = "0.01"
    taker_fee = "0.06"
    RETRY_COUNT = 3

    def __init__(self, tickers_list: list = None):
        self.tickers_list = tickers_list
        if tickers_list is None:
            self.tickers_list = self.get_tickers()

    @staticmethod
    def get_multiplier(symbol: str) -> Decimal:
        instrument_info = requests.get(f"https://api.bybit.com/derivatives/v3/public/instruments-info"
                                       f"?symbol={symbol}"
                                       f"&category=linear").json()
        return Decimal(instrument_info['result']['list'][0]['lotSizeFilter']["qtyStep"])

    def get_futures_depth(self, ticker: str = None, limit: int = 10) -> dict:
        if ticker is not None:
            req = requests.get(f"https://api.bybit.com/derivatives/v3/public/order-book/L2"
                               f"?category=linear&symbol={ticker}"
                               f"&limit={limit}")
            if req.status_code != 200:
                raise ConnectionError
            req_json = req.json()
            result = {"bids": req_json["result"]["b"], "asks": req_json["result"]["a"]}
        else:
            result = {}
            for ticker in self.tickers_list:
                req = requests.get(f"https://api.bybit.com/derivatives/v3/public/order-book/L2"
                                   f"?category=linear&symbol={ticker}"
                                   f"&limit={limit}")
                if req.status_code != 200:
                    raise ConnectionError
                req_json = req.json()
                result[ticker] = {"bids": [[Decimal(elem[0]), Decimal(elem[1])] for elem in req_json["result"]["b"]],
                                  "asks": [[Decimal(elem[0]), Decimal(elem[1])] for elem in req_json["result"]["a"]]}
        return result

    def get_funding_rate(self, quote_asset: str = None) -> dict:
        req = requests.get(f"https://api.bybit.com/derivatives/v3/public/tickers?category=linear")
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        result = {}
        for pair in req_json["result"]["list"]:
            if self.tickers_list is not None:
                if pair["symbol"] not in self.tickers_list:
                    continue
                if quote_asset is not None:
                    if not pair["symbol"].endswith(quote_asset):
                        continue
            result[pair["symbol"]] = {"funding_rate": Decimal(pair["fundingRate"]) * 100,
                                      "original_symbol": pair["symbol"]}
        return result

    def get_max_leverage_for_usdt_amount(self, symbol: str) -> tuple[Decimal, Decimal]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": symbol
                }

                response = requests.get(
                    f"https://api.bybit.com/derivatives/v3/public/instruments-info?" + urlencode(params))
                break
            except BaseException:
                counter += 1

        if response is None:
            raise ConnectionError("Connection error to bybit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            return Decimal(response["result"]["list"][0]["leverageFilter"]["maxLeverage"]), \
                Decimal(response["result"]["list"][0]["leverageFilter"]["leverageStep"])

    @staticmethod
    def get_tickers(contract_type: str = "linearPerpetual") -> list[str]:
        req = requests.get(f"https://api.bybit.com/derivatives/v3/public/instruments-info?contractType={contract_type}")
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        tickers_list = []
        for ticker in req_json["result"]["list"]:
            tickers_list.append(ticker["symbol"])
        return tickers_list

    @staticmethod
    def get_kline_open_price(symbol: str, dtime: datetime.datetime, interval: str = "30") -> str:
        req = requests.get(f"https://api.bybit.com/derivatives/v3/public/kline"
                           f"?symbol={symbol}"
                           f"&start={int(dtime.timestamp() * 1000)}"
                           f"&end={int(dtime.timestamp() * 1000) + 999}"
                           f"&limit=1&interval={interval}")
        kline = req.json()
        return kline["result"]["list"][0][1]

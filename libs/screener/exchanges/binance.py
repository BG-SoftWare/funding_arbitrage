import hashlib
import hmac
import time
from decimal import Decimal
from urllib.parse import urlencode

import requests


class Binance:
    exchange_name = "Binance"
    maker_fee = "0.02"
    taker_fee = "0.04"
    blacklist = [
        "HNTUSDT"
    ]

    RETRY_COUNT = 3

    def __init__(self, tickers_list=None, auth_data=None):
        self.tickers_list = tickers_list
        if tickers_list is None:
            self.tickers_list = self.get_tickers()

        self.auth_data = auth_data

    @staticmethod
    def get_multiplier(symbol_to_find: str) -> Decimal:
        exchange_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
        for symbol in exchange_info['symbols']:
            if symbol['symbol'] == symbol_to_find:
                for exchange_filter in symbol["filters"]:
                    if exchange_filter["filterType"] == "LOT_SIZE":
                        return Decimal(exchange_filter["stepSize"])

    def get_futures_depth(self, ticker: str = None, limit: int = 10) -> dict:
        if ticker is not None:
            req = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={ticker}&limit={limit}")
            if req.status_code != 200:
                raise ConnectionError
            req_json = req.json()
            result = {"bids": req_json["bids"], "asks": req_json["asks"]}
        else:
            result = {}
            for ticker in self.tickers_list:
                req = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={ticker}&limit={limit}")
                if req.status_code != 200:
                    raise ConnectionError
                req_json = req.json()
                result[ticker] = {"bids": [[Decimal(elem[0]), Decimal(elem[1])] for elem in req_json["bids"]],
                                  "asks": [[Decimal(elem[0]), Decimal(elem[1])] for elem in req_json["asks"]]}
        return result

    def get_funding_rate(self, quote_asset: str = None) -> dict:
        req = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex")
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        result = {}
        for pair in req_json:
            if self.tickers_list is not None:
                if pair["symbol"] not in self.tickers_list:
                    continue
            if quote_asset is not None:
                if not pair["symbol"].endswith(quote_asset):
                    continue
                if pair["symbol"] in self.blacklist:
                    continue
            result[pair["symbol"]] = {"funding_rate": Decimal(pair["lastFundingRate"]) * 100,
                                      "original_symbol": pair["symbol"]}
        return result

    @staticmethod
    def get_tickers(contract_type: str = "PERPETUAL") -> list[str]:
        req = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        tickers_list = []
        for ticker in req_json["symbols"]:
            if ticker["contractType"] == contract_type and ticker["status"] == "TRADING":
                tickers_list.append(ticker["symbol"])

        return tickers_list

    def get_max_leverage_for_usdt_amount(self, symbol: str, usdt_amount: Decimal) -> tuple[Decimal, Decimal]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": 59999
                }

                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.auth_data["api_sec"], "UTF-8"),
                                               bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://fapi.binance.com/fapi/v1/leverageBracket?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.auth_data["api_key"]})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            for bracket in response[0]["brackets"]:
                if usdt_amount * bracket["initialLeverage"] < bracket["notionalCap"]:
                    return Decimal(bracket["initialLeverage"]), Decimal("1")

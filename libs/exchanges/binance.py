import copy
import datetime
import hashlib
import hmac
import logging
import time
import uuid
from decimal import Decimal
from urllib.parse import urlencode

import requests

from libs.exchanges.ws.binance import Binance as BinanceWS
from libs.objects.Income import Income
from libs.objects.Order import Order
from libs.objects.OrderInfo import OrderInfo
from libs.objects.Position import Position
from libs.objects.Trade import Trade


class Binance:
    __required_args = [
        "api_key",
        "api_sec",
        "symbol",
        "recv_window",
        "base_url",
        "websockets_base_url"
    ]

    funding_times = [0, 28800, 57600]

    __ws_controller = BinanceWS
    GOOD_UNTIL_CANCEL = "GTC"
    IMMEDIATE_OR_CANCEL = "IOK"
    FILL_OR_KILL = "FOK"
    GOOD_TILL_CROSSING = "GTX"

    LIMIT_ORDER = "LIMIT"
    STOP_ORDER = "STOP"
    TAKE_PROFIT_ORDER = "TAKE_PROFIT"
    STOP_MARKET_ORDER = "STOP_MARKET"
    TAKE_PROFIT_MARKET_ORDER = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_ORDER = "TRAILING_STOP_MARKET"
    RETRY_COUNT = 3

    ISOLATED_MARGIN = "ISOLATED"
    CROSSED_MARGIN = "CROSSED"

    LONG = "long"
    SHORT = "short"

    BUY = "BUY"
    SELL = "SELL"

    MARKET_ORDER = "MARKET"

    def __init__(self, args):
        self.symbol = args["symbol"]
        self.__api_key = args["api_key"]
        self.__api_sec = args["api_sec"]
        self.__recv_window = args["recv_window"]
        self.__base_url = args["base_url"]
        self.__ws_url = args["websockets_base_url"]
        self.__ws_url = "wss://{1}/stream?streams=LISTENKEY/{0}@depth@100ms/{0}@markPrice@1s".format(
            self.symbol.lower(), args["websockets_base_url"])

    def get_websockets_handler(self, orderbook, order_reports, order_lock, reports_lock, balance_list, balance_lock):
        return self.__ws_controller(url=self.__ws_url, exchange="Binance", orderbook=orderbook,
                                    order_reports=order_reports,
                                    reports_lock=reports_lock,
                                    ticker=self.symbol, order_lock=order_lock, api_key=self.__api_key,
                                    api_sec=self.__api_sec, balance_list=balance_list, balance_lock=balance_lock,
                                    http_url=self.__base_url)

    def get_multiplier(self) -> Decimal:
        exchange_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
        for symbol in exchange_info['symbols']:
            if symbol['symbol'] == self.symbol:
                for exchange_filter in symbol["filters"]:
                    if exchange_filter["filterType"] == "LOT_SIZE":
                        return Decimal(exchange_filter["stepSize"])

    def get_balances(self) -> dict:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window,
                }
                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v2/balance?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            print(response.text)
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            balances = {}
            for balance in response:
                balances[balance["asset"]] = {
                    "balance": balance["balance"],
                    "available": balance["availableBalance"],
                }
            return balances

    def closest_time_before_funding(self, secs: int) -> bool | None:
        now = datetime.datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = (now - midnight).seconds
        for funding_time in self.funding_times:
            if secs < funding_time - seconds < secs + 60:
                return True

    def place_order(self, route: str, price: Decimal, amount: Decimal, order_type: str = LIMIT_ORDER,
                    time_in_force: str = GOOD_UNTIL_CANCEL, stop_price: Decimal = None,
                    close_position: bool = None, reduce_only: bool = False) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "side": route,
                    "type": order_type,
                    "quantity": float(Decimal(amount)),
                    "price": float(("%.17f" % Decimal(price)).rstrip('0').rstrip('.')),
                    "newClientOrderId": str(uuid.uuid4()),
                    "reduceOnly": reduce_only,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window,
                    "timeInForce": time_in_force
                }
                if stop_price is not None:
                    params["stopPrice"] = Decimal(stop_price)
                if close_position is not None:
                    params["closePosition"] = Decimal(close_position)

                if order_type == self.MARKET_ORDER:
                    del params["price"]
                    del params["timeInForce"]

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.post(f"https://{self.__base_url}/fapi/v1/order", data=params,
                                         headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            response = response.json()
            if response["code"] == -5021:
                return Order(order_id="", client_order_id="", symbol=self.symbol, price=price, status="REJECTED")
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return self.get_order_status(Order(order_id=response["orderId"], client_order_id=response["clientOrderId"],
                                               symbol=self.symbol, price=price, status=response["status"]))

    def get_order_status(self, order: Order) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window,
                }

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v1/openOrder?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            if response.json()["msg"] == "Order does not exist.":
                order_copy = copy.deepcopy(order)
                order_info = None
                counter_2 = 0
                while counter_2 < self.RETRY_COUNT:
                    try:
                        params = {
                            "symbol": self.symbol,
                            "orderId": order.order_id,
                            "timestamp": int(time.time() * 1000),
                            "recvWindow": self.__recv_window,
                        }
                        logging.debug(str(params))
                        string_for_sign = urlencode(params)
                        params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                                       hashlib.sha256).hexdigest()
                        order_info = requests.get(f"https://{self.__base_url}/fapi/v1/order?" + urlencode(params),
                                                  headers={"X-MBX-APIKEY": self.__api_key})
                        break
                    except BaseException:
                        counter_2 += 1
                if order_info is None:
                    raise ConnectionError("Connection error to binance")
                if order_info.status_code != 200:
                    raise ConnectionError("\n".join([response.text, order_info.text]))

                order_copy.status = order_info.json()["status"]
                return order_copy

            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return Order(order_id=response["orderId"], client_order_id=response["clientOrderId"], symbol=self.symbol,
                         price=response["price"], status=response["status"])

    def get_order_info(self, order: Order) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }
                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v1/userTrades?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            counter_2 = 0
            order_info = None
            while counter_2 < self.RETRY_COUNT:
                try:
                    params = {
                        "symbol": self.symbol,
                        "orderId": order.order_id,
                        "timestamp": int(time.time() * 1000),
                        "recvWindow": self.__recv_window,
                    }

                    logging.debug(str(params))
                    string_for_sign = urlencode(params)
                    params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                                   hashlib.sha256).hexdigest()
                    order_info = requests.get(f"https://{self.__base_url}/fapi/v1/order?" + urlencode(params),
                                              headers={"X-MBX-APIKEY": self.__api_key})
                    break
                except BaseException:
                    counter_2 += 1
            if order_info is None:
                raise ConnectionError("Connection error to binance")
            if order_info.status_code != 200:
                raise ConnectionError("\n".join([response.text, order_info.text]))

            order_time = order_info.json()["time"]
            response = response.json()
            logging.debug(response.text)
            fee = 0
            usdt_amount = 0
            qty = 0
            position_side = ""
            route = ""

            for fill in response:
                usdt_amount += Decimal(fill["quoteQty"])
                fee += Decimal(fill["commission"])
                position_side = fill["positionSide"]
                route = fill["side"]
                qty += Decimal(fill["qty"])

            return OrderInfo(order_id=order.order_id, client_order_id=order.client_order_id,
                             symbol=order.symbol,
                             price=order.price, status=order.status, fee=fee, position_side=position_side,
                             route=route, avg_order_price=usdt_amount / qty, quote_qty=usdt_amount, qty=qty,
                             order_time=datetime.datetime.fromtimestamp(int(order_time) / 1000))

    def get_trades(self, start_timestamp: str | int, end_timestamp: str | int) -> list[Trade]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "startTime": int(start_timestamp),
                    "endTime": int(end_timestamp),
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v1/userTrades?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            trades = []
            for trade in response:
                trades.append(Trade(
                    symbol=trade["symbol"], trade_id=trade["id"], order_id=trade["orderId"], side=trade["side"],
                    price=trade["price"], qty=trade["qty"], realized_pnl=trade["realizedPnl"],
                    margin_asset=trade["marginAsset"], quote_qty=trade["quoteQty"], commission=trade["commission"],
                    commission_asset=trade["commissionAsset"], datetime=trade["time"],
                    position_side=trade["positionSide"], maker=trade["maker"], buyer=trade["buyer"]
                ))
            return trades

    def get_positions(self) -> list[Position]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v2/positionRisk?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            positions = []
            for position in response:
                positions.append(Position(entry_price=position["entryPrice"],
                                          position_value=position["positionAmt"],
                                          cum_pnl=position["unRealizedProfit"],
                                          mark_price=position["markPrice"],
                                          liquidation_price=position["liquidationPrice"],
                                          leverage=position["leverage"],
                                          margin_type=position["marginType"]
                                          ))
            return positions

    def get_income_history(self, start_time: str | int = None, end_time: str | int = None) -> list[Income]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }
                if start_time is not None and end_time is not None:
                    params["startTime"] = start_time
                    params["endTime"] = end_time

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v1/income?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            logging.debug(response.text)
            response = response.json()
            incoming = []
            for income in response:
                incoming.append(
                    Income(**income)
                )
            return incoming

    def get_max_leverage_for_usdt_amount(self, usdt_amount: Decimal) -> tuple[Decimal, Decimal]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }
                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.get(f"https://{self.__base_url}/fapi/v1/leverageBracket?" + urlencode(params),
                                        headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            for bracket in response[0]["brackets"]:
                if usdt_amount * bracket["initialLeverage"] < bracket["notionalCap"]:
                    return Decimal(bracket["initialLeverage"]), Decimal("1")

    def cancel_order(self, order: Order) -> bool:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.delete(f"https://{self.__base_url}/fapi/v1/order", data=params,
                                           headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return True

    def get_funding_rate(self) -> Decimal:
        req = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + self.symbol)
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        return Decimal(req_json["lastFundingRate"]) * 100

    def get_income_funding_fee(self, start_time: str | int, end_time: str | int) -> float:
        incomes = self.get_income_history(start_time, end_time)
        funding = 0
        for income in incomes:
            if income.income_type == "FUNDING_FEE":
                funding += income.income
        return funding

    def __set_leverage(self, leverage: int | str) -> bool:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "leverage": leverage,
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }
                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.post(f"https://{self.__base_url}/fapi/v1/leverage", data=params,
                                         headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return True

    def funding_timeout(self, secs: int) -> bool | None:
        now = datetime.datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = (now - midnight).seconds
        for funding_time in self.funding_times:
            if secs < seconds - funding_time < secs + 60:
                return True

    def set_margin_type_and_leverage(self, margin_type: str, leverage: int | str):
        self.__set_leverage(leverage)
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "marginType": margin_type,
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = urlencode(params)
                params['signature'] = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                               hashlib.sha256).hexdigest()
                response = requests.post(f"https://{self.__base_url}/fapi/v1/marginType", data=params,
                                         headers={"X-MBX-APIKEY": self.__api_key})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to binance")

        if response.status_code != 200:
            err = response.json()["msg"]
            if err == "No need to change margin type.":
                return True
            raise ConnectionError(response.text)

        else:
            response = response.json()
            logging.debug(response.text)
            return True

    def get_unified_symbol_name(self) -> str:
        return self.symbol

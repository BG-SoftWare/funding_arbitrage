import datetime
import hashlib
import hmac
import logging
import time
from decimal import Decimal
from threading import Lock
from urllib.parse import urlencode

import requests

from libs.exchanges.ws.bybit.bybit_sink import ByBit as ByBitWS
from libs.objects.Income import Income
from libs.objects.Order import Order
from libs.objects.OrderInfo import OrderInfo
from libs.objects.Position import Position
from libs.objects.Trade import Trade


class ByBit:
    __required_args = [
        "api_key",
        "api_sec",
        "symbol",
        "recv_window",
        "base_url",
        "websockets_base_url"
    ]
    __ws_controller = ByBitWS
    funding_times = [0, 28800, 57600]
    GOOD_UNTIL_CANCEL = "GoodTillCancel"
    IMMEDIATE_OR_CANCEL = "ImmediateOrCancel"
    FILL_OR_KILL = "FillOrKill"

    BUY = "Buy"
    SELL = "Sell"

    LIMIT_ORDER = "Limit"
    MARKET_ORDER = "Market"
    STOP_ORDER = "STOP"
    TAKE_PROFIT_ORDER = "TAKE_PROFIT"
    STOP_MARKET_ORDER = "STOP_MARKET"
    TAKE_PROFIT_MARKET_ORDER = "TAKE_PROFIT_MARKET"

    RETRY_COUNT = 3

    LONG = "long"
    SHORT = "short"

    def __init__(self, kwargs: dict):
        self.__recv_window = kwargs["recv_window"]
        self.symbol = kwargs["symbol"]
        self.__api_key = kwargs["api_key"]
        self.__api_sec = kwargs["api_sec"]
        self.__base_url = kwargs["base_url"]
        self.__ws_url = kwargs["websockets_base_url"]

    def get_websockets_handler(self, order_book: dict, order_reports: dict, order_lock: Lock,
                               reports_lock: Lock, balance_list: dict, balance_lock: Lock):

        return self.__ws_controller(url=self.__ws_url, exchange="ByBit", orderbook=order_book,
                                    order_reports=order_reports, reports_lock=reports_lock,
                                    ticker=self.symbol, order_lock=order_lock, api_key=self.__api_key,
                                    api_sec=self.__api_sec, balance_list=balance_list, balance_lock=balance_lock,
                                    http_url=self.__base_url)

    def get_multiplier(self) -> Decimal:
        instrument_info = requests.get(
            f"https://api.bybit.com/derivatives/v3/public/instruments-info?symbol={self.symbol}&category=linear").json()
        return Decimal(instrument_info['result']['list'][0]['lotSizeFilter']["qtyStep"])

    def get_balances(self) -> dict:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = str(int(time.time() * 1000))
                params = {

                }
                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = str(hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                    hashlib.sha256).hexdigest())
                response = requests.get(f"{self.__base_url}/contract/v3/private/account/wallet/balance",
                                        headers={"X-BAPI-API-KEY": self.__api_key,
                                                 "X-BAPI-TIMESTAMP": str(int(time.time() * 1000)),
                                                 "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                 "X-BAPI-SIGN": sign
                                                 })
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            balances = {}
            for balance in response["result"]["list"]:
                balances[balance["coin"]] = {
                    "balance": balance["walletBalance"],
                    "available": balance["availableBalance"],
                }
            return balances

    def place_order(self, route: str, amount: Decimal, order_type: str = LIMIT_ORDER, price: Decimal = None,
                    time_in_force: str = GOOD_UNTIL_CANCEL, stop_price: Decimal = None, reduce_only: bool = False,
                    take_profit_price: Decimal = None) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            ts = str(int(time.time() * 1000))
            try:
                params = {
                    "symbol": self.symbol,
                    "side": route,
                    "orderType": order_type,
                    "qty": str(float(Decimal(amount))),
                    "timeInForce": time_in_force
                }
                if price is not None:
                    params["price"] = ("%.17f" % Decimal(price)).rstrip('0').rstrip('.')
                if stop_price is not None:
                    params["stopPrice"] = ("%.17f" % Decimal(stop_price)).rstrip('0').rstrip('.')
                if take_profit_price is not None:
                    params["takeProfit"] = ("%.17f" % Decimal(take_profit_price)).rstrip('0').rstrip('.')
                if reduce_only:
                    params["reduceOnly"] = True

                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.post(f"{self.__base_url}/contract/v3/private/order/create", data=params,
                                         headers={"X-BAPI-API-KEY": self.__api_key,
                                                  "X-BAPI-TIMESTAMP": str(ts),
                                                  "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                  "X-BAPI-SIGN": sign})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            if response["retCode"] != 0:
                raise ConnectionError(response.text)
            return self.get_order_status(Order(order_id=response["result"]["orderId"],
                                               client_order_id=response["result"]["orderId"], symbol=self.symbol,
                                               price=price, status="NEW"))

    def get_order_status(self, order: Order) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = int(time.time() * 1000)
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                }

                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.get(f"{self.__base_url}/contract/v3/private/order/list?" + urlencode(params),
                                        headers={"X-BAPI-API-KEY": self.__api_key,
                                                 "X-BAPI-TIMESTAMP": str(ts),
                                                 "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                 "X-BAPI-SIGN": sign})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return Order(order_id=response["result"]["list"][0]["orderId"],
                         client_order_id=response["result"]["list"][0]["orderId"], symbol=self.symbol,
                         price=Decimal(response["result"]["list"][0]["price"]),
                         status=response["result"]["list"][0]["orderStatus"].upper())

    def get_order_info(self, order: Order) -> Order:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = int(time.time() * 1000)
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                }

                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.get(f"{self.__base_url}/contract/v3/private/order/list?" + urlencode(params),
                                        headers={"X-BAPI-API-KEY": self.__api_key,
                                                 "X-BAPI-TIMESTAMP": str(ts),
                                                 "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                 "X-BAPI-SIGN": sign})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            if response["result"]["list"][0]["orderStatus"].upper() not in ["REJECTED", "CANCELLED"]:
                avg_order_price = (Decimal(response["result"]["list"][0]["cumExecValue"]) /
                                   Decimal(response["result"]["list"][0]["cumExecQty"]))
                fee = Decimal(response["result"]["list"][0]["cumExecFee"])
                quote_qty = Decimal(response["result"]["list"][0]["cumExecValue"])
                qty = Decimal(response["result"]["list"][0]["cumExecQty"])
            else:
                avg_order_price = Decimal("0")
                fee = Decimal("0")
                quote_qty = Decimal("0")
                qty = Decimal("0")
            order_time = datetime.datetime.fromtimestamp(int(response["result"]["list"][0]["createdTime"]) / 1000)
            order_info = OrderInfo(order_id=response["result"]["list"][0]["orderId"],
                                   client_order_id=response["result"]["list"][0]["orderId"],
                                   symbol=self.symbol,
                                   price=Decimal(response["result"]["list"][0]["price"]),
                                   status=response["result"]["list"][0]["orderStatus"].upper(),
                                   avg_order_price=avg_order_price,
                                   fee=fee,
                                   position_side="",
                                   quote_qty=quote_qty,
                                   qty=qty,
                                   route=response["result"]["list"][0]["side"],
                                   order_time=order_time
                                   )
            return order_info

    def get_trades(self, start_timestamp: int, end_timestamp: int) -> list[Trade]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            ts = str(int(time.time() * 1000))
            try:
                params = {
                    "symbol": self.symbol,
                    "startTime": int(start_timestamp),
                    "endTime": int(end_timestamp),
                    "limit": 200,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }
                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.get(
                    f"{self.__base_url}/contract/v3/private/position/closed-pnl" + urlencode(params),
                    headers={"X-BAPI-API-KEY": self.__api_key,
                             "X-BAPI-TIMESTAMP": str(int(time.time() * 1000)),
                             "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                             "X-BAPI-SIGN": sign})
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            trades = []
            for trade in response["result"]["list"]:
                trades.append(Trade(
                    symbol=trade["symbol"], trade_id=trade["orderId"], order_id=trade["orderId"],
                    side=trade["side"].upper(),
                    price=Decimal(trade["orderPrice"]), qty=Decimal(trade["qty"]),
                    realized_pnl=Decimal(trade["closedPnl"]),
                    margin_asset="USDT", quote_qty=Decimal(trade["qty"]) * Decimal(trade["orderPrice"]),
                    commission=Decimal(trade["cumExecFee"]),
                    commission_asset="", datetime=trade["createdAt"], position_side=None,
                    maker=None, buyer=None
                ))
            return trades

    def get_positions(self) -> list[Position]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = str(int(time.time() * 1000))
                params = {
                    "symbol": self.symbol
                }
                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = str(hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                    hashlib.sha256).hexdigest())
                response = requests.get(f"{self.__base_url}/contract/v3/private/position/list?" + urlencode(params),
                                        headers={"X-BAPI-API-KEY": self.__api_key,
                                                 "X-BAPI-TIMESTAMP": str(int(time.time() * 1000)),
                                                 "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                 "X-BAPI-SIGN": sign
                                                 })
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            positions = []
            for position in response["result"]["list"]:
                positions.append(Position(entry_price=Decimal(position["entryPrice"]),
                                          position_value=Decimal(position["positionValue"]),
                                          cum_pnl=Decimal(position["cumRealisedPnl"]),
                                          mark_price=Decimal(position["markPrice"]),
                                          liquidation_price=Decimal(position["liqPrice"]),
                                          leverage=Decimal(position["leverage"]),
                                          margin_type="cross" if position["tradeMode"] == 0 else "isolated"
                                          ))
            return positions

    def get_income_history(self, start_time: int, end_time: int) -> list[Income]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = str(int(time.time() * 1000))
                params = {
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 100,
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.get(
                    f"{self.__base_url}/contract/v3/private/position/closed-pnl?" + urlencode(params),
                    headers={"X-BAPI-API-KEY": self.__api_key,
                             "X-BAPI-TIMESTAMP": str(ts),
                             "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                             "X-BAPI-SIGN": sign
                             })
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
            incoming = []
            for income in response["result"]["list"]:
                incoming.append(
                    Income(
                        symbol=income["symbol"],
                        income_type="PNL",
                        income=Decimal(income["closedPnl"]),
                        asset="USDT",
                        datetime=Decimal(income["createdAt"]),
                        info=income["orderId"],
                        tran_id=income["orderId"],
                        trade_id=income["orderId"]
                    )
                )
            return incoming

    def cancel_order(self, order: Order) -> bool:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            ts = int(time.time() * 1000)
            try:
                params = {
                    "symbol": self.symbol,
                    "orderId": order.order_id,
                }

                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.post(f"{self.__base_url}/contract/v3/private/order/cancel", data=params,
                                         headers={
                                             "X-BAPI-API-KEY": self.__api_key,
                                             "X-BAPI-TIMESTAMP": str(ts),
                                             "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                             "X-BAPI-SIGN": sign
                                         })
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return True

    def get_funding_rate(self) -> Decimal:
        req = requests.get(f"https://api.bybit.com/derivatives/v3/public/tickers?category=linear&symbol=" + self.symbol)
        if req.status_code != 200:
            raise ConnectionError
        req_json = req.json()
        return Decimal(req_json["result"]["list"][0]["fundingRate"]) * 100

    def __set_leverage(self, leverage: int):
        counter = 0
        while counter < self.RETRY_COUNT:
            ts = int(time.time() * 1000)
            try:
                params = {
                    "symbol": self.symbol,
                    "buyLeverage": str(leverage),
                    "sellLeverage": str(leverage)
                }
                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                requests.post(f"{self.__base_url}/contract/v3/private/position/set-leverage", data=params,
                              headers={
                                  "X-BAPI-API-KEY": self.__api_key,
                                  "X-BAPI-TIMESTAMP": str(ts),
                                  "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                  "X-BAPI-SIGN": sign
                              })
                break
            except BaseException:
                counter += 1

    def set_margin_type_and_leverage(self, margin_type: str, leverage: int) -> bool:
        self.__set_leverage(leverage)
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            ts = int(time.time() * 1000)
            try:
                params = {
                    "symbol": self.symbol,
                    "tradeMode": margin_type,
                    "buyLeverage": str(leverage),
                    "sellLeverage": str(leverage)
                }
                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.post(f"{self.__base_url}/contract/v3/private/position/switch-isolated", data=params,
                                         headers={
                                             "X-BAPI-API-KEY": self.__api_key,
                                             "X-BAPI-TIMESTAMP": str(ts),
                                             "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                             "X-BAPI-SIGN": sign
                                         })
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to ByBit")

        if response.status_code != 200:
            err = response.text
            if err == "No need to change margin type":
                return True
            raise ConnectionError(response.text)

        else:
            return True

    def get_income_funding_fee(self, start_time: int, end_time: int) -> Decimal:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                ts = str(int(time.time() * 1000))
                params = {
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": 100,
                    "symbol": self.symbol,
                    "timestamp": int(time.time() * 1000),
                    "recvWindow": self.__recv_window
                }

                logging.debug(str(params))
                string_for_sign = str(ts) + self.__api_key + str(self.__recv_window) + urlencode(params)
                sign = hmac.new(bytes(self.__api_sec, "UTF-8"), bytes(string_for_sign, "UTF-8"),
                                hashlib.sha256).hexdigest()
                response = requests.get(f"{self.__base_url}/contract/v3/private/execution/list?" + urlencode(params),
                                        headers={"X-BAPI-API-KEY": self.__api_key,
                                                 "X-BAPI-TIMESTAMP": str(ts),
                                                 "X-BAPI-RECV-WINDOW": str(self.__recv_window),
                                                 "X-BAPI-SIGN": sign
                                                 })
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to bybit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            funding = Decimal("0")
            for income in response["result"]["list"]:
                if income["execType"] == "Funding":
                    funding += Decimal(income["execFee"])
        return funding

    def get_max_leverage_for_usdt_amount(self) -> tuple[Decimal, Decimal]:
        counter = 0
        response = None
        while counter < self.RETRY_COUNT:
            try:
                params = {
                    "symbol": self.symbol
                }
                logging.debug(str(params))
                response = requests.get(f"{self.__base_url}/derivatives/v3/public/instruments-info?" + urlencode(params))
                break
            except BaseException:
                counter += 1
        if response is None:
            raise ConnectionError("Connection error to bybit")

        if response.status_code != 200:
            raise ConnectionError(response.text)
        else:
            response = response.json()
            logging.debug(response.text)
            return Decimal(response["result"]["list"][0]["leverageFilter"]["maxLeverage"]), \
                Decimal(response["result"]["list"][0]["leverageFilter"]["leverageStep"])

    def closest_time_before_funding(self, secs: int) -> bool | None:
        now = datetime.datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = (now - midnight).seconds
        for funding_time in self.funding_times:
            if secs < funding_time - seconds < secs + 60:
                return True

    def get_unified_symbol_name(self) -> str:
        return self.symbol

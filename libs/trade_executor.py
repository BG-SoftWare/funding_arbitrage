import datetime
import logging
import time
from decimal import Decimal

from libs.database_connector import DatabaseConnector
from libs.objects.OrderBook import OrderBook
from libs.objects.OrderInfo import OrderInfo
from libs.thread_with_return_value import ThreadWithReturnValue


class TradeLogic:
    TRADE_INSTRUCTION = {
        "long": [{"route": "BUY", "isAsk": True}, {"route": "SELL", "isAsk": False}],
        "short": [{"route": "SELL", "isAsk": False}, {"route": "BUY", "isAsk": True}]
    }

    def __init__(self, exchange_1, exchange_2, token_amount, leverage, funding_timeout, funding_rate_1, funding_rate_2,
                 db_connection_string, bot_alert):
        self.bot_alert = bot_alert
        self.funding_rate_1, self.funding_rate_2 = funding_rate_1, funding_rate_2
        self.db = DatabaseConnector(db_connection_string)
        self.exchanges = {exchange_1[0]: exchange_1[1][0], exchange_2[0]: exchange_2[1][0]}
        self.ws_data = {exchange_1[0]: exchange_1[1][1], exchange_2[0]: exchange_2[1][1]}
        self.token_amount = token_amount
        self.exchange_names = list(self.exchanges.keys())
        self.leverage = leverage
        self.funding_timeout = funding_timeout
        self.mylogger = logging.getLogger(f"{exchange_1[0]}_{exchange_2[0]}_{token_amount}_{leverage}")
        filehandler = logging.FileHandler(filename=f"trade_executor_{exchange_1[0]}_{exchange_2[0]}_"
                                                   f"{time.strftime('%d-%m-%Y_%H_%M_%S', time.gmtime())}.txt")
        filehandler.setFormatter(logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self.mylogger.addHandler(filehandler)
        self.mylogger.setLevel(logging.INFO)
        self.bot_alert.send_text_message(f"I'm starting to trade\n exchanges={list(self.exchanges.keys())},\n"
                                         f"token_amount={self.token_amount},\n leverage={self.leverage},\n"
                                         f"funding_timeout={self.funding_timeout},\n"
                                         f"funding_rate_1={self.funding_rate_1},\n"
                                         f"funding_rate_2={self.funding_rate_2}\n")

    @staticmethod
    def time_threshold(start_time, timeout_in_seconds):
        if start_time + timeout_in_seconds < time.time():
            return True
        else:
            return False

    def setup_leverage(self):
        setter_1 = ThreadWithReturnValue(target=self.exchanges[self.exchange_names[0]].set_margin_type_and_leverage,
                                         args=(self.exchanges[self.exchange_names[0]].ISOLATED_MARGIN, self.leverage))
        setter_2 = ThreadWithReturnValue(target=self.exchanges[self.exchange_names[0]].set_margin_type_and_leverage,
                                         args=(self.exchanges[self.exchange_names[0]].ISOLATED_MARGIN, self.leverage))
        setter_1.start()
        setter_2.start()
        setter_1.waiting()
        setter_2.waiting()

    def order_place(self, position_state, exchange_routes, prices):
        self.mylogger.info("Starting threads")
        idx = 0 if position_state == "OPEN" else 1
        orders = {}
        if prices is None:
            prices = {}
            for exchange in self.exchanges:
                prices[exchange] = 0

        for exchange in self.exchanges:
            exchange_route = exchange_routes[exchange]
            if self.TRADE_INSTRUCTION[exchange_route][idx]["route"] == "SELL":
                route = self.exchanges[exchange].SELL
            else:
                route = self.exchanges[exchange].BUY
            orders[exchange] = ThreadWithReturnValue(
                target=self.handler_orders,
                args=(self.exchanges[exchange].place_order,
                      dict(route=route, amount=self.token_amount, type=self.exchanges[exchange].MARKET_ORDER,
                           time_in_force=self.exchanges[exchange].GOOD_UNTIL_CANCEL, price=0,
                           pos_side=self.exchanges[exchange].SHORT if exchange_route == "short" else
                           self.exchanges[exchange].LONG)
                      )
            )
            orders[exchange].start()
        return orders

    def get_orders_from_threads(self, orders):
        self.mylogger.info("Get orders from threads")
        order_data = {}
        for exchange in orders:
            order_data[exchange] = orders[exchange].waiting()
        self.mylogger.info(repr(order_data))
        return order_data

    def get_order_info(self, orders):
        self.mylogger.info("Collecting order info")
        order_info = {}
        for exchange in orders:
            order_info[exchange] = self.exchanges[exchange].get_order_info(orders[exchange])
        self.mylogger.info(repr(order_info))
        return order_info

    def handler_orders(self, order_place_func, kw):
        try:
            return order_place_func(**kw)
        except Exception as placed_order_exception:
            self.mylogger.error("something went wrong when order place", placed_order_exception)

    def wait_for_funding(self):
        self.mylogger.info("Wait for funding")
        stop_checking_funding_list = []
        while len(stop_checking_funding_list) != 2:
            for exchange in self.exchanges:
                if exchange in stop_checking_funding_list:
                    continue
                if self.exchanges[exchange].funding_timeout(self.funding_timeout):
                    self.mylogger.info(f"Exchange {exchange} funding check closed by timeout")
                    stop_checking_funding_list.append(exchange)
                with self.ws_data[exchange]["reports_lock"]:
                    if "funding_collected" in self.ws_data[exchange]["order_reports"]:
                        self.mylogger.info(f"Exchange {exchange} funding check closed by WS")
                        stop_checking_funding_list.append(exchange)

    @staticmethod
    def handle_fok_orders(orders):
        rejected_on_exchange = []
        for exchange in orders:
            if orders[exchange] is None:
                rejected_on_exchange.append(exchange)
        return rejected_on_exchange

    def do_rollback_order_when_open(self, exchange, exchange_routes):
        rollback_order = self.exchanges[exchange].place_order(
            route=self.exchanges[exchange].SELL if
            self.TRADE_INSTRUCTION[exchange_routes[exchange]][1]["route"] == "SELL" else self.exchanges[exchange].BUY,
            amount=self.token_amount,
            order_type=self.exchanges[exchange].MARKET_ORDER,
            price=0,
            pos_side=self.exchanges[exchange].SHORT if exchange_routes == "short" else
            self.exchanges[exchange].LONG
        )
        return rollback_order

    def get_funding_fees(self, start_trade_time):
        fund_fees = {}
        for exchange in self.exchanges:
            fund_fees[exchange] = self.exchanges[exchange].get_income_funding_fee(start_trade_time - 60000,
                                                                                  int(time.time() * 1000 + 60000))
            self.mylogger.info(f"Funding fee {exchange}: {fund_fees[exchange]}")
        return fund_fees

    def wait_for_close_prices(self, exchange_routes, open_prices, funding_time):
        order_books = {}
        close_prices = {}
        delta_usdt = {}
        while True:
            for exchange in self.exchange_names:
                with self.ws_data[exchange]["order_lock"]:
                    if len({"bids", "asks", "timestamp"}.intersection(
                            set(self.ws_data[exchange]["orderbook"].keys()))) != 3:
                        continue
                    order_books[exchange] = OrderBook(asks=self.ws_data[exchange]["orderbook"]["asks"],
                                                      bids=self.ws_data[exchange]["orderbook"]["bids"],
                                                      timestamp=self.ws_data[exchange]["orderbook"]["timestamp"],
                                                      symbol="")

                calculated_data = order_books[self.exchange_names[0]].calculate(
                    route=self.TRADE_INSTRUCTION[exchange_routes[exchange]][1]["route"],
                    amount=self.token_amount
                )
                close_prices[exchange] = calculated_data[0] if calculated_data is not None else -1

                if close_prices[exchange] == -1:
                    logging.info(f"Not enough in depth on {exchange}")
                    continue

                if exchange_routes[exchange] == "long":
                    delta_usdt[exchange] = self.token_amount * (close_prices[exchange] - open_prices[exchange])
                else:
                    delta_usdt[exchange] = self.token_amount * (open_prices[exchange] - close_prices[exchange])

                if self.time_threshold(funding_time, 7 * 3600 + 54 * 60):
                    return None
            time.sleep(0.1)
            self.mylogger.info(
                f"Deltas {self.exchange_names[0]}: {delta_usdt[self.exchange_names[0]]}, "
                f"{self.exchange_names[1]}: {delta_usdt[self.exchange_names[1]]} ")
            self.mylogger.info(
                f"Orderbook {self.exchange_names[0]}: bid {order_books[self.exchange_names[0]].bids[0][0]}, "
                f"ask {order_books[self.exchange_names[0]].asks[0][0]} "
                f"ts {order_books[self.exchange_names[0]].timestamp}")
            self.mylogger.info(
                f"Orderbook {self.exchange_names[1]}: bid {order_books[self.exchange_names[1]].bids[0][0]}, "
                f"ask {order_books[self.exchange_names[1]].asks[0][0]} "
                f"ts {order_books[self.exchange_names[1]].timestamp}")
            if sum(delta_usdt.values()) >= 0:
                return close_prices

    def collect_pnl_info(self, start_place_order_ts, end_place_order_ts):
        total_pnl = 0
        for exchange in self.exchanges:
            incomes = self.exchanges[exchange].get_income_history(start_place_order_ts - 60000,
                                                                  end_place_order_ts + 60000)
            pnl = 0
            for income in incomes:
                pnl += Decimal(income.income)
            self.mylogger.info(exchange + " " + repr(incomes))
            self.mylogger.info(f"PnL {exchange}: {pnl}")
            total_pnl += pnl
        return total_pnl

    def execute_trade(self, exchange_routes, open_prices):
        self.mylogger.info(f"I will execute trade with exchange_routes={exchange_routes} and open_prices={open_prices}")
        try:
            start_place_order_ts = int(time.time() * 1000)
            self.mylogger.info("Start opening FOK orders on exchanges")
            order_threads = self.order_place("OPEN", exchange_routes, open_prices)
            open_position_orders_info = self.get_orders_from_threads(order_threads)
            rejected_on_exchange = self.handle_fok_orders(open_position_orders_info)
            rollback_order = None

            if len(rejected_on_exchange) == 2:
                self.mylogger.info("Rejected orders on all exchanges. I will stop trade.")
                return None
            elif len(rejected_on_exchange) == 1:
                exchange_for_rollback = self.exchange_names[0] if self.exchange_names[0] != rejected_on_exchange[0] \
                    else self.exchange_names[1]
                self.mylogger.info(
                    f"Rejected orders on {rejected_on_exchange[0]}. "
                    f"I will rollback position on {exchange_for_rollback}.")
                rollback_order = self.do_rollback_order_when_open(exchange_for_rollback, exchange_routes)

            if rollback_order is not None:
                self.mylogger.info("Place trade report to db")
                open_position_orders_info.pop(rejected_on_exchange[0])
                open_position_orders_info = self.get_order_info(open_position_orders_info)
                closed_position_order_info = self.get_order_info(
                    {list(open_position_orders_info.keys())[0]: rollback_order})

                self.report_failed_trade_to_db(open_position_orders_info, closed_position_order_info,
                                               rejected_on_exchange[0], exchange_routes, start_place_order_ts,
                                               time.time() * 1000)
                return None

            self.wait_for_funding()
            funding_time = time.time()
            close_prices = self.wait_for_close_prices(exchange_routes, open_prices, funding_time)
            self.mylogger.info("Going to closing positions")
            if close_prices is None:
                self.mylogger.info("Timeout while waiting sum(delta_price)! Going to close by market")
            self.mylogger.info("Place close position orders")
            close_position_orders = self.order_place("CLOSE", exchange_routes, close_prices)
            close_position_orders = self.get_orders_from_threads(close_position_orders)
            self.mylogger.info("Checking rejected orders")
            rejected_on_exchange = self.handle_fok_orders(close_position_orders)

            if len(rejected_on_exchange) > 0:
                for rejected in rejected_on_exchange:
                    self.mylogger.info(f"Rejected order on exchange {rejected}. Closing position by market!")
                    close_position_orders[rejected] = self.do_rollback_order_when_open(rejected, exchange_routes)

            time.sleep(15)

            end_place_order_ts = int(time.time() * 1000)
            self.mylogger.info(f"Start timestamp: {start_place_order_ts} end timestamp: {end_place_order_ts}")
            self.mylogger.info("Collecting funding fees info")
            funding_fees = self.get_funding_fees(start_place_order_ts)
            time.sleep(1)
            self.mylogger.info("Collecting open position orders info")
            open_position_orders_info = self.get_order_info(open_position_orders_info)
            time.sleep(1)
            self.mylogger.info("Collecting pnl info")
            total_pnl = self.collect_pnl_info(start_place_order_ts, end_place_order_ts)
            time.sleep(1)
            self.mylogger.info("Collecting close order info")
            close_position_orders_info = self.get_order_info(close_position_orders)
            self.mylogger.info("Reporting to database")
            self.report_trade_to_db(open_position_orders_info, close_position_orders_info, total_pnl, exchange_routes,
                                    funding_fees, start_place_order_ts, end_place_order_ts)
        except BaseException as e:
            self.bot_alert.send_text_message("Something went wrong.", e)

    def report_failed_trade_to_db(self, open_position_orders_info, close_position_orders_info, which_exchange_failed,
                                  exchange_routes, start_place_order_ts, end_place_order_ts):
        mock_order = OrderInfo(order_id="", client_order_id="", symbol="",
                               price=Decimal("0"), status="REJECTED", route="SELL",
                               position_side="SHORT", fee=Decimal("0"), quote_qty=Decimal("0"),
                               avg_order_price=Decimal("0"), qty=Decimal("0"), order_time="")
        open_position_orders_info[which_exchange_failed] = mock_order
        close_position_orders_info[which_exchange_failed] = mock_order
        total_pnl = self.collect_pnl_info(start_place_order_ts, end_place_order_ts)
        funding_fees = {}
        for exchange in self.exchange_names:
            funding_fees[exchange] = 0
        self.report_trade_to_db(open_position_orders_info, close_position_orders_info, total_pnl, exchange_routes,
                                funding_fees, start_place_order_ts, end_place_order_ts)

    def report_trade_to_db(self, open_position_orders_info, close_position_orders_info, total_pnl, exchange_routes,
                           funding_fees, start_place_order_ts, end_place_order_ts):
        self.db.insert_trade(
            [open_position_orders_info[self.exchange_names[0]], close_position_orders_info[self.exchange_names[0]]],
            [open_position_orders_info[self.exchange_names[1]], close_position_orders_info[self.exchange_names[1]]],
            exchange_routes[self.exchange_names[0]].upper(),
            exchange_routes[self.exchange_names[1]].upper(), total_pnl, self.leverage,
            [self.funding_rate_1, funding_fees[self.exchange_names[0]]],
            [self.funding_rate_2, funding_fees[
                self.exchange_names[1]]], self.exchange_names[0], self.exchange_names[1],
            datetime.datetime.fromtimestamp(start_place_order_ts / 1000), datetime.datetime.fromtimestamp(
                end_place_order_ts / 1000), self.exchanges[self.exchange_names[0]].get_unified_symbol_name())
        self.bot_alert.send_text_message(f"I got out of position. Total PnL={total_pnl}")

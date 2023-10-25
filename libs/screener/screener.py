import itertools
import logging
import threading
import time
from decimal import Decimal

from libs.exchanges.binance import Binance as BinanceTradable
from libs.exchanges.bybit import ByBit as ByBitTradable
from libs.funding_calculator import calculate_delta, long_short_router, calculate_crypto_amount_for_usdt, \
    calculate_estimate_pnl_percent
from libs.misc import runtime
from libs.objects.OrderBook import OrderBook
from libs.screener.exchanges.binance import Binance
from libs.screener.exchanges.bybit import ByBit
from libs.thread_with_return_value import ThreadWithReturnValue


class ArbitrageChecker:
    exchange_classes = {
        "Binance": Binance,
        "ByBit": ByBit
    }
    tradable_classes = {
        "Binance": BinanceTradable,
        "ByBit": ByBitTradable
    }
    TRADE_INSTRUCTION = {
        "long": [{"route": "BUY", "isAsk": True}, {"route": "SELL", "isAsk": False}],
        "short": [{"route": "SELL", "isAsk": False}, {"route": "BUY", "isAsk": True}]
    }
    auth_data = None

    @runtime
    def find_arbitrage(self, usdt_amount, leverage, auth_data):
        self.auth_data = auth_data
        threads = []
        collected_data = []
        collected_dict = {}
        common_tickers = {}

        for exchange_class in [Binance, ByBit]:
            threads.append(ThreadWithReturnValue(target=self.handle_exchanges, args=(exchange_class,)))
            threads[-1].start()

        for thread in threads:
            collected_data.append(thread.waiting())

        for exchange in collected_data:
            collected_dict[exchange[1]] = {
                "funding": exchange[0],
                "fee": exchange[2]
            }
            for ticker in collected_dict[exchange[1]]["funding"].keys():
                if ticker not in common_tickers:
                    common_tickers[ticker] = [exchange[1]]
                else:
                    common_tickers[ticker].append(exchange[1])

        for ticker in list(common_tickers.keys()):
            if len(common_tickers[ticker]) < 2:
                common_tickers.pop(ticker)

        funding_deltas = []

        for ticker in common_tickers:
            for comb in itertools.combinations(common_tickers[ticker], 2):
                ex1, ex2 = comb
                delta_funding_without_fee = calculate_delta(collected_dict[ex1]["funding"][ticker]["funding_rate"],
                                                            collected_dict[ex2]["funding"][ticker]["funding_rate"], 0,
                                                            0)
                delta_funding_with_fee = calculate_delta(collected_dict[ex1]["funding"][ticker]["funding_rate"],
                                                         collected_dict[ex2]["funding"][ticker]["funding_rate"],
                                                         Decimal(collected_dict[ex1]["fee"]),
                                                         Decimal(collected_dict[ex2]["fee"]))
                funding_deltas.append([ex1, ex2,
                                       collected_dict[ex1]["funding"][ticker]["original_symbol"],
                                       collected_dict[ex2]["funding"][ticker]["original_symbol"],
                                       collected_dict[ex1]["funding"][ticker]["funding_rate"],
                                       collected_dict[ex2]["funding"][ticker]["funding_rate"],
                                       delta_funding_without_fee,
                                       delta_funding_with_fee,
                                       Decimal(collected_dict[ex1]["fee"]),
                                       Decimal(collected_dict[ex2]["fee"])])

        funding_deltas_sorted = sorted(funding_deltas, key=lambda x: x[7], reverse=True)
        logging.info(funding_deltas_sorted)
        funding_deltas_first_filter = []
        exchange_in_work = []
        for delta_funding in funding_deltas_sorted:
            trade_candidate = delta_funding.copy()
            if delta_funding[7] > Decimal("0.1"):
                if delta_funding[0] not in exchange_in_work and delta_funding[1] not in exchange_in_work:
                    exchange_in_work.append(delta_funding[0])
                    exchange_in_work.append(delta_funding[1])
                    funding_deltas_first_filter.append(trade_candidate)

        logging.info('AFTER FIRST FILTER ' + str(funding_deltas_first_filter))

        where_collect_prices = {}

        for row in funding_deltas_first_filter:
            for i in range(2):
                if row[i] not in where_collect_prices:
                    where_collect_prices[row[i]] = set()
                where_collect_prices[row[i]].update([row[i + 2]])

        tradable_classes = {}
        collected_prices = {}
        collected_multipliers = {}
        collected_leverages = {}
        for exchange in where_collect_prices:
            if exchange not in collected_prices:
                tradable_classes[exchange] = {}
                collected_prices[exchange] = {}
                collected_multipliers[exchange] = {}
                collected_leverages[exchange] = {}
            for ticker in where_collect_prices[exchange]:
                tradable_classes[exchange][ticker] = ThreadWithReturnValue(
                    target=self.init_tradable_classes_and_depth,
                    args=(self.tradable_classes[exchange], ticker, self.auth_data[exchange], leverage))
                collected_multipliers[exchange][ticker] = ThreadWithReturnValue(
                    target=self.handle_multiplier,
                    args=(self.exchange_classes[exchange], ticker))
                collected_leverages[exchange][ticker] = ThreadWithReturnValue(
                    target=self.handle_leverages,
                    args=(self.exchange_classes[exchange], ticker, usdt_amount, self.auth_data[exchange]))
                tradable_classes[exchange][ticker].start()
                collected_multipliers[exchange][ticker].start()
                collected_leverages[exchange][ticker].start()

        for exchange in tradable_classes:
            for ticker in tradable_classes[exchange]:
                tradable_classes[exchange][ticker] = tradable_classes[exchange][ticker].waiting()
                collected_multipliers[exchange][ticker] = collected_multipliers[exchange][ticker].waiting()
                collected_leverages[exchange][ticker] = collected_leverages[exchange][ticker].waiting()

        for arbitrage_opportunity in funding_deltas_first_filter:
            exchange_1, ticker_1, exchange_2, ticker_2 = (arbitrage_opportunity[0], arbitrage_opportunity[2],
                                                          arbitrage_opportunity[1], arbitrage_opportunity[3])

            used_leverage = self.calculate_leverage(collected_leverages[exchange_1][ticker_1],
                                                    collected_leverages[exchange_2][ticker_2], leverage)
            logging.info(f"used leverage {used_leverage}")
            setter_1 = ThreadWithReturnValue(
                target=tradable_classes[exchange_1][ticker_1][0].set_margin_type_and_leverage,
                args=(tradable_classes[exchange_1][ticker_1][0].ISOLATED_MARGIN, used_leverage))
            setter_2 = ThreadWithReturnValue(
                target=tradable_classes[exchange_2][ticker_2][0].set_margin_type_and_leverage,
                args=(tradable_classes[exchange_2][ticker_2][0].ISOLATED_MARGIN, used_leverage))
            setter_1.start()
            setter_2.start()
            setter_1.waiting()
            setter_2.waiting()

        time.sleep(10)

        for exchange in tradable_classes:
            for ticker in tradable_classes[exchange]:
                logging.info(" ".join((exchange, ticker)))
                bids = tradable_classes[exchange][ticker][1]["orderbook"]["bids"]
                asks = tradable_classes[exchange][ticker][1]["orderbook"]["asks"]
                ts = tradable_classes[exchange][ticker][1]["orderbook"]["timestamp"]
                collected_prices[exchange][ticker] = OrderBook(ticker, bids, asks, ts)

        funding_deltas_with_price_delta = []
        for arbitrage_opportunity in funding_deltas_first_filter:
            arbitrage_opportunity_copy = arbitrage_opportunity.copy()
            logging.info(arbitrage_opportunity_copy)
            (exchange_1, funding_rate_1, ticker_1, exchange_2,
             funding_rate_2, ticker_2, fee_1, fee_2) = (arbitrage_opportunity[0], arbitrage_opportunity[4],
                                                        arbitrage_opportunity[2], arbitrage_opportunity[1],
                                                        arbitrage_opportunity[5], arbitrage_opportunity[3],
                                                        arbitrage_opportunity[8], arbitrage_opportunity[9])

            exchange_routes = long_short_router(exchange_1, funding_rate_1, exchange_2, funding_rate_2)
            logging.info(exchange_routes)
            ex1_price = collected_prices[exchange_1][ticker_1].asks[1][0] if (
                    self.TRADE_INSTRUCTION[exchange_routes[exchange_1]][0]["isAsk"] is True) \
                else collected_prices[exchange_1][ticker_1].bids[1][0]
            ex2_price = collected_prices[exchange_2][ticker_2].asks[1][0] if (
                    self.TRADE_INSTRUCTION[exchange_routes[exchange_2]][0]["isAsk"] is True) \
                else collected_prices[exchange_2][ticker_2].bids[1][0]

            used_leverage = self.calculate_leverage(collected_leverages[exchange_1][ticker_1],
                                                    collected_leverages[exchange_2][ticker_2], leverage)

            filtered_amount = calculate_crypto_amount_for_usdt(ex_1=self.exchange_classes[exchange_1],
                                                               ex_2=self.exchange_classes[exchange_2],
                                                               ex_1_price=ex1_price, ex_2_price=ex2_price,
                                                               usdt_amount=usdt_amount, ex1_ticker=ticker_1,
                                                               ex2_ticker=ticker_2,
                                                               multiplier_ex1=collected_multipliers[exchange_1][
                                                                   ticker_1],
                                                               multiplier_ex2=collected_multipliers[exchange_2][
                                                                   ticker_2])
            if filtered_amount == -1:
                continue

            estimated_pnl_percent = calculate_estimate_pnl_percent(
                funding_rate_1 / 100,
                funding_rate_2 / 100,
                filtered_amount * ex1_price * used_leverage,
                filtered_amount * ex2_price * used_leverage,
                fee_1 / 100,
                fee_2 / 100, filtered_amount,
                ex1_price if exchange_routes[exchange_1].upper() == "LONG" else ex2_price,
                ex2_price if exchange_routes[exchange_2].upper() == "SHORT" else ex1_price, used_leverage
            )

            arbitrage_opportunity_copy.extend(
                [filtered_amount, ex1_price, ex2_price, estimated_pnl_percent, used_leverage, exchange_routes])
            arbitrage_opportunity_copy[0] = (arbitrage_opportunity_copy[0], tradable_classes[exchange_1][ticker_1])
            arbitrage_opportunity_copy[1] = (arbitrage_opportunity_copy[1], tradable_classes[exchange_2][ticker_2])
            funding_deltas_with_price_delta.append(arbitrage_opportunity_copy)
        return funding_deltas_with_price_delta

    @runtime
    def handle_exchanges(self, exchange):
        ex = exchange()
        return ex.get_funding_rate("USDT"), ex.exchange_name, ex.taker_fee

    @runtime
    def handle_depth(self, exchange, ticker):
        ex = exchange()
        order_book = ex.get_futures_depth(ticker)
        return OrderBook(ticker, order_book['bids'], order_book['asks'], 0)

    @staticmethod
    def handle_multiplier(exchange, ticker):
        return exchange.get_multiplier(ticker)

    @staticmethod
    def handle_leverages(exchange, ticker, usdt_amount, auth_data):
        ex = exchange([], auth_data=auth_data)
        return ex.get_max_leverage_for_usdt_amount(ticker, usdt_amount)

    @staticmethod
    def calculate_leverage(leverage_config_1, leverage_config_2, leverage):
        max_lever_1, step_1 = leverage_config_1
        max_lever_2, step_2 = leverage_config_2

        if leverage < max_lever_1 and leverage < max_lever_2:
            return leverage
        else:
            max_step = max(step_1, step_2)
            max_common_lever = min(max_lever_1, max_lever_2)
            return max_common_lever.quantize(max_step)

    @staticmethod
    def init_tradable_classes_and_depth(tradable_class, ticker, auth_data):

        ws_metadata = dict(orderbook={}, order_reports={}, balancelist={}, orderlock=threading.Lock(),
                           balancelock=threading.Lock(), reportslock=threading.Lock())
        auth_data = auth_data.copy()
        auth_data["symbol"] = ticker
        t_class = tradable_class(auth_data)
        ws_metadata["thread"] = t_class.get_websockets_handler(**ws_metadata)
        ws_metadata["thread"].daemon = True
        ws_metadata["thread"].start()

        return t_class, ws_metadata

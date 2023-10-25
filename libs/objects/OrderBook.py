import json
from decimal import Decimal


class OrderBook:
    def __init__(self, symbol: str, bids: list, asks: list, timestamp: int):
        self.bids = bids
        self.asks = asks
        self.symbol = symbol
        self.timestamp = timestamp

    @staticmethod
    def calc_delta(num_1: Decimal, num_2: Decimal):
        """
        Args:
            num_1: Number 1
            num_2: Number 2
        Returns:
            delta: delta between numbers in percent
        """
        return (abs(num_1 - num_2) / ((num_1 + num_2) / 2)) * 100

    def calculate(self, route: str, amount: Decimal = 0) -> (Decimal, Decimal, Decimal):
        """
        Args:
            route: Route for calculation. 'BUY' or 'SELL' only
            amount: Amount token in first part of ticker
        Returns:
            price: order price
            avg_order_price: average order price
            usdt_amount: spent/received token in second part of ticker
        """
        amount_orig = amount
        if route == "BUY":
            if amount > 0:
                buy_price, avg_buy_price, usdt_amount, is_first = 0, 0, 0, True
                for i in self.asks:
                    if Decimal(i[1]) >= amount and is_first:
                        return Decimal(i[0]), Decimal(i[0]), amount * Decimal(i[0])
                    elif Decimal(i[1]) < amount and is_first:
                        is_first = False
                        usdt_amount += Decimal(i[1]) * Decimal(i[0])
                        amount -= Decimal(i[1])
                        continue
                    if not is_first:
                        if Decimal(i[1]) <= amount:
                            usdt_amount += Decimal(i[1]) * Decimal(i[0])
                            amount -= Decimal(i[1])
                            if amount == 0:
                                return Decimal(i[0]), usdt_amount / amount_orig, usdt_amount
                        else:
                            usdt_amount += amount * Decimal(i[0])
                            return Decimal(i[0]), usdt_amount / amount_orig, usdt_amount
        elif route == "SELL":
            if amount > 0:
                sell_price, avg_sell_price, usdt_amount, is_first = 0, 0, 0, True
                for i in self.bids:
                    if Decimal(i[1]) >= amount and is_first:
                        return Decimal(i[0]), Decimal(i[0]), amount * Decimal(i[0])
                    elif Decimal(i[1]) < amount and is_first:
                        is_first = False
                        usdt_amount += Decimal(i[1]) * Decimal(i[0])
                        amount -= Decimal(i[1])
                        continue
                    if not is_first:
                        if Decimal(i[1]) <= amount:
                            usdt_amount += Decimal(i[1]) * Decimal(i[0])
                            amount -= Decimal(i[1])
                            if amount == 0:
                                return Decimal(i[0]), usdt_amount / amount_orig, usdt_amount
                        else:
                            usdt_amount += amount * Decimal(i[0])
                            return Decimal(i[0]), usdt_amount / amount_orig, usdt_amount

    def calculate_for_usdt(self, route: str, amount: Decimal = 0):
        """
        Args:
            route: Route for calculation. 'BUY' or 'SELL' only
            amount: Amount token in second part of ticker
        Returns:
            price: order price
            avg_order_price: average order price
            first_amount: spent/received token in first part of ticker
        """
        amount_orig = amount
        if route == "BUY":
            if amount > 0:
                buy_price, avg_buy_price, usdt_amount, is_first = 0, 0, 0, True
                for i in self.asks:
                    if Decimal(i[1]) * Decimal(i[0]) >= amount and is_first:
                        return Decimal(i[0]), Decimal(i[0]), amount / Decimal(i[0])
                    elif Decimal(i[1]) * Decimal(i[0]) < amount and is_first:
                        is_first = False
                        usdt_amount += Decimal(i[1])
                        amount -= Decimal(i[1]) * Decimal(i[0])
                        continue
                    if not is_first:
                        if Decimal(i[1]) * Decimal(i[0]) <= amount:
                            usdt_amount += Decimal(i[1])
                            amount -= Decimal(i[1]) * Decimal(i[0])
                            if amount == 0:
                                return Decimal(i[0]), amount_orig / usdt_amount, usdt_amount
                        else:
                            usdt_amount += amount / Decimal(i[0])
                            return Decimal(i[0]), amount_orig / usdt_amount, usdt_amount
        elif route == "SELL":
            if amount > 0:
                sell_price, avg_sell_price, usdt_amount, is_first = 0, 0, 0, True
                for i in self.bids:
                    if Decimal(i[1]) * Decimal(i[0]) >= amount and is_first:
                        return Decimal(i[0]), Decimal(i[0]), amount / Decimal(i[0])
                    elif Decimal(i[1]) * Decimal(i[0]) < amount and is_first:
                        is_first = False
                        usdt_amount += Decimal(i[1])
                        amount -= Decimal(i[1]) * Decimal(i[0])
                        continue
                    if not is_first:
                        if Decimal(i[1]) * Decimal(i[0]) <= amount:
                            usdt_amount += Decimal(i[1])
                            amount -= Decimal(i[1]) * Decimal(i[0])
                            if amount == 0:
                                return Decimal(i[0]), amount_orig / usdt_amount, usdt_amount
                        else:
                            usdt_amount += amount / Decimal(i[0])
                            return Decimal(i[0]), amount_orig / usdt_amount, usdt_amount

    def to_json(self):
        return json.dumps({"bids": self.bids[:25], "asks": self.asks[:25], "timestamp": self.timestamp}, default=str)

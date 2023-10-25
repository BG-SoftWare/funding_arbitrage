import json
from decimal import Decimal


class Order:
    def __init__(self, order_id: str, client_order_id: str, symbol: str, price: Decimal, status: str):
        self.order_id = order_id
        self.client_order_id = client_order_id
        self.symbol = symbol
        self.price = price
        self.status = status

    def __repr__(self):
        return json.dumps({
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "price": self.price,
            "status": self.status
        }, default=str)

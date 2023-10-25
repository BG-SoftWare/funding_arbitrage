import json
from decimal import Decimal
from datetime import datetime
from libs.objects.Order import Order


class OrderInfo(Order):
    def __init__(self, order_id: str, client_order_id: str, symbol: str, price: Decimal, status: str,
                 route: str, position_side: str, fee: Decimal, quote_qty: Decimal,
                 avg_order_price: Decimal, qty: Decimal, order_time: datetime | str):
        self.route = route
        self.position_side = position_side
        self.fee = fee
        self.quote_qty = quote_qty
        self.avg_order_price = avg_order_price
        self.qty = qty
        self.order_time = order_time
        super().__init__(order_id, client_order_id, symbol, price, status)

    def __repr__(self):
        return json.dumps({
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "price": self.price,
            "status": self.status,
            "route": self.route,
            "position_side": self.position_side,
            "fee": self.fee,
            "quote_qty": self.quote_qty,
            "avg_order_price": self.avg_order_price,
            "qty": self.qty,
            "order_time": self.order_time
        }, default=str)

import json
from decimal import Decimal


class Trade:
    def __init__(self, symbol: str, trade_id: str, order_id: str, side: str, price: Decimal, qty: Decimal,
                 realized_pnl: Decimal, margin_asset: str, quote_qty: Decimal, commission: Decimal,
                 commission_asset: str, datetime: int, position_side: str | None, maker: bool | None, buyer: bool | None):
        self.symbol = symbol
        self.trade_id = trade_id
        self.order_id = order_id
        self.side = side
        self.price = price
        self.qty = qty
        self.realized_pnl = realized_pnl
        self.margin_asset = margin_asset
        self.quote_qty = quote_qty
        self.commission = commission
        self.commissionAsset = commission_asset
        self.time = datetime
        self.position_side = position_side
        self.maker = maker
        self.buyer = buyer

    def __repr__(self):
        return json.dumps({
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "orderId": self.order_id,
            "side": self.side,
            "price": self.price,
            "qty": self.qty,
            "realizedPnl": self.realized_pnl,
            "marginAsset": self.margin_asset,
            "quoteQty": self.quote_qty,
            "commission": self.commission,
            "commissionAsset": self.commissionAsset,
            "time": self.time,
            "positionSide": self.position_side,
            "maker": self.maker,
            "buyer": self.buyer
        }, default=str)

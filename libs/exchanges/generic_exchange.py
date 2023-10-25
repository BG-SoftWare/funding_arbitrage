class GenericExchange:
    __required_args = None

    def __init__(self, kwargs):
        pass

    def place_order(self, route, price, amount, order_type):
        raise NotImplementedError("You must implement order")

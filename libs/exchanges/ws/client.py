import logging
import threading

import websocket


class Client(threading.Thread):
    def __init__(self, url: str, exchange: str, headers=None):
        super().__init__()
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
            header=headers
        )
        self.exchange = exchange

    def run(self):
        while True:
            try:
                self.ws.run_forever()
            except KeyboardInterrupt:
                break
            except BaseException as e:
                print(e)

    def on_message(self, message):
        logging.debug(message)
        pass

    def on_error(self, error):
        logging.debug(error)

    def on_close(self):
        logging.debug("### closed ###")

    def on_open(self):
        logging.debug(f'Connected to {self.exchange}\n')

    def query_ws_order_status(self):
        pass



import requests


class BotAlert:
    def __init__(self, chat_id, token):
        self.chat_id = chat_id
        self.token = token

    def send_text_message(self, text):
        params = {"chat_id": str(self.chat_id),
                  "text": "{0}".format(text)}
        requests.post("https://api.telegram.org/bot{0}/sendMessage".format(self.token), params=params)

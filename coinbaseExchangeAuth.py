class CoinbaseExchangeAuth():
    def __init__(self):
        self.api_key = API_KEY
        self.secret_key = API_SECRET
        self.passphrase = API_PASS

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

class CoinbaseConsts:
    KEY_PRODUCTID = 'id'
    KEY_COINNAME = 'base_currency'
    KEY_QUOTECURRENCY = 'quote_currency'


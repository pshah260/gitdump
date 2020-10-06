import ssl
import websocket
import json
import base64
import hmac
import hashlib
import time
from api import api

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")


gemini_api_key = api().key
gemini_api_secret = api().secret.encode()

payload = {"request": "/v1/order/events", "nonce": int(time.time()*1000)}
encoded_payload = json.dumps(payload).encode()
b64 = base64.b64encode(encoded_payload)
signature = hmac.new(gemini_api_secret, b64, hashlib.sha384).hexdigest()

ws = websocket.WebSocketApp("wss://api.gemini.com/v1/order/events?symbolFilter=btcusd&eventTypeFilter=fill&eventTypeFilter=closed&apiSessionFilter=UI",
                            on_message=on_message,
                            header={
                                'X-GEMINI-PAYLOAD': b64.decode(),
                                'X-GEMINI-APIKEY': gemini_api_key,
                                'X-GEMINI-SIGNATURE': signature
                            })
ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

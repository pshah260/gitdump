import ssl
import websocket

def on_message(ws, message):
    print(message)

ws = websocket.WebSocketApp(
    "wss://api.gemini.com/v1/marketdata/BTCUSD?trades=true",
    on_message=on_message)
ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

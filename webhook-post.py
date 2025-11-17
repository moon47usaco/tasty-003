import requests #

url = "https://miffed-unjudgeable-loyce.ngrok-free.dev/tradingview-webhook" #Set this adress in tradingview
payload = { "ticker": "MES1!", "action": "buy", "sentiment": "long", "quantity": "1", "price": "22928.00", "time": "2025-09-08T06:04:00Z" }
# payload = { "ticker": "MES1!", "action": "sell", "sentiment": "flat", "quantity": "1", "price": "6505.00", "time": "2025-09-08T06:04:00Z" }
headers = {"Content-Type": "application/json"}

response = requests.post(url, json=payload, headers=headers)


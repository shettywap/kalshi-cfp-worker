import os
import time
import json
import base64
import requests
from urllib.parse import urlparse
from datetime import datetime
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import firebase_admin
from firebase_admin import credentials, firestore

# ---------------------
# FIREBASE INIT
# ---------------------
if not firebase_admin._apps:
    cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"]))
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ---------------------
# KALSHI CONFIG
# ---------------------
API_KEY = os.environ["KALSHI_API_KEY"]

BASE = "https://api.elections.kalshi.com/trade-api/v2"
CFP_EVENT = "KXNCAAFPLAYOFF-25"

# Load private key
private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_PRIVATE_KEY"].encode(),
    password=None
)

# ---------------------
# SIGNED REQUEST (EXACT COLAB VERSION)
# ---------------------
def kalshi_signed_request(method, url, body=None):
    path = urlparse(url).path
    timestamp = str(int(time.time() * 1000))
    message = timestamp + method.upper() + path

    signature_bytes = private_key.sign(
        message.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    signature = base64.b64encode(signature_bytes).decode()

    headers = {
        "KALSHI-ACCESS-KEY": API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "Content-Type": "application/json"
    }

    if method == "GET":
        return requests.get(url, headers=headers)
    else:
        return requests.post(url, headers=headers, data=json.dumps(body))


# ---------------------
# FETCH MARKETS
# ---------------------
def fetch_cfp_markets():
    url = f"{BASE}/markets?event_ticker={CFP_EVENT}&limit=500"
    resp = kalshi_signed_request("GET", url)

    # Handle non-JSON
    try:
        data = resp.json()
    except:
        print("❌ NON-JSON RESPONSE:", resp.text)
        return []

    if resp.status_code != 200:
        print("❌ API ERROR:", data)
        return []

    # Some responses contain garbage "Associated Markets" keys → remove invalid entries
    markets = data.get("markets", [])
    clean = []

    for m in markets:
        # Valid markets MUST have ticker + yes_price or last_price
        if not isinstance(m, dict):
            continue
        if "ticker" not in m:
            continue
        clean.append(m)

    return clean


# ---------------------
# POLLING LOGIC
# ---------------------
def poll_once(last_prices):
    markets = fetch_cfp_markets()
    ts = datetime.utcnow().isoformat()

    if not markets:
        print(f"{ts} | No markets returned.")
        return last_prices, 0

    movers = []
    for m in markets:
        ticker = m.get("ticker")
        yesp = m.get("yes_price")
        lastp = m.get("last_price")

        price = yesp if yesp is not None else lastp
        if price is None:
            continue

        prev = last_prices.get(ticker)

        # First time seen
        if prev is None:
            last_prices[ticker] = price
            continue

        # Detect movement ≥ 1 point
        diff = price - prev
        if abs(diff) >= 1:
            movers.append({
                "ticker": ticker,
                "change": diff,
                "old": prev,
                "new": price,
                "ts": ts
            })

            last_prices[ticker] = price

    # Write movers into Firebase
    if movers:
        doc = db.collection("movers").document(ts)
        doc.set({
            "timestamp": ts,
            "count": len(movers),
            "items": movers
        })

    print(f"{ts} | tickers={len(markets)} | movers={len(movers)}")

    return last_prices, len(movers)


# ---------------------
# MAIN LOOP
# ---------------------
def main():
    last_prices = {}
    print("🚀 Worker started with BASE =", BASE)

    while True:
        try:
            last_prices, mover_count = poll_once(last_prices)
        except Exception as e:
            print("❌ ERROR in poller:", e)

        time.sleep(5)


if __name__ == "__main__":
    main()

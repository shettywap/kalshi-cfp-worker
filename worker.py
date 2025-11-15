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
# Railway env: FIREBASE_SERVICE_ACCOUNT_JSON (full JSON string)
if not firebase_admin._apps:
    firebase_sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]
    cred_dict = json.loads(firebase_sa_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ---------------------
# KALSHI CONFIG
# ---------------------
# Railway envs:
#   KALSHI_API_KEY_ID
#   KALSHI_PRIVATE_KEY_PEM
API_KEY = os.environ["KALSHI_API_KEY_ID"]

BASE = "https://api.elections.kalshi.com/trade-api/v2"
CFP_EVENT = "KXNCAAFPLAYOFF-25"

# How big a move (in price points) counts as "major"
MIN_MOVE = float(os.getenv("MIN_MOVE", "1.0"))

# Load private key (PEM)
private_key_pem = os.environ["KALSHI_PRIVATE_KEY_PEM"].encode()
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None
)

# ---------------------
# SIGNED REQUEST
# ---------------------
def kalshi_signed_request(method, url, body=None):
    """
    Sign a Kalshi request using the same logic that worked in Colab.
    """
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

    try:
        if method.upper() == "GET":
            return requests.get(url, headers=headers, timeout=10)
        else:
            return requests.post(url, headers=headers, data=json.dumps(body or {}), timeout=10)
    except requests.exceptions.RequestException as e:
        print("❌ HTTP ERROR:", e)
        return None


# ---------------------
# FETCH MARKETS
# ---------------------
def fetch_cfp_markets():
    url = f"{BASE}/markets?event_ticker={CFP_EVENT}&limit=500"
    resp = kalshi_signed_request("GET", url)

    if resp is None:
        return []

    # Handle non-JSON
    try:
        data = resp.json()
    except Exception:
        print("❌ NON-JSON RESPONSE:", resp.text[:500])
        return []

    if resp.status_code != 200:
        print("❌ API ERROR:", resp.status_code, data)
        return []

    markets = data.get("markets", [])
    clean = []

    for m in markets:
        # Valid markets MUST have ticker and at least one price field
        if not isinstance(m, dict):
            continue
        if "ticker" not in m:
            continue
        clean.append(m)

    return clean


# ---------------------
# POLLING + FIREBASE WRITE
# ---------------------
def poll_once(last_prices):
    markets = fetch_cfp_markets()
    ts = datetime.utcnow().isoformat() + "Z"

    if not markets:
        print(f"{ts} | No markets returned.")
        return last_prices, 0

    # --- Write current odds for ticker tape ---
    ticker_payload = []
    for m in markets:
        ticker = m.get("ticker")
        yesp = m.get("yes_price")
        lastp = m.get("last_price")
        best_bid = m.get("best_bid")
        best_ask = m.get("best_ask")

        # Choose a representative price (yes_price preferred)
        price = yesp if yesp is not None else lastp
        if price is None:
            continue

        # Probability as 0–1 if price is in points (0–100)
        prob = None
        try:
            prob = float(price) / 100.0
        except Exception:
            pass

        ticker_payload.append({
            "ticker": ticker,
            "yes_price": yesp,
            "last_price": lastp,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "probability": prob
        })

    # Single doc used by the app for the live ticker
    db.collection("cfp_markets").document("current").set({
        "timestamp": ts,
        "event_ticker": CFP_EVENT,
        "markets": ticker_payload
    })

    # --- Detect major movers ---
    movers = []
    for m in markets:
        ticker = m.get("ticker")
        yesp = m.get("yes_price")
        lastp = m.get("last_price")

        price = yesp if yesp is not None else lastp
        if price is None:
            continue

        try:
            price = float(price)
        except Exception:
            continue

        prev = last_prices.get(ticker)

        # First time for this ticker: just initialize
        if prev is None:
            last_prices[ticker] = price
            continue

        diff = price - prev
        if abs(diff) >= MIN_MOVE:
            movers.append({
                "ticker": ticker,
                "change": diff,
                "old": prev,
                "new": price,
                "timestamp": ts
            })
            last_prices[ticker] = price  # update stored price

    # --- Write movers for alerting ---
    if movers:
        # Use timestamp as doc id so they’re naturally ordered
        doc_ref = db.collection("movers").document(ts)
        doc_ref.set({
            "timestamp": ts,
            "event_ticker": CFP_EVENT,
            "min_move": MIN_MOVE,
            "count": len(movers),
            "items": movers
        })

    print(f"{ts} | tickers={len(ticker_payload)} | movers={len(movers)}")

    return last_prices, len(movers)


# ---------------------
# MAIN LOOP
# ---------------------
def main():
    last_prices = {}
    print("🚀 Worker started")
    print("   BASE =", BASE)
    print("   EVENT =", CFP_EVENT)
    print("   MIN_MOVE =", MIN_MOVE)

    while True:
        try:
            last_prices, mover_count = poll_once(last_prices)
        except Exception as e:
            # Keep the worker alive on unexpected errors
            print("❌ ERROR in poller:", repr(e))

        # Poll every 5 seconds
        time.sleep(5)


if __name__ == "__main__":
    main()

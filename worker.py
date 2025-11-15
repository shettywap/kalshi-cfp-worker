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

print("📄 Importing worker.py...", flush=True)

# ---------------------
# FIREBASE INIT
# ---------------------
db = None

def init_firebase():
    global db
    if firebase_admin._apps:
        print("✅ Firebase already initialized", flush=True)
        db = firestore.client()
        return

    try:
        print("🔑 Loading FIREBASE_SERVICE_ACCOUNT_JSON...", flush=True)
        firebase_sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]
    except KeyError:
        print("❌ Missing env var: FIREBASE_SERVICE_ACCOUNT_JSON", flush=True)
        raise

    try:
        cred_dict = json.loads(firebase_sa_json)
    except json.JSONDecodeError as e:
        print("❌ Failed to parse FIREBASE_SERVICE_ACCOUNT_JSON as JSON:", e, flush=True)
        raise

    try:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase initialized", flush=True)
    except Exception as e:
        print("❌ Error initializing Firebase:", repr(e), flush=True)
        raise


# ---------------------
# KALSHI CONFIG
# ---------------------
try:
    API_KEY = os.environ["KALSHI_API_KEY_ID"]
    print("✅ Loaded KALSHI_API_KEY_ID", flush=True)
except KeyError:
    print("❌ Missing env var: KALSHI_API_KEY_ID", flush=True)
    raise

BASE = "https://api.elections.kalshi.com/trade-api/v2"
CFP_EVENT = "KXNCAAFPLAYOFF-25"

# How big a move counts as "major"
try:
    MIN_MOVE = float(os.getenv("MIN_MOVE", "1.0"))
    print(f"✅ MIN_MOVE set to {MIN_MOVE}", flush=True)
except ValueError:
    print("❌ MIN_MOVE is not a valid number", flush=True)
    raise

# Load private key (PEM)
try:
    private_key_pem = os.environ["KALSHI_PRIVATE_KEY_PEM"].encode()
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=None
    )
    print("✅ Loaded KALSHI_PRIVATE_KEY_PEM", flush=True)
except KeyError:
    print("❌ Missing env var: KALSHI_PRIVATE_KEY_PEM", flush=True)
    raise
except Exception as e:
    print("❌ Error loading private key:", repr(e), flush=True)
    raise


# ---------------------
# SIGNED REQUEST
# ---------------------
def kalshi_signed_request(method, url, body=None):
    path = urlparse(url).path
    timestamp = str(int(time.time() * 1000))
    message = timestamp + method.upper() + path

    try:
        signature_bytes = private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
    except Exception as e:
        print("❌ Error signing message:", repr(e), flush=True)
        return None

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
        print("❌ HTTP ERROR:", e, flush=True)
        return None


# ---------------------
# FETCH MARKETS
# ---------------------
def fetch_cfp_markets():
    url = f"{BASE}/markets?event_ticker={CFP_EVENT}&limit=500"
    print(f"🌐 Fetching markets from {url}", flush=True)

    resp = kalshi_signed_request("GET", url)

    if resp is None:
        print("❌ No response from Kalshi", flush=True)
        return []

    # Handle non-JSON
    try:
        data = resp.json()
    except Exception:
        print("❌ NON-JSON RESPONSE:", resp.status_code, resp.text[:500], flush=True)
        return []

    if resp.status_code != 200:
        print("❌ API ERROR:", resp.status_code, data, flush=True)
        return []

    markets = data.get("markets", [])
    clean = []

    for m in markets:
        if not isinstance(m, dict):
            continue
        if "ticker" not in m:
            continue
        clean.append(m)

    print(f"✅ Fetched {len(clean)} clean markets", flush=True)
    return clean


# ---------------------
# POLLING + FIRESTORE WRITE
# ---------------------
def poll_once(last_prices):
    markets = fetch_cfp_markets()
    ts = datetime.utcnow().isoformat() + "Z"

    if not markets:
        print(f"{ts} | No markets returned.", flush=True)
        return last_prices, 0

    # --- Write current odds for ticker tape ---
    ticker_payload = []
    for m in markets:
        ticker = m.get("ticker")
        yesp = m.get("yes_price")
        lastp = m.get("last_price")
        best_bid = m.get("best_bid")
        best_ask = m.get("best_ask")

        price = yesp if yesp is not None else lastp
        if price is None:
            continue

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

    try:
        db.collection("cfp_markets").document("current").set({
            "timestamp": ts,
            "event_ticker": CFP_EVENT,
            "markets": ticker_payload
        })
        print(f"{ts} | ✅ Wrote {len(ticker_payload)} markets to Firestore", flush=True)
    except Exception as e:
        print("❌ Error writing current markets to Firestore:", repr(e), flush=True)

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
            last_prices[ticker] = price

    if movers:
        try:
            db.collection("movers").document(ts).set({
                "timestamp": ts,
                "event_ticker": CFP_EVENT,
                "min_move": MIN_MOVE,
                "count": len(movers),
                "items": movers
            })
            print(f"{ts} | 🚨 Recorded {len(movers)} movers", flush=True)
        except Exception as e:
            print("❌ Error writing movers to Firestore:", repr(e), flush=True)
    else:
        print(f"{ts} | No movers this tick", flush=True)

    return last_prices, len(movers)


# ---------------------
# MAIN LOOP
# ---------------------
def main():
    print("🚀 Starting worker main()", flush=True)
    init_firebase()

    last_prices = {}
    print("✅ Worker fully initialized", flush=True)
    print("   BASE     =", BASE, flush=True)
    print("   EVENT    =", CFP_EVENT, flush=True)
    print("   MIN_MOVE =", MIN_MOVE, flush=True)

    while True:
        try:
            last_prices, _ = poll_once(last_prices)
        except Exception as e:
            print("❌ ERROR in poller:", repr(e), flush=True)

        time.sleep(5)


if __name__ == "__main__":
    main()

import os
import time
import json
import base64
import requests
import traceback
from urllib.parse import urlparse
from datetime import datetime

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from google.cloud import firestore_v1
from google.oauth2 import service_account

print("📄 Importing worker.py (REST Firestore)...", flush=True)

# =====================
# FIRESTORE INIT (REST)
# =====================

def init_firestore():
    """
    Initialize a Firestore client using REST transport and a service account JSON
    stored in FIREBASE_SERVICE_ACCOUNT_JSON.
    """
    try:
        sa_raw = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]
        print("🔑 Loaded FIREBASE_SERVICE_ACCOUNT_JSON from env", flush=True)
    except KeyError:
        print("❌ Missing env var: FIREBASE_SERVICE_ACCOUNT_JSON", flush=True)
        raise

    try:
        sa_info = json.loads(sa_raw)
    except json.JSONDecodeError as e:
        print("❌ FIREBASE_SERVICE_ACCOUNT_JSON is not valid JSON:", e, flush=True)
        raise

    project_id = sa_info.get("project_id")
    if not project_id:
        print("❌ Service account JSON has no 'project_id'", flush=True)
        raise RuntimeError("Service account JSON missing project_id")

    try:
        creds = service_account.Credentials.from_service_account_info(sa_info)
        # Use REST transport explicitly
        client = firestore_v1.Client(
            project=project_id,
            credentials=creds,
            client_options={"api_endpoint": "firestore.googleapis.com"},
            transport="rest"
        )
        print(f"✅ Firestore client initialized (project={project_id}, transport=rest)", flush=True)
        return client
    except Exception as e:
        print("❌ Error initializing Firestore client:", repr(e), flush=True)
        traceback.print_exc()
        raise


db = init_firestore()

# =====================
# KALSHI CONFIG
# =====================

try:
    API_KEY = os.environ["KALSHI_API_KEY_ID"]
    print("✅ Loaded KALSHI_API_KEY_ID", flush=True)
except KeyError:
    print("❌ Missing env var: KALSHI_API_KEY_ID", flush=True)
    raise

BASE = "https://api.elections.kalshi.com/trade-api/v2"
CFP_EVENT = "KXNCAAFPLAYOFF-25"

try:
    MIN_MOVE = float(os.getenv("MIN_MOVE", "1.0"))
    print(f"✅ MIN_MOVE set to {MIN_MOVE}", flush=True)
except ValueError:
    print("❌ MIN_MOVE is not a valid number", flush=True)
    raise

# Load Kalshi private key (PEM)
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
    traceback.print_exc()
    raise


# =====================
# SIGNED REQUEST
# =====================

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
        print("❌ Error signing Kalshi message:", repr(e), flush=True)
        traceback.print_exc()
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
        print("❌ HTTP ERROR talking to Kalshi:", e, flush=True)
        traceback.print_exc()
        return None


# =====================
# FETCH MARKETS
# =====================

def fetch_cfp_markets():
    url = f"{BASE}/markets?event_ticker={CFP_EVENT}&limit=500"
    print(f"🌐 Fetching markets from {url}", flush=True)

    resp = kalshi_signed_request("GET", url)
    if resp is None:
        print("❌ No response from Kalshi", flush=True)
        return []

    try:
        data = resp.json()
    except Exception:
        print("❌ NON-JSON RESPONSE:", resp.status_code, resp.text[:500], flush=True)
        return []

    if resp.status_code != 200:
        print("❌ Kalshi API ERROR:", resp.status_code, data, flush=True)
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


# =====================
# POLLING + FIRESTORE WRITE
# =====================

def poll_once(last_prices):
    markets = fetch_cfp_markets()
    ts = datetime.utcnow().isoformat() + "Z"

    if not markets:
        print(f"{ts} | No markets returned.", flush=True)
        return last_prices, 0

    # ---- Current odds doc for ticker tape ----
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
        print(f"{ts} | ✅ Wrote {len(ticker_payload)} markets to Firestore (cfp_markets/current)", flush=True)
    except Exception as e:
        print("❌ Error writing current markets to Firestore:", repr(e), flush=True)
        traceback.print_exc()

    # ---- Detect major movers ----
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
            traceback.print_exc()
    else:
        print(f"{ts} | No movers this tick", flush=True)

    return last_prices, len(movers)


# =====================
# MAIN LOOP
# =====================

def main():
    print("🚀 Worker main() starting", flush=True)
    last_prices = {}

    print("✅ Worker initialized", flush=True)
    print("   BASE     =", BASE, flush=True)
    print("   EVENT    =", CFP_EVENT, flush=True)
    print("   MIN_MOVE =", MIN_MOVE, flush=True)

    while True:
        try:
            last_prices, _ = poll_once(last_prices)
        except Exception as e:
            print("❌ ERROR in poller loop:", repr(e), flush=True)
            traceback.print_exc()

        time.sleep(5)


if __name__ == "__main__":
    main()

import os
import time
import json
import requests
import base64
from datetime import datetime, timezone

from google.cloud import firestore
from google.oauth2 import service_account
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ============================================================
# CONFIG
# ============================================================

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM")

# SPORTS MARKETS LIVE IN V3, NOT V2
BASE_URL = "https://api.elections.kalshi.com/trade-api/v3"

CFP_PREFIX = "KXNCAAFPLAYOFF-25-"   # THIS IS THE ONLY RELIABLE IDENTIFIER

MIN_MOVE = float(os.getenv("MIN_MOVE", "1"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))

FIREBASE_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

if not API_KEY_ID:
    raise RuntimeError("KALSHI_API_KEY_ID not set")
if not PRIVATE_KEY_PEM:
    raise RuntimeError("KALSHI_PRIVATE_KEY_PEM not set")
if not FIREBASE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON not set")


# ============================================================
# FIRESTORE INIT
# ============================================================

service_account_info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
firebase_creds = service_account.Credentials.from_service_account_info(
    service_account_info
)
db = firestore.Client(
    project=service_account_info["project_id"],
    credentials=firebase_creds,
)


# ============================================================
# SIGNING
# ============================================================

def load_private_key():
    return serialization.load_pem_private_key(
        PRIVATE_KEY_PEM.encode("utf-8"),
        password=None,
    )

PRIVATE_KEY = load_private_key()

def sign_message(message: str) -> str:
    signature = PRIVATE_KEY.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def kalshi_signed_get(path: str):
    from urllib.parse import urlparse

    base = BASE_URL.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    url = base + path

    parsed = urlparse(url)
    method = "GET"
    timestamp = str(int(time.time() * 1000))
    message = timestamp + method + parsed.path

    headers = {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": sign_message(message),
        "Content-Type": "application/json",
    }

    return requests.get(url, headers=headers, timeout=10)


def kalshi_json(path: str):
    resp = kalshi_signed_get(path)
    try:
        data = resp.json()
    except:
        raise RuntimeError(f"Non-JSON response ({resp.status_code}): {resp.text}")

    if resp.status_code != 200:
        raise RuntimeError(
            f"Kalshi error {resp.status_code}:\n{json.dumps(data, indent=2)}"
        )

    return data


# ============================================================
# MARKET FETCHING (v3 only)
# ============================================================

def fetch_cfp_markets():
    """Fetch all CFP markets using v3 search API."""
    data = kalshi_json(f"/markets?search={CFP_PREFIX}")
    markets = data.get("markets", [])

    # Log sample of what we're getting
    print(f"RAW_RESPONSE_COUNT={len(markets)}")

    # Keep only real CFP yes/no markets
    cfp = []
    for m in markets:
        t = m.get("ticker", "")
        if t.startswith(CFP_PREFIX) and m.get("yes_price") is not None:
            cfp.append(m)

    print(f"FOUND_CFP_MARKETS={len(cfp)}")
    return cfp


def fetch_market_details(ticker: str):
    """Fetch a single market by ticker."""
    data = kalshi_json(f"/market?ticker={ticker}")
    return data.get("market", {})


# ============================================================
# POLLER LOOP
# ============================================================

prev_prices = {}

def poll_once():
    global prev_prices

    now = datetime.now(timezone.utc)

    markets = fetch_cfp_markets()
    if not markets:
        print(f"{now.isoformat()} | No CFP markets found in v3 search")
        return

    batch = db.batch()
    movers = []

    for m in markets:
        t = m["ticker"]
        title = m.get("title", t)
        yes_price = m.get("yes_price")
        no_price = m.get("no_price")
        volume = m.get("volume")

        # ------------------------
        # Store snapshot
        # ------------------------
        doc_ref = db.collection("cfp_markets").document(t)
        batch.set(
            doc_ref,
            {
                "ticker": t,
                "team": title,
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": volume,
                "updated_at": now,
            },
            merge=True
        )

        # ------------------------
        # Movement detection
        # ------------------------
        prev = prev_prices.get(t)
        if prev is not None and yes_price is not None and yes_price != prev:
            diff = yes_price - prev

            movers.append({
                "ticker": t,
                "team": title,
                "prev_yes": prev,
                "curr_yes": yes_price,
                "diff": diff,
                "significant": abs(diff) >= MIN_MOVE,
                "detected_at": now,
            })

        if yes_price is not None:
            prev_prices[t] = yes_price

    batch.commit()

    # Store movers
    for mv in movers:
        db.collection("cfp_movers").add(mv)

    print(
        f"{now.isoformat()} | tickers={len(markets)} "
        f"| movers={len(movers)} "
        f"| examples={movers[:2]}"
    )


# ============================================================
# MAIN LOOP
# ============================================================

if __name__ == "__main__":
    print("=== CFP WORKER STARTED (v3 API) ===")
    while True:
        try:
            poll_once()
        except Exception as e:
            print("ERROR in poller:", e)
        time.sleep(POLL_INTERVAL)

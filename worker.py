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

# ==========================
# CONFIG
# ==========================

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM")

# MUST BE https://api.elections.kalshi.com/trade-api/v2
BASE_URL = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2"
)

# CFP prefix — WE NOW SEARCH BY PREFIX, NOT EVENT GROUPING
CFP_EVENT_TICKER = "KXNCAAFPLAYOFF-25"

MIN_MOVE = float(os.getenv("MIN_MOVE", "0.5"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))

FIREBASE_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

if not API_KEY_ID:
    raise RuntimeError("KALSHI_API_KEY_ID not set.")
if not PRIVATE_KEY_PEM:
    raise RuntimeError("KALSHI_PRIVATE_KEY_PEM not set.")
if not FIREBASE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON not set.")


# ==========================
# FIRESTORE
# ==========================

service_account_info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
firebase_creds = service_account.Credentials.from_service_account_info(service_account_info)
db = firestore.Client(project=service_account_info["project_id"], credentials=firebase_creds)


# ==========================
# KALSHI SIGNING
# ==========================

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


def kalshi_get_json(path: str):
    resp = kalshi_signed_get(path)
    try:
        data = resp.json()
    except:
        raise RuntimeError(f"Non-JSON ({resp.status_code}): {resp.text[:200]}")
    if resp.status_code != 200:
        raise RuntimeError(
            f"Kalshi returned {resp.status_code}:\n{json.dumps(data, indent=2)}"
        )
    return data


# ==========================
# MARKET FETCHING (FIXED)
# ==========================

def fetch_cfp_market_tickers():
    """
    Fetch CFP markets using prefix search, because Kalshi does NOT attach
    these markets to the event group. Your Colab uses these tickers:
    KXNCAAFPLAYOFF-25-OSU, KXNCAAFPLAYOFF-25-ND, etc.
    """
    path = f"/markets?search={CFP_EVENT_TICKER}-"
    data = kalshi_get_json(path)

    # DEBUG: Show raw response
    print("RAW_MARKETS_RESPONSE:", json.dumps(data, indent=2)[:800])

    markets = data.get("markets", [])

    # CFP markets have YES/NO prices.
    tickers = [
        m["ticker"]
        for m in markets
        if m.get("yes_price") is not None
    ]

    print(f"FOUND {len(markets)} total entries, {len(tickers)} real CFP markets")
    return tickers


def fetch_market(ticker: str):
    data = kalshi_get_json(f"/market?ticker={ticker}")
    return data.get("market", {})


# ==========================
# POLLING LOOP
# ==========================

prev_prices = {}

def process_once():
    global prev_prices

    tickers = fetch_cfp_market_tickers()
    now = datetime.now(timezone.utc)

    batch = db.batch()
    movers = []
    samples = []

    for t in tickers:
        m = fetch_market(t)

        yes_price = m.get("yes_price")
        no_price = m.get("no_price")
        title = m.get("title", t)
        volume = m.get("volume")

        # Save market snapshot
        doc_ref = db.collection("cfp_markets").document(t)
        batch.set(
            doc_ref,
            {
                "ticker": t,
                "team": title,
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": volume,
                "event_ticker": CFP_EVENT_TICKER,
                "updated_at": now,
            },
            merge=True,
        )

        # Detect movement
        prev = prev_prices.get(t)
        if prev is not None and yes_price is not None and yes_price != prev:
            diff = yes_price - prev
            if len(samples) < 5:
                samples.append(f"{t}: {prev} → {yes_price}")

            movers.append({
                "ticker": t,
                "team": title,
                "prev_yes": prev,
                "curr_yes": yes_price,
                "diff": diff,
                "significant": abs(diff) >= MIN_MOVE,
                "detected_at": now,
                "event_ticker": CFP_EVENT_TICKER,
            })

        if yes_price is not None:
            prev_prices[t] = yes_price

    batch.commit()

    for mv in movers:
        db.collection("cfp_movers").add(mv)

    print(
        f"{now.isoformat()} | tickers={len(tickers)} "
        f"| movers={len(movers)} "
        f"| samples={samples or 'none'}"
    )


if __name__ == "__main__":
    while True:
        try:
            process_once()
        except Exception as e:
            print("Error:", e)
        time.sleep(POLL_INTERVAL)

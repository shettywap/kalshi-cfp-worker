import os
import time
import json
import base64
from datetime import datetime, timezone

import requests
from google.cloud import firestore
from google.oauth2 import service_account
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ==========================
#  ENV / CONFIG
# ==========================

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM")

# IMPORTANT: this *must* be the Elections API base. It includes sports.
BASE_URL = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
)

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
#  FIRESTORE CLIENT
# ==========================

service_account_info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
firebase_creds = service_account.Credentials.from_service_account_info(
    service_account_info
)
db = firestore.Client(
    project=service_account_info["project_id"],
    credentials=firebase_creds,
)


# ==========================
#  KALSHI SIGNING
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

    resp = requests.get(url, headers=headers, timeout=10)
    return resp


def kalshi_get_json(path: str):
    resp = kalshi_signed_get(path)
    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(f"Non-JSON ({resp.status_code}): {resp.text[:200]}")
    if resp.status_code != 200:
        raise RuntimeError(
            f"Kalshi returned {resp.status_code} for {path}:\n"
            + json.dumps(data, indent=2)
        )
    return data


# ==========================
#  EVENT → MARKET TICKERS
# ==========================

def fetch_cfp_market_tickers():
    data = kalshi_get_json(f"/event?ticker={CFP_EVENT_TICKER}")
    return data["event"].get("markets", [])


def fetch_cfp_markets():
    tickers = fetch_cfp_market_tickers()
    markets = []
    for t in tickers:
        md = kalshi_get_json(f"/market?ticker={t}")
        market = md.get("market", {})
        markets.append(market)
    return markets


# ==========================
#  POLLER LOOP
# ==========================

prev_prices = {}  # ticker -> last YES seen


def process_once():
    global prev_prices

    markets = fetch_cfp_markets()
    now = datetime.now(timezone.utc)

    batch = db.batch()
    movers_to_add = []

    changed_count = 0
    sample_changes = []

    for m in markets:
        ticker = m.get("ticker")
        title = m.get("title", ticker)
        yes_price = m.get("yes_price")
        no_price = m.get("no_price")
        volume = m.get("volume")

        # Update Firestore market doc
        doc_ref = db.collection("cfp_markets").document(ticker)
        batch.set(
            doc_ref,
            {
                "ticker": ticker,
                "team": title,
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": volume,
                "event_ticker": CFP_EVENT_TICKER,
                "updated_at": now,
            },
            merge=True,
        )

        # Movement detection
        prev = prev_prices.get(ticker)
        if prev is not None and yes_price is not None and yes_price != prev:
            diff = yes_price - prev
            changed_count += 1
            if len(sample_changes) < 5:
                sample_changes.append(f"{ticker}: {prev} → {yes_price} (diff={diff})")

            movers_to_add.append({
                "ticker": ticker,
                "team": title,
                "prev_yes": prev,
                "curr_yes": yes_price,
                "diff": diff,
                "significant": abs(diff) >= MIN_MOVE,
                "detected_at": now,
                "event_ticker": CFP_EVENT_TICKER,
            })

        if yes_price is not None:
            prev_prices[ticker] = yes_price

    batch.commit()

    for mv in movers_to_add:
        db.collection("cfp_movers").add(mv)

    print(
        f"{now.isoformat()} | markets: {len(markets)} | movers: {len(movers_to_add)} | "
        f"changed tickers: {changed_count} | samples: {sample_changes or 'none'}"
    )


if __name__ == "__main__":
    while True:
        try:
            process_once()
        except Exception as e:
            print("Error:", e)
        time.sleep(POLL_INTERVAL)

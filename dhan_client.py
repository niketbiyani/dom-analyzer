"""Dhan API client for fetching market depth (DOM) data."""

import time
import httpx
import pyotp
from config import (
    DHAN_CLIENT_ID,
    DHAN_ACCESS_TOKEN,
    DHAN_TOTP_SECRET,
    DHAN_SECURITY_ID,
    DHAN_EXCHANGE_SEGMENT,
)

BASE_URL = "https://api.dhan.co/v2"

# Map exchange segment strings to Dhan API values
EXCHANGE_MAP = {
    "NSE_FNO": "NSE_FNO",
    "NSE_EQ": "NSE_EQ",
    "BSE_FNO": "BSE_FNO",
    "BSE_EQ": "BSE_EQ",
    "MCX_COMM": "MCX_COMM",
}


def get_headers():
    return {
        "Content-Type": "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
    }


def generate_totp():
    """Generate TOTP code for Dhan authentication."""
    if not DHAN_TOTP_SECRET:
        return None
    totp = pyotp.TOTP(DHAN_TOTP_SECRET)
    return totp.now()


def fetch_market_depth():
    """
    Fetch 20-level market depth for the configured Nifty future.

    Returns dict with:
        - bids: list of {price, quantity, orders} sorted by price descending
        - asks: list of {price, quantity, orders} sorted by price ascending
        - ltp: last traded price
        - timestamp: server timestamp
    """
    url = f"{BASE_URL}/marketfeed/depth"

    payload = {
        "securityId": str(DHAN_SECURITY_ID),
        "exchangeSegment": EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT),
    }

    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return _parse_depth(data)
    except httpx.HTTPStatusError as e:
        # Fall back to depth endpoint via GET
        return _fetch_depth_fallback()
    except Exception as e:
        print(f"[DhanClient] Error fetching depth: {e}")
        return None


def _fetch_depth_fallback():
    """Fallback: use the quotes endpoint which includes 5-level depth."""
    url = f"{BASE_URL}/marketfeed/quote"

    payload = {
        "securityId": str(DHAN_SECURITY_ID),
        "exchangeSegment": EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT),
    }

    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return _parse_depth(data)
    except Exception as e:
        print(f"[DhanClient] Fallback error: {e}")
        return None


def _parse_depth(data):
    """Parse Dhan depth response into normalized format."""
    bids = []
    asks = []
    ltp = 0.0

    # Dhan v2 depth response structure
    if "data" in data:
        d = data["data"]
    else:
        d = data

    ltp = float(d.get("last_price", 0) or d.get("LTP", 0) or d.get("ltp", 0) or 0)

    # Parse depth levels - Dhan provides depth as arrays
    depth = d.get("depth", d)

    # Handle buy side
    buy_data = depth.get("buy", [])
    if isinstance(buy_data, list):
        for level in buy_data:
            price = float(level.get("price", 0))
            qty = int(level.get("quantity", 0))
            orders = int(level.get("orders", 0))
            if price > 0:
                bids.append({"price": price, "quantity": qty, "orders": orders})

    # Handle sell side
    sell_data = depth.get("sell", [])
    if isinstance(sell_data, list):
        for level in sell_data:
            price = float(level.get("price", 0))
            qty = int(level.get("quantity", 0))
            orders = int(level.get("orders", 0))
            if price > 0:
                asks.append({"price": price, "quantity": qty, "orders": orders})

    # Sort: bids descending, asks ascending
    bids.sort(key=lambda x: x["price"], reverse=True)
    asks.sort(key=lambda x: x["price"])

    return {
        "bids": bids,
        "asks": asks,
        "ltp": ltp,
        "timestamp": time.time(),
    }


def fetch_last_traded_price():
    """Fetch just the LTP for the configured instrument."""
    url = f"{BASE_URL}/marketfeed/ltp"
    payload = {
        "securityId": str(DHAN_SECURITY_ID),
        "exchangeSegment": EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT),
    }
    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        d = data.get("data", data)
        return float(d.get("last_price", 0) or d.get("LTP", 0) or d.get("ltp", 0) or 0)
    except Exception as e:
        print(f"[DhanClient] LTP error: {e}")
        return 0.0

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
    Fetch market depth for the configured Nifty future.

    Returns dict with:
        - bids: list of {price, quantity, orders} sorted by price descending
        - asks: list of {price, quantity, orders} sorted by price ascending
        - ltp: last traded price
        - timestamp: server timestamp
    """
    url = f"{BASE_URL}/marketfeed/quote"
    segment = EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT)

    payload = {
        segment: [DHAN_SECURITY_ID],
    }

    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return _parse_depth(data, segment)
    except Exception as e:
        print(f"[DhanClient] Error fetching depth: {e}")
        return None


def _parse_depth(data, segment="NSE_FNO"):
    """Parse Dhan v2 depth response into normalized format.

    Response structure: {"data":{"NSE_FNO":{"51714":{...}}},"status":"success"}
    """
    bids = []
    asks = []
    ltp = 0.0

    # Navigate: data -> segment -> security_id -> instrument data
    root = data.get("data", {})
    seg_data = root.get(segment, {})

    # Get the first (only) instrument in the response
    sec_id = str(DHAN_SECURITY_ID)
    d = seg_data.get(sec_id, {})

    if not d:
        return None

    ltp = float(d.get("last_price", 0))

    # Parse depth levels
    depth = d.get("depth", {})

    for level in depth.get("buy", []):
        price = float(level.get("price", 0))
        qty = int(level.get("quantity", 0))
        orders = int(level.get("orders", 0))
        if price > 0:
            bids.append({"price": price, "quantity": qty, "orders": orders})

    for level in depth.get("sell", []):
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
    segment = EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT)
    payload = {
        segment: [DHAN_SECURITY_ID],
    }
    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        seg_data = data.get("data", {}).get(segment, {})
        d = seg_data.get(str(DHAN_SECURITY_ID), {})
        return float(d.get("last_price", 0))
    except Exception as e:
        print(f"[DhanClient] LTP error: {e}")
        return 0.0

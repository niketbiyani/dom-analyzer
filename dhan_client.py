"""Dhan API client for fetching market depth (DOM) and historical candle data."""

import logging
import time
from datetime import datetime, timedelta

import httpx
import pyotp
from config import (
    DHAN_CLIENT_ID,
    DHAN_ACCESS_TOKEN,
    DHAN_TOTP_SECRET,
    DHAN_SECURITY_ID,
    DHAN_EXCHANGE_SEGMENT,
)

logger = logging.getLogger(__name__)

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


# --- Instrument type mapping for charts API ---
INSTRUMENT_MAP = {
    "NSE_FNO": "FUTIDX",
    "NSE_EQ": "EQUITY",
    "BSE_FNO": "FUTIDX",
    "BSE_EQ": "EQUITY",
    "MCX_COMM": "FUTCOM",
}


def fetch_intraday_candles(from_date: str, to_date: str, interval: str = "5"):
    """
    Fetch intraday candle data from Dhan charts API.

    Args:
        from_date: "YYYY-MM-DD HH:MM:SS" format
        to_date: "YYYY-MM-DD HH:MM:SS" format
        interval: "1", "5", "15", "25", or "60" (minutes)

    Returns:
        list of {open, high, low, close, volume, timestamp} dicts
    """
    url = f"{BASE_URL}/charts/intraday"
    segment = EXCHANGE_MAP.get(DHAN_EXCHANGE_SEGMENT, DHAN_EXCHANGE_SEGMENT)
    instrument = INSTRUMENT_MAP.get(DHAN_EXCHANGE_SEGMENT, "FUTIDX")

    payload = {
        "securityId": str(DHAN_SECURITY_ID),
        "exchangeSegment": segment,
        "instrument": instrument,
        "interval": str(interval),
        "fromDate": from_date,
        "toDate": to_date,
    }

    try:
        resp = httpx.post(url, json=payload, headers=get_headers(), timeout=10.0)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "success":
            candles = data.get("data", [])
            logger.info(
                "[DhanClient] Fetched %d candles (%s to %s, %sm interval)",
                len(candles), from_date, to_date, interval,
            )
            return candles
        else:
            logger.warning("[DhanClient] Candle API returned: %s", data)
            return []
    except Exception as e:
        logger.error("[DhanClient] Error fetching candles: %s", e)
        return []


def fetch_historical_candles(days: int = 3, interval: str = "5"):
    """
    Fetch last N trading days of intraday candles.

    Args:
        days: number of calendar days to look back (use 5 for ~3 trading days)
        interval: candle interval in minutes

    Returns:
        list of candle dicts sorted by timestamp
    """
    if not DHAN_ACCESS_TOKEN:
        logger.info("[DhanClient] No access token, skipping historical candle fetch")
        return []

    now = datetime.now()
    # Go back extra days to account for weekends/holidays
    from_dt = now - timedelta(days=days + 2)

    from_str = from_dt.strftime("%Y-%m-%d 09:15:00")
    to_str = now.strftime("%Y-%m-%d %H:%M:%S")

    candles = fetch_intraday_candles(from_str, to_str, interval)

    if not candles:
        logger.info("[DhanClient] No historical candles returned")
        return []

    # Normalize: ensure all candles have expected fields
    result = []
    for c in candles:
        try:
            result.append({
                "open": float(c.get("open", 0)),
                "high": float(c.get("high", 0)),
                "low": float(c.get("low", 0)),
                "close": float(c.get("close", 0)),
                "volume": int(c.get("volume", 0)),
                "timestamp": float(c.get("timestamp", 0)),
            })
        except (ValueError, TypeError):
            continue

    result.sort(key=lambda x: x["timestamp"])
    logger.info("[DhanClient] Returning %d historical candles spanning %d days", len(result), days)
    return result

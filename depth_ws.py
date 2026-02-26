"""
WebSocket client for Dhan 20-level depth feed.

Connects to wss://depth-api-feed.dhan.co/twentydepth, subscribes to
the configured instrument, parses binary depth packets, and exposes
snapshots in the same format that analytics.py expects.

Adapted from the working risk-management DhanDepthAnalyzer.
"""

import asyncio
import json
import logging
import struct
import time

import websockets

from config import (
    DHAN_ACCESS_TOKEN,
    DHAN_CLIENT_ID,
    DHAN_SECURITY_ID,
    DHAN_EXCHANGE_SEGMENT,
)

logger = logging.getLogger(__name__)

DEPTH_WS_URL = "wss://depth-api-feed.dhan.co/twentydepth"

# Binary feed codes from Dhan
FEED_CODE_BID = 41
FEED_CODE_ASK = 51


class DepthWebSocket:
    """20-level depth WebSocket client with auto-reconnection."""

    def __init__(self):
        self._connected = False
        self._ws = None

        # Order book state (20 levels each side)
        self._bids = [{"price": 0.0, "quantity": 0, "orders": 0} for _ in range(20)]
        self._asks = [{"price": 0.0, "quantity": 0, "orders": 0} for _ in range(20)]
        self._last_update = 0.0

        # Callback for new data
        self._on_update = None

    @property
    def connected(self):
        return self._connected

    def on_update(self, callback):
        """Register a callback: async def callback(snapshot)"""
        self._on_update = callback

    def get_snapshot(self):
        """Return current order book in analytics-compatible format."""
        bids = [b.copy() for b in self._bids if b["price"] > 0]
        asks = [a.copy() for a in self._asks if a["price"] > 0]

        ltp = 0.0
        if bids and asks:
            ltp = round((bids[0]["price"] + asks[0]["price"]) / 2, 2)

        return {
            "bids": bids,
            "asks": asks,
            "ltp": ltp,
            "timestamp": self._last_update or time.time(),
        }

    async def run(self):
        """Main connection loop with auto-reconnect."""
        while True:
            try:
                if not DHAN_ACCESS_TOKEN or not DHAN_CLIENT_ID:
                    logger.warning("[DEPTH-WS] No credentials, waiting 10s...")
                    await asyncio.sleep(10)
                    continue

                url = (
                    f"{DEPTH_WS_URL}"
                    f"?token={DHAN_ACCESS_TOKEN}"
                    f"&clientId={DHAN_CLIENT_ID}"
                    f"&authType=2"
                )

                logger.info("[DEPTH-WS] Connecting to 20-level depth feed...")

                async with websockets.connect(
                    url, ping_interval=30, ping_timeout=10
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    logger.info("[DEPTH-WS] Connected!")

                    # Subscribe
                    sub_msg = {
                        "RequestCode": 23,
                        "InstrumentCount": 1,
                        "InstrumentList": [{
                            "ExchangeSegment": DHAN_EXCHANGE_SEGMENT,
                            "SecurityId": str(DHAN_SECURITY_ID),
                        }],
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.info("[DEPTH-WS] Subscribed to security %s", DHAN_SECURITY_ID)

                    # Read loop
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            if isinstance(msg, bytes):
                                self._process_binary(msg)
                            else:
                                logger.debug("[DEPTH-WS] Text msg: %s", msg[:200])
                        except asyncio.TimeoutError:
                            continue
                        except websockets.ConnectionClosed:
                            logger.warning("[DEPTH-WS] Connection closed by server")
                            break

            except Exception as e:
                logger.error("[DEPTH-WS] Connection error: %s", e)

            self._connected = False
            self._ws = None
            logger.info("[DEPTH-WS] Reconnecting in 5s...")
            await asyncio.sleep(5)

    def _process_binary(self, data):
        """Parse one or more depth packets from a WebSocket frame."""
        offset = 0
        while offset < len(data):
            if offset + 12 > len(data):
                break

            # Header: msg_len(2) + feed_code(1) + exchange(1) + sec_id(4) + seq(4) = 12 bytes
            msg_len, feed_code, exchange_seg, sec_id, seq = struct.unpack_from(
                '<HBBII', data, offset
            )

            if msg_len == 0 or msg_len > len(data):
                self._parse_single_packet(data)
                break

            self._parse_single_packet(data[offset:offset + msg_len])
            offset += msg_len

            if offset >= len(data):
                break

    def _parse_single_packet(self, data):
        """Parse a single depth packet (bid or ask side, 20 levels)."""
        if len(data) < 12:
            return

        header = struct.unpack_from('<HBBII', data, 0)
        feed_code = header[1]
        payload = data[12:]

        levels = []
        for i in range(20):
            off = i * 16
            if off + 16 > len(payload):
                break
            # Each level: price(double/8 bytes) + qty(uint32/4) + orders(uint32/4) = 16 bytes
            price, qty, orders = struct.unpack_from('<dII', payload, off)
            levels.append({
                "price": round(price, 2),
                "quantity": qty,
                "orders": orders,
            })

        # Pad to 20 if fewer levels
        while len(levels) < 20:
            levels.append({"price": 0.0, "quantity": 0, "orders": 0})

        if feed_code == FEED_CODE_BID:
            self._bids = levels
        elif feed_code == FEED_CODE_ASK:
            self._asks = levels
        else:
            return

        self._last_update = time.time()

        # Fire callback
        if self._on_update:
            snapshot = self.get_snapshot()
            asyncio.ensure_future(self._on_update(snapshot))

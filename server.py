"""
DOM Analyzer Server

FastAPI backend with WebSocket for real-time DOM streaming.
Connects to Dhan's 20-level depth WebSocket feed and pushes analytics to frontend.
Falls back to REST polling or demo mode if WebSocket fails.
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from config import REFRESH_INTERVAL_MS, PORT, FYERS_SYMBOL, DHAN_SECURITY_ID, DHAN_ACCESS_TOKEN
from analytics import DOMAnalytics
from depth_ws import DepthWebSocket

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Global state
analytics = DOMAnalytics()
clients: set[WebSocket] = set()
depth_ws = DepthWebSocket()
demo_mode = False


async def broadcast(data: dict):
    """Send data to all connected WebSocket clients."""
    dead = set()
    for ws in clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


async def on_depth_update(snapshot: dict):
    """Called by DepthWebSocket when new depth data arrives."""
    if not snapshot or not snapshot.get("bids"):
        return

    analytics.process_snapshot(snapshot)

    payload = {
        "type": "dom_update",
        "snapshot": snapshot,
        "analytics": analytics.get_full_state(),
        "symbol": FYERS_SYMBOL,
    }

    await broadcast(payload)


def generate_demo_snapshot():
    """Generate realistic demo DOM data for testing without API."""
    import random
    import math

    base_price = 24500.0
    t = time.time()
    drift = math.sin(t / 30) * 50
    ltp = base_price + drift + random.uniform(-5, 5)
    ltp = round(ltp / 0.05) * 0.05

    bids = []
    asks = []

    for i in range(20):
        bid_price = round(ltp - (i + 1) * 0.05, 2)
        ask_price = round(ltp + (i + 1) * 0.05, 2)

        base_qty = random.randint(50, 300)
        bid_qty = base_qty
        ask_qty = base_qty

        if abs(bid_price - round(bid_price / 50) * 50) < 0.1:
            bid_qty = random.randint(800, 2000)
        if abs(ask_price - round(ask_price / 50) * 50) < 0.1:
            ask_qty = random.randint(800, 2000)
        if i == 3 and int(t) % 60 < 30:
            bid_qty = max(100, int(1500 * (1 - (t % 30) / 30)))

        bids.append({"price": bid_price, "quantity": bid_qty, "orders": random.randint(1, 20)})
        asks.append({"price": ask_price, "quantity": ask_qty, "orders": random.randint(1, 20)})

    return {
        "bids": bids,
        "asks": asks,
        "ltp": round(ltp, 2),
        "timestamp": t,
    }


async def demo_poll():
    """Fallback: push demo data when no Dhan credentials."""
    interval = REFRESH_INTERVAL_MS / 1000.0
    while True:
        try:
            snapshot = generate_demo_snapshot()
            analytics.process_snapshot(snapshot)
            payload = {
                "type": "dom_update",
                "snapshot": snapshot,
                "analytics": analytics.get_full_state(),
                "symbol": FYERS_SYMBOL,
            }
            await broadcast(payload)
        except Exception as e:
            logger.error("[Server] Demo poll error: %s", e)
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app):
    """Start depth feed on server startup."""
    global demo_mode

    if not DHAN_ACCESS_TOKEN:
        demo_mode = True
        logger.info("[Server] No DHAN_ACCESS_TOKEN — running in DEMO mode")
        task = asyncio.create_task(demo_poll())
    else:
        logger.info("[Server] Starting 20-level depth WebSocket feed...")
        depth_ws.on_update(on_depth_update)
        task = asyncio.create_task(depth_ws.run())

    yield

    task.cancel()


app = FastAPI(title="DOM Analyzer", lifespan=lifespan)

# Serve static files
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(static_dir / "index.html"))


@app.get("/api/config")
async def get_config():
    return {
        "symbol": FYERS_SYMBOL,
        "security_id": DHAN_SECURITY_ID,
        "refresh_ms": REFRESH_INTERVAL_MS,
        "demo_mode": demo_mode,
        "depth_connected": depth_ws.connected,
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=True)

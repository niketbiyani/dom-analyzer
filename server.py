"""
DOM Analyzer Server

FastAPI backend with WebSocket for real-time DOM streaming.
Polls Dhan API for market depth and pushes analytics to frontend.
"""

import asyncio
import json
import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from config import REFRESH_INTERVAL_MS, PORT, FYERS_SYMBOL, DHAN_SECURITY_ID
from dhan_client import fetch_market_depth
from analytics import DOMAnalytics

app = FastAPI(title="DOM Analyzer")

# Serve static files
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Global analytics engine
analytics = DOMAnalytics()

# Connected WebSocket clients
clients: set[WebSocket] = set()

# Demo mode flag (when Dhan API not configured)
demo_mode = False


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
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            # Keep connection alive, handle any client messages
            data = await ws.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


async def broadcast(data: dict):
    """Send data to all connected WebSocket clients."""
    dead = set()
    for ws in clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


def generate_demo_snapshot():
    """Generate realistic demo DOM data for testing without API."""
    import random
    import math

    base_price = 24500.0
    t = time.time()
    # Slow drift
    drift = math.sin(t / 30) * 50
    ltp = base_price + drift + random.uniform(-5, 5)
    ltp = round(ltp / 0.05) * 0.05  # Nifty tick size

    bids = []
    asks = []

    for i in range(20):
        bid_price = round(ltp - (i + 1) * 0.05, 2)
        ask_price = round(ltp + (i + 1) * 0.05, 2)

        # Base quantity with some randomness
        base_qty = random.randint(50, 300)

        # Inject walls at certain levels
        bid_qty = base_qty
        ask_qty = base_qty

        # Big bid wall at round number below
        if abs(bid_price - round(bid_price / 50) * 50) < 0.1:
            bid_qty = random.randint(800, 2000)

        # Big ask wall at round number above
        if abs(ask_price - round(ask_price / 50) * 50) < 0.1:
            ask_qty = random.randint(800, 2000)

        # Simulate absorption: walls that slowly drain
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


async def poll_depth():
    """Background task: poll market depth and push to clients."""
    global demo_mode

    from config import DHAN_ACCESS_TOKEN
    if not DHAN_ACCESS_TOKEN:
        demo_mode = True
        print("[Server] No DHAN_ACCESS_TOKEN set — running in DEMO mode with simulated data")

    interval = REFRESH_INTERVAL_MS / 1000.0

    while True:
        try:
            if demo_mode:
                snapshot = generate_demo_snapshot()
            else:
                snapshot = fetch_market_depth()

            if snapshot:
                # Process through analytics engine
                analytics.process_snapshot(snapshot)

                # Build payload
                payload = {
                    "type": "dom_update",
                    "snapshot": snapshot,
                    "analytics": analytics.get_full_state(),
                    "symbol": FYERS_SYMBOL,
                }

                await broadcast(payload)

        except Exception as e:
            print(f"[Server] Poll error: {e}")

        await asyncio.sleep(interval)


@app.on_event("startup")
async def startup():
    asyncio.create_task(poll_depth())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=True)

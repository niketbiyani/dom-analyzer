# DOM Analyzer — Nifty Futures

Real-time Depth of Market (DOM) analyzer for Nifty Futures trading on NSE.
Connects to Dhan API for market depth data and provides advanced order flow analytics.

## Architecture

- **Backend**: Python + FastAPI with WebSocket streaming
- **Frontend**: Single-page HTML/JS app with canvas charts (no framework dependencies)
- **Data Source**: Dhan API v2 (market depth endpoint)

## Features

1. **DOM Heatmap** — Color-coded bid/ask depth with visual quantity bars
2. **Wall Detection** — Automatically highlights price levels with abnormally large quantity (configurable threshold, default 3x average)
3. **Absorption Alerts** — Tracks walls over time; detects whether they're holding (absorbing) or breaking (draining)
4. **Pulling/Stacking Detection** — Detects large orders appearing (stacking) or disappearing (pulling/spoofing)
5. **Cumulative Delta** — Running buy vs sell pressure indicator with trend signal
6. **Tick-wise Volume** — Bookmap-style buyer/seller volume estimation from order book changes
7. **Alerts Log** — Consolidated feed of all detected events

## Setup

```bash
cp .env.example .env
# Edit .env with your Dhan credentials
pip install -r requirements.txt
python server.py
```

Runs on http://localhost:8000. Without Dhan credentials, runs in demo mode with simulated data.

## Key Files

- `server.py` — FastAPI server, WebSocket streaming, demo data generator
- `dhan_client.py` — Dhan API integration for market depth
- `analytics.py` — Core analytics engine (walls, absorption, delta, tick volume)
- `config.py` — Environment variable configuration
- `static/index.html` — Complete frontend (DOM heatmap, charts, alerts)

## Symbol Maintenance

Update `DHAN_SECURITY_ID` and `FYERS_SYMBOL` in `.env` when the near-month Nifty future expires:
- March: `NIFTY26MARFUT` / security ID from Dhan instrument list
- April: `NIFTY26APRFUT` / etc.

## Configuration (.env)

| Variable | Description | Default |
|---|---|---|
| `DHAN_CLIENT_ID` | Dhan client ID | — |
| `DHAN_ACCESS_TOKEN` | Dhan API access token | — |
| `DHAN_TOTP_SECRET` | TOTP secret for auth | — |
| `DHAN_SECURITY_ID` | Nifty future security ID | 49081 |
| `REFRESH_INTERVAL_MS` | DOM polling interval (ms) | 500 |
| `WALL_THRESHOLD` | Wall detection multiplier | 3.0 |
| `ABSORPTION_TICKS` | Min ticks before absorption alert | 10 |

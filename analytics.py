"""
DOM Analytics Engine

Tracks order book over time and computes:
- Wall detection (support/resistance)
- Absorption alerts
- Pulling/stacking detection
- Cumulative delta
- Tick-wise buyer/seller volume
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from config import WALL_THRESHOLD, ABSORPTION_TICKS


@dataclass
class WallLevel:
    """A detected wall (large qty at a price level)."""
    price: float
    side: str  # "bid" or "ask"
    quantity: int
    avg_quantity: float
    ratio: float  # qty / avg_qty
    first_seen: float
    last_seen: float
    history: list = field(default_factory=list)  # [(timestamp, qty), ...]


@dataclass
class AbsorptionAlert:
    """Alert when a wall is absorbing or draining."""
    price: float
    side: str
    alert_type: str  # "absorbing" (holding strong) or "draining" (breaking)
    initial_qty: int
    current_qty: int
    pct_remaining: float
    ticks_tracked: int
    timestamp: float


@dataclass
class PullStackEvent:
    """Pulling or stacking event at a price level."""
    price: float
    side: str
    event_type: str  # "pulling" or "stacking"
    qty_change: int
    old_qty: int
    new_qty: int
    timestamp: float


@dataclass
class TickVolume:
    """Single tick volume record."""
    timestamp: float
    price: float
    buy_volume: int
    sell_volume: int
    delta: int  # buy - sell


class DOMAnalytics:
    """Main analytics engine that processes DOM snapshots over time."""

    def __init__(self, wall_threshold: float = WALL_THRESHOLD, max_history: int = 5000):
        self.wall_threshold = wall_threshold
        self.max_history = max_history

        # Time-series storage of DOM snapshots
        self.snapshots = deque(maxlen=max_history)

        # Previous snapshot for delta computation
        self.prev_snapshot = None

        # Wall tracking: price -> WallLevel
        self.bid_walls: dict[float, WallLevel] = {}
        self.ask_walls: dict[float, WallLevel] = {}

        # Active absorption alerts
        self.absorption_alerts: list[AbsorptionAlert] = []

        # Recent pull/stack events (keep last 50)
        self.pull_stack_events = deque(maxlen=50)

        # Cumulative delta tracking
        self.cumulative_delta = 0
        self.delta_history = deque(maxlen=max_history)  # [(timestamp, cum_delta)]

        # Tick-wise volume (Bookmap-style)
        self.tick_volumes = deque(maxlen=max_history)

        # For detecting trades from book changes
        self._prev_bids = {}  # price -> qty
        self._prev_asks = {}  # price -> qty
        self._prev_ltp = 0.0

    def process_snapshot(self, snapshot: dict):
        """
        Process a new DOM snapshot.

        Args:
            snapshot: {bids: [...], asks: [...], ltp: float, timestamp: float}
        """
        if not snapshot:
            return

        ts = snapshot["timestamp"]
        bids = snapshot["bids"]
        asks = snapshot["asks"]
        ltp = snapshot["ltp"]

        # Store snapshot
        self.snapshots.append(snapshot)

        # Build current price->qty maps
        curr_bids = {b["price"]: b["quantity"] for b in bids}
        curr_asks = {a["price"]: a["quantity"] for a in asks}

        # --- Wall Detection ---
        self._detect_walls(bids, asks, ts)

        # --- Absorption Detection ---
        self._detect_absorption(curr_bids, curr_asks, ts)

        # --- Pulling/Stacking ---
        if self._prev_bids or self._prev_asks:
            self._detect_pull_stack(curr_bids, curr_asks, ts)

        # --- Tick Volume & Delta ---
        self._compute_tick_volume(curr_bids, curr_asks, ltp, ts)

        # Update previous state
        self._prev_bids = curr_bids
        self._prev_asks = curr_asks
        self._prev_ltp = ltp
        self.prev_snapshot = snapshot

    def _detect_walls(self, bids: list, asks: list, ts: float):
        """Detect price levels with abnormally large quantity."""
        # Compute average quantities
        bid_qtys = [b["quantity"] for b in bids if b["quantity"] > 0]
        ask_qtys = [a["quantity"] for a in asks if a["quantity"] > 0]

        avg_bid = sum(bid_qtys) / len(bid_qtys) if bid_qtys else 1
        avg_ask = sum(ask_qtys) / len(ask_qtys) if ask_qtys else 1

        # Detect bid walls
        current_bid_walls = set()
        for b in bids:
            ratio = b["quantity"] / avg_bid if avg_bid > 0 else 0
            if ratio >= self.wall_threshold:
                price = b["price"]
                current_bid_walls.add(price)
                if price in self.bid_walls:
                    # Update existing wall
                    wall = self.bid_walls[price]
                    wall.quantity = b["quantity"]
                    wall.ratio = ratio
                    wall.last_seen = ts
                    wall.history.append((ts, b["quantity"]))
                    # Keep history manageable
                    if len(wall.history) > 100:
                        wall.history = wall.history[-100:]
                else:
                    # New wall
                    self.bid_walls[price] = WallLevel(
                        price=price,
                        side="bid",
                        quantity=b["quantity"],
                        avg_quantity=avg_bid,
                        ratio=ratio,
                        first_seen=ts,
                        last_seen=ts,
                        history=[(ts, b["quantity"])],
                    )

        # Remove stale bid walls
        stale = [p for p in self.bid_walls if p not in current_bid_walls]
        for p in stale:
            del self.bid_walls[p]

        # Detect ask walls
        current_ask_walls = set()
        for a in asks:
            ratio = a["quantity"] / avg_ask if avg_ask > 0 else 0
            if ratio >= self.wall_threshold:
                price = a["price"]
                current_ask_walls.add(price)
                if price in self.ask_walls:
                    wall = self.ask_walls[price]
                    wall.quantity = a["quantity"]
                    wall.ratio = ratio
                    wall.last_seen = ts
                    wall.history.append((ts, a["quantity"]))
                    if len(wall.history) > 100:
                        wall.history = wall.history[-100:]
                else:
                    self.ask_walls[price] = WallLevel(
                        price=price,
                        side="ask",
                        quantity=a["quantity"],
                        avg_quantity=avg_ask,
                        ratio=ratio,
                        first_seen=ts,
                        last_seen=ts,
                        history=[(ts, a["quantity"])],
                    )

        stale = [p for p in self.ask_walls if p not in current_ask_walls]
        for p in stale:
            del self.ask_walls[p]

    def _detect_absorption(self, curr_bids: dict, curr_asks: dict, ts: float):
        """
        Detect absorption: wall being hit but holding vs draining.
        Only fires after tracking a wall for enough ticks.
        """
        self.absorption_alerts = []

        for price, wall in self.bid_walls.items():
            if len(wall.history) >= ABSORPTION_TICKS:
                initial_qty = wall.history[0][1]
                current_qty = wall.quantity
                if initial_qty > 0:
                    pct = (current_qty / initial_qty) * 100
                    if pct >= 80:
                        alert_type = "absorbing"  # Wall holding strong
                    elif pct <= 40:
                        alert_type = "draining"  # Wall breaking down
                    else:
                        continue

                    self.absorption_alerts.append(AbsorptionAlert(
                        price=price,
                        side="bid",
                        alert_type=alert_type,
                        initial_qty=initial_qty,
                        current_qty=current_qty,
                        pct_remaining=pct,
                        ticks_tracked=len(wall.history),
                        timestamp=ts,
                    ))

        for price, wall in self.ask_walls.items():
            if len(wall.history) >= ABSORPTION_TICKS:
                initial_qty = wall.history[0][1]
                current_qty = wall.quantity
                if initial_qty > 0:
                    pct = (current_qty / initial_qty) * 100
                    if pct >= 80:
                        alert_type = "absorbing"
                    elif pct <= 40:
                        alert_type = "draining"
                    else:
                        continue

                    self.absorption_alerts.append(AbsorptionAlert(
                        price=price,
                        side="ask",
                        alert_type=alert_type,
                        initial_qty=initial_qty,
                        current_qty=current_qty,
                        pct_remaining=pct,
                        ticks_tracked=len(wall.history),
                        timestamp=ts,
                    ))

    def _detect_pull_stack(self, curr_bids: dict, curr_asks: dict, ts: float):
        """Detect pulling (large orders disappearing) and stacking (large orders appearing)."""
        # Check bids
        all_bid_qtys = list(curr_bids.values()) + list(self._prev_bids.values())
        avg_bid = sum(all_bid_qtys) / len(all_bid_qtys) if all_bid_qtys else 1
        threshold = avg_bid * 2  # Significant change threshold

        for price in set(list(curr_bids.keys()) + list(self._prev_bids.keys())):
            old_qty = self._prev_bids.get(price, 0)
            new_qty = curr_bids.get(price, 0)
            change = new_qty - old_qty

            if abs(change) >= threshold:
                if change > 0:
                    event_type = "stacking"
                else:
                    event_type = "pulling"

                self.pull_stack_events.append(PullStackEvent(
                    price=price,
                    side="bid",
                    event_type=event_type,
                    qty_change=change,
                    old_qty=old_qty,
                    new_qty=new_qty,
                    timestamp=ts,
                ))

        # Check asks
        all_ask_qtys = list(curr_asks.values()) + list(self._prev_asks.values())
        avg_ask = sum(all_ask_qtys) / len(all_ask_qtys) if all_ask_qtys else 1
        threshold = avg_ask * 2

        for price in set(list(curr_asks.keys()) + list(self._prev_asks.keys())):
            old_qty = self._prev_asks.get(price, 0)
            new_qty = curr_asks.get(price, 0)
            change = new_qty - old_qty

            if abs(change) >= threshold:
                if change > 0:
                    event_type = "stacking"
                else:
                    event_type = "pulling"

                self.pull_stack_events.append(PullStackEvent(
                    price=price,
                    side="ask",
                    event_type=event_type,
                    qty_change=change,
                    old_qty=old_qty,
                    new_qty=new_qty,
                    timestamp=ts,
                ))

    def _compute_tick_volume(self, curr_bids: dict, curr_asks: dict, ltp: float, ts: float):
        """
        Estimate tick-wise buyer/seller volume from order book changes.

        Logic:
        - If bid qty at a level decreased and price >= that level → someone sold into the bid (sell volume)
        - If ask qty at a level decreased and price <= that level → someone bought from the ask (buy volume)
        - LTP movement helps disambiguate: uptick = buy, downtick = sell
        """
        buy_vol = 0
        sell_vol = 0

        if not self._prev_bids and not self._prev_asks:
            self._prev_ltp = ltp
            return

        # Estimate sells: bid qty decreased (someone hit the bid)
        for price, old_qty in self._prev_bids.items():
            new_qty = curr_bids.get(price, 0)
            if new_qty < old_qty:
                consumed = old_qty - new_qty
                sell_vol += consumed

        # Estimate buys: ask qty decreased (someone lifted the ask)
        for price, old_qty in self._prev_asks.items():
            new_qty = curr_asks.get(price, 0)
            if new_qty < old_qty:
                consumed = old_qty - new_qty
                buy_vol += consumed

        # Use LTP direction as tiebreaker when both sides change
        if ltp > self._prev_ltp and self._prev_ltp > 0:
            # Uptick — lean towards buy
            if buy_vol == 0 and sell_vol == 0:
                buy_vol = 1  # Minimal tick
        elif ltp < self._prev_ltp and self._prev_ltp > 0:
            # Downtick — lean towards sell
            if buy_vol == 0 and sell_vol == 0:
                sell_vol = 1

        delta = buy_vol - sell_vol
        self.cumulative_delta += delta

        tick = TickVolume(
            timestamp=ts,
            price=ltp,
            buy_volume=buy_vol,
            sell_volume=sell_vol,
            delta=delta,
        )
        self.tick_volumes.append(tick)
        self.delta_history.append((ts, self.cumulative_delta))

    # --- Public getters for API ---

    def get_walls(self) -> dict:
        """Get all current walls."""
        return {
            "bid_walls": [
                {
                    "price": w.price,
                    "quantity": w.quantity,
                    "ratio": round(w.ratio, 2),
                    "age_seconds": round(time.time() - w.first_seen, 1),
                    "history": w.history[-20:],  # Last 20 data points
                }
                for w in sorted(self.bid_walls.values(), key=lambda x: x.price, reverse=True)
            ],
            "ask_walls": [
                {
                    "price": w.price,
                    "quantity": w.quantity,
                    "ratio": round(w.ratio, 2),
                    "age_seconds": round(time.time() - w.first_seen, 1),
                    "history": w.history[-20:],
                }
                for w in sorted(self.ask_walls.values(), key=lambda x: x.price)
            ],
        }

    def get_absorption_alerts(self) -> list:
        return [
            {
                "price": a.price,
                "side": a.side,
                "type": a.alert_type,
                "initial_qty": a.initial_qty,
                "current_qty": a.current_qty,
                "pct_remaining": round(a.pct_remaining, 1),
                "ticks_tracked": a.ticks_tracked,
            }
            for a in self.absorption_alerts
        ]

    def get_pull_stack_events(self) -> list:
        return [
            {
                "price": e.price,
                "side": e.side,
                "type": e.event_type,
                "qty_change": e.qty_change,
                "old_qty": e.old_qty,
                "new_qty": e.new_qty,
                "timestamp": e.timestamp,
            }
            for e in list(self.pull_stack_events)[-20:]  # Last 20 events
        ]

    def get_cumulative_delta(self) -> dict:
        return {
            "current": self.cumulative_delta,
            "history": list(self.delta_history)[-500:],
        }

    def get_tick_volumes(self) -> list:
        return [
            {
                "timestamp": t.timestamp,
                "price": t.price,
                "buy_volume": t.buy_volume,
                "sell_volume": t.sell_volume,
                "delta": t.delta,
            }
            for t in list(self.tick_volumes)[-500:]
        ]

    def get_full_state(self, snapshot: Optional[dict] = None) -> dict:
        """Get complete analytics state for the frontend."""
        return {
            "walls": self.get_walls(),
            "absorption": self.get_absorption_alerts(),
            "pull_stack": self.get_pull_stack_events(),
            "cumulative_delta": self.get_cumulative_delta(),
            "tick_volumes": self.get_tick_volumes(),
        }

    def restore(self, cumulative_delta: int, tick_volumes: list, delta_history: list):
        """Restore state from persistent storage after server restart."""
        self.cumulative_delta = cumulative_delta
        for tv in tick_volumes:
            self.tick_volumes.append(TickVolume(
                timestamp=tv["timestamp"],
                price=tv["price"],
                buy_volume=tv["buy_volume"],
                sell_volume=tv["sell_volume"],
                delta=tv["delta"],
            ))
        for dh in delta_history:
            self.delta_history.append(dh)

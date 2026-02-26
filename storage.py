"""SQLite persistence for DOM analytics data.

Stores bookmap frames, tick volumes, delta history, and events
so data survives server restarts and client reconnections.
"""

import json
import sqlite3
import time
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "dom_data.db"


class Storage:
    """Thread-safe SQLite storage with buffered writes."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        self._write_buffer = []
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS bookmap_frames (
                    timestamp REAL PRIMARY KEY,
                    ltp REAL NOT NULL,
                    bids TEXT NOT NULL,
                    asks TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tick_volumes (
                    timestamp REAL PRIMARY KEY,
                    price REAL NOT NULL,
                    buy_volume INTEGER NOT NULL,
                    sell_volume INTEGER NOT NULL,
                    delta INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS delta_history (
                    timestamp REAL PRIMARY KEY,
                    cumulative_delta INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ev_ts ON events(timestamp);

                CREATE TABLE IF NOT EXISTS candle_history (
                    timestamp REAL PRIMARY KEY,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL
                );
            """)
        logger.info("[Storage] SQLite initialized at %s", self.db_path)

    def buffer_snapshot(self, snapshot, tick_volume=None, cum_delta=None, events=None):
        """Buffer data for batch writing."""
        with self._lock:
            self._write_buffer.append({
                "snapshot": snapshot,
                "tick_volume": tick_volume,
                "cum_delta": cum_delta,
                "events": events or [],
            })

    def flush(self):
        """Flush buffered data to SQLite in a single transaction."""
        with self._lock:
            buffer = self._write_buffer[:]
            self._write_buffer.clear()

        if not buffer:
            return

        try:
            with self._conn() as conn:
                for item in buffer:
                    s = item["snapshot"]
                    if s:
                        conn.execute(
                            "INSERT OR REPLACE INTO bookmap_frames (timestamp, ltp, bids, asks) VALUES (?, ?, ?, ?)",
                            (
                                s["timestamp"], s["ltp"],
                                json.dumps([[b["price"], b["quantity"]] for b in s["bids"]]),
                                json.dumps([[a["price"], a["quantity"]] for a in s["asks"]]),
                            )
                        )

                    tv = item["tick_volume"]
                    if tv:
                        conn.execute(
                            "INSERT OR REPLACE INTO tick_volumes (timestamp, price, buy_volume, sell_volume, delta) VALUES (?, ?, ?, ?, ?)",
                            (tv["timestamp"], tv["price"], tv["buy_volume"], tv["sell_volume"], tv["delta"])
                        )

                    cd = item["cum_delta"]
                    if cd:
                        conn.execute(
                            "INSERT OR REPLACE INTO delta_history (timestamp, cumulative_delta) VALUES (?, ?)",
                            (cd[0], cd[1])
                        )

                    for ev in item["events"]:
                        conn.execute(
                            "INSERT INTO events (timestamp, event_type, side, price, data) VALUES (?, ?, ?, ?, ?)",
                            (ev["timestamp"], ev["event_type"], ev["side"], ev["price"], json.dumps(ev))
                        )
        except Exception as e:
            logger.error("[Storage] Flush error: %s", e)

    def load_bookmap_frames(self, limit=3000):
        """Load recent bookmap frames for history."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, ltp, bids, asks FROM bookmap_frames ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
        rows.reverse()
        return [
            {"ts": r[0], "ltp": r[1], "bids": json.loads(r[2]), "asks": json.loads(r[3])}
            for r in rows
        ]

    def load_tick_volumes(self, limit=5000):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, price, buy_volume, sell_volume, delta FROM tick_volumes ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
        rows.reverse()
        return [
            {"timestamp": r[0], "price": r[1], "buy_volume": r[2], "sell_volume": r[3], "delta": r[4]}
            for r in rows
        ]

    def load_delta_history(self, limit=5000):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, cumulative_delta FROM delta_history ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
        rows.reverse()
        return [(r[0], r[1]) for r in rows]

    def get_last_cumulative_delta(self):
        """Get the most recent cumulative delta for state restoration."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT cumulative_delta FROM delta_history ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else 0

    def save_candles(self, candles):
        """Save historical candles to SQLite (bulk insert, skip duplicates)."""
        if not candles:
            return
        try:
            with self._conn() as conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO candle_history (timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
                    [(c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"]) for c in candles]
                )
            logger.info("[Storage] Saved %d candles", len(candles))
        except Exception as e:
            logger.error("[Storage] Error saving candles: %s", e)

    def load_candles(self, days=5):
        """Load candles from the last N days."""
        cutoff = time.time() - days * 86400
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, open, high, low, close, volume FROM candle_history WHERE timestamp >= ? ORDER BY timestamp",
                (cutoff,)
            ).fetchall()
        return [
            {"timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows
        ]

    def has_candles_for_date(self, date_str):
        """Check if we already have candles cached for a given date (YYYY-MM-DD)."""
        from datetime import datetime
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = dt.timestamp()
        end = start + 86400
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM candle_history WHERE timestamp >= ? AND timestamp < ?",
                (start, end)
            ).fetchone()
        return row[0] > 0

    def cleanup(self, max_age_hours=24):
        """Remove data older than max_age_hours. Candle history kept longer (7 days)."""
        cutoff = time.time() - max_age_hours * 3600
        candle_cutoff = time.time() - 7 * 86400
        try:
            with self._conn() as conn:
                for table in ["bookmap_frames", "tick_volumes", "delta_history", "events"]:
                    conn.execute(f"DELETE FROM {table} WHERE timestamp < ?", (cutoff,))
                conn.execute("DELETE FROM candle_history WHERE timestamp < ?", (candle_cutoff,))
            logger.info("[Storage] Cleaned up data older than %dh", max_age_hours)
        except Exception as e:
            logger.error("[Storage] Cleanup error: %s", e)

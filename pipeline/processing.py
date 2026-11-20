"""Pure stream-processing logic (no Kafka / DB in here, so it is easy to unit-test).

* WindowAggregator – tumbling 1-minute windows per wiki, keyed on EVENT time.
* AlertRules       – stateful per-event rules that emit alerts.
"""
import hashlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone


def parse_ts(iso: str) -> float:
    """ISO-8601 (with trailing Z) -> epoch seconds."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def minute_floor(ts: float) -> int:
    return int(ts // 60) * 60


# ----------------------------------------------------------------------------- windows
@dataclass
class Window:
    edits: int = 0
    bot_edits: int = 0
    human_edits: int = 0
    new_pages: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    users: set = field(default_factory=set)


class WindowAggregator:
    """Aggregates events into (wiki, minute) tumbling windows and remembers which ones changed."""

    def __init__(self, retain_seconds: int = 300):
        self.windows: dict[tuple[str, int], Window] = {}
        self.dirty: set[tuple[str, int]] = set()
        self.retain_seconds = retain_seconds
        self.max_ts = 0.0

    def add(self, ev: dict) -> None:
        ts = parse_ts(ev["event_time"])
        self.max_ts = max(self.max_ts, ts)
        key = (ev["wiki"], minute_floor(ts))
        w = self.windows.setdefault(key, Window())
        w.edits += 1
        if ev["is_bot"]:
            w.bot_edits += 1
        else:
            w.human_edits += 1
        if ev["event_type"] == "new":
            w.new_pages += 1
        delta = ev["byte_delta"] or 0
        if delta >= 0:
            w.bytes_added += delta
        else:
            w.bytes_removed += -delta
        if ev.get("user_name"):
            w.users.add(ev["user_name"])
        self.dirty.add(key)

    def pop_dirty_rows(self) -> list[tuple]:
        """Rows ready for UPSERT into edit_stats_minute; clears the dirty set."""
        rows = []
        for key in self.dirty:
            w = self.windows[key]
            rows.append((key[0], datetime.fromtimestamp(key[1], tz=timezone.utc), w.edits, w.bot_edits,
                         w.human_edits, w.new_pages, w.bytes_added, w.bytes_removed, len(w.users)))
        self.dirty.clear()
        return rows

    def prune(self) -> None:
        """Forget windows that closed more than `retain_seconds` ago (late events beyond that are dropped)."""
        cutoff = self.max_ts - self.retain_seconds
        for key in [k for k in self.windows if k[1] + 60 < cutoff]:
            self.windows.pop(key, None)
            self.dirty.discard(key)


# ----------------------------------------------------------------------------- alert rules
class AlertRules:
    """Stateful per-event rules. `process(event)` returns zero or more alert dicts.

    MASS_DELETION  – a non-bot edit removes >= `mass_delete_bytes` bytes (page blanking / vandalism candidate)
    EDIT_BURST     – >= `burst_edits` edits by >= `burst_users` distinct users on one page within `burst_window` s
                     (edit war or breaking news)
    HIGH_RATE_USER – a non-bot account makes >= `user_rate` edits within `user_window` s (script / spam candidate)
    Each (rule, subject) alerts at most once per `cooldown` seconds.
    """

    def __init__(self, mass_delete_bytes=10_000, burst_edits=5, burst_users=3, burst_window=300,
                 user_rate=20, user_window=60, cooldown=600):
        self.mass_delete_bytes = mass_delete_bytes
        self.burst_edits, self.burst_users, self.burst_window = burst_edits, burst_users, burst_window
        self.user_rate, self.user_window = user_rate, user_window
        self.cooldown = cooldown
        self.title_hist: dict[tuple, deque] = {}
        self.user_hist: dict[tuple, deque] = {}
        self.last_alert: dict[tuple, float] = {}
        self._n = 0

    def _cooling(self, key: tuple, ts: float) -> bool:
        last = self.last_alert.get(key)
        if last is not None and ts - last < self.cooldown:
            return True
        self.last_alert[key] = ts
        return False

    @staticmethod
    def _alert(ev, ts, rule, severity, value, detail) -> dict:
        raw_id = f"{rule}|{ev['wiki']}|{ev.get('title')}|{ev.get('user_name')}|{ev['event_id']}"
        return {
            "alert_id": hashlib.sha1(raw_id.encode()).hexdigest(),      # deterministic -> replays are idempotent
            "alert_time": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "rule": rule, "severity": severity, "wiki": ev["wiki"], "title": ev.get("title"),
            "user_name": ev.get("user_name"), "value": float(value), "detail": detail,
        }

    def process(self, ev: dict) -> list[dict]:
        ts = parse_ts(ev["event_time"])
        out = []
        self._n += 1

        # rule 1: mass deletion by a human
        if not ev["is_bot"] and (ev["byte_delta"] or 0) <= -self.mass_delete_bytes:
            if not self._cooling(("MASS_DELETION", ev["wiki"], ev.get("title")), ts):
                out.append(self._alert(ev, ts, "MASS_DELETION", "high", ev["byte_delta"],
                                       f"{ev.get('user_name')} removed {-ev['byte_delta']:,} bytes from '{ev.get('title')}'"))

        # rule 2: burst of edits by several users on the same page
        tkey = (ev["wiki"], ev.get("title"))
        hist = self.title_hist.setdefault(tkey, deque())
        hist.append((ts, ev.get("user_name")))
        while hist and ts - hist[0][0] > self.burst_window:
            hist.popleft()
        users = {u for _, u in hist}
        if len(hist) >= self.burst_edits and len(users) >= self.burst_users:
            if not self._cooling(("EDIT_BURST", *tkey), ts):
                out.append(self._alert(ev, ts, "EDIT_BURST", "medium", len(hist),
                                       f"{len(hist)} edits by {len(users)} users on '{ev.get('title')}' in {self.burst_window // 60} min"))

        # rule 3: very fast human editor
        if not ev["is_bot"] and ev.get("user_name"):
            ukey = (ev["wiki"], ev["user_name"])
            uh = self.user_hist.setdefault(ukey, deque())
            uh.append(ts)
            while uh and ts - uh[0] > self.user_window:
                uh.popleft()
            if len(uh) >= self.user_rate and not self._cooling(("HIGH_RATE_USER", *ukey), ts):
                out.append(self._alert(ev, ts, "HIGH_RATE_USER", "low", len(uh),
                                       f"{ev['user_name']} made {len(uh)} edits in {self.user_window}s on {ev['wiki']}"))

        if self._n % 5000 == 0:
            self._gc(ts)
        return out

    def _gc(self, now_ts: float) -> None:
        """Drop state for pages/users that have been quiet, so memory stays bounded."""
        for store, horizon in ((self.title_hist, self.burst_window), (self.user_hist, self.user_window)):
            for k in [k for k, d in store.items() if not d or now_ts - (d[-1][0] if isinstance(d[-1], tuple) else d[-1]) > horizon]:
                del store[k]
        for k in [k for k, t in self.last_alert.items() if now_ts - t > self.cooldown]:
            del self.last_alert[k]

"""Statistics collected while watching a broker."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class TopicStats:
    messages: int = 0
    payload_bytes: int = 0
    qos_counts: dict[int, int] = field(default_factory=dict)
    first_seen: float | None = None
    last_seen: float | None = None
    retained_payload: bytes | None = None
    retained_at: float | None = None


class WatchStats:
    """Connection history and per-topic traffic, driven by explicit timestamps."""

    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self.started_at = clock()
        self.connects = 0
        self.disconnects = 0
        self.messages_total = 0
        self.bytes_total = 0
        self.topics: dict[str, TopicStats] = {}
        self._connected_since: float | None = None
        self._connected_seconds = 0.0

    # -- connection history ------------------------------------------------
    def record_connect(self, now: float | None = None) -> None:
        now = self._clock() if now is None else now
        self.connects += 1
        self._connected_since = now

    def record_disconnect(self, now: float | None = None) -> None:
        now = self._clock() if now is None else now
        self.disconnects += 1
        if self._connected_since is not None:
            self._connected_seconds += now - self._connected_since
            self._connected_since = None

    @property
    def is_connected(self) -> bool:
        return self._connected_since is not None

    def connected_seconds(self, now: float | None = None) -> float:
        now = self._clock() if now is None else now
        total = self._connected_seconds
        if self._connected_since is not None:
            total += now - self._connected_since
        return total

    def uptime_ratio(self, now: float | None = None) -> float:
        now = self._clock() if now is None else now
        elapsed = now - self.started_at
        if elapsed <= 0:
            return 0.0
        return self.connected_seconds(now) / elapsed

    # -- traffic ------------------------------------------------------------
    def record_message(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        now: float | None = None,
    ) -> None:
        now = self._clock() if now is None else now
        entry = self.topics.setdefault(topic, TopicStats())
        entry.messages += 1
        entry.payload_bytes += len(payload)
        entry.qos_counts[qos] = entry.qos_counts.get(qos, 0) + 1
        if entry.first_seen is None:
            entry.first_seen = now
        entry.last_seen = now
        if retain:
            # An empty retained payload clears the retained message.
            entry.retained_payload = payload if payload else None
            entry.retained_at = now
        self.messages_total += 1
        self.bytes_total += len(payload)

    def retained_inventory(self) -> dict[str, TopicStats]:
        return {t: s for t, s in self.topics.items() if s.retained_payload is not None}

    def quiet_topics(self, threshold_seconds: float, now: float | None = None) -> list[str]:
        """Topics seen at least once but silent for longer than the threshold."""
        now = self._clock() if now is None else now
        return sorted(
            t
            for t, s in self.topics.items()
            if s.last_seen is not None and now - s.last_seen > threshold_seconds
        )

    def top_topics(self, limit: int = 10) -> list[tuple[str, TopicStats]]:
        return sorted(self.topics.items(), key=lambda kv: -kv[1].messages)[:limit]

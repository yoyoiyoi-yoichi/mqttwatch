"""Plain-text summary of what the watcher has seen so far."""

from __future__ import annotations

import time

from .stats import WatchStats


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "-"
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _preview(payload: bytes, limit: int = 40) -> str:
    text = payload.decode("utf-8", "replace").replace("\n", " ")
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def format_summary(
    stats: WatchStats,
    now: float | None = None,
    quiet_threshold: float | None = None,
) -> str:
    now = time.time() if now is None else now
    lines = [
        f"uptime {stats.uptime_ratio(now) * 100:.1f}%  "
        f"connects {stats.connects}  disconnects {stats.disconnects}  "
        f"messages {stats.messages_total}  bytes {stats.bytes_total}",
    ]
    if stats.topics:
        lines.append("topics (by messages):")
        for topic, entry in stats.top_topics():
            qos = ",".join(f"q{q}:{n}" for q, n in sorted(entry.qos_counts.items()))
            lines.append(
                f"  {topic}  msgs={entry.messages} bytes={entry.payload_bytes} "
                f"qos[{qos}] last={_fmt_ts(entry.last_seen)}"
            )
    retained = stats.retained_inventory()
    if retained:
        lines.append("retained messages:")
        for topic, entry in sorted(retained.items()):
            lines.append(f"  {topic} = {_preview(entry.retained_payload or b'')}")
    if quiet_threshold is not None:
        quiet = stats.quiet_topics(quiet_threshold, now)
        if quiet:
            lines.append(f"quiet > {quiet_threshold:.0f}s: " + ", ".join(quiet))
    return "\n".join(lines)

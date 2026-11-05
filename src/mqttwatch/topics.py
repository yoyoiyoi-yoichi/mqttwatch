"""MQTT topic-filter matching, including the $-topic rule."""

from __future__ import annotations


def topic_matches(topic_filter: str, topic: str) -> bool:
    """True if `topic` matches `topic_filter` under MQTT wildcard rules.

    - ``+`` matches exactly one level.
    - ``#`` (only valid as the last level) matches all remaining levels,
      including none: ``sport/#`` also matches ``sport`` itself.
    - A filter not starting with ``$`` never matches a topic starting
      with ``$`` (so ``#`` does not match ``$SYS/broker/uptime``).
    """
    if topic.startswith("$") and not topic_filter.startswith("$"):
        return False
    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")
    for i, level in enumerate(filter_levels):
        if level == "#":
            return i == len(filter_levels) - 1
        if i >= len(topic_levels):
            return False
        if level == "+":
            continue
        if level != topic_levels[i]:
            return False
    return len(topic_levels) == len(filter_levels)

"""Command line: run the watcher, log events, print periodic summaries."""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time

from .client import WatchClient
from .report import format_summary
from .stats import WatchStats


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mqttwatch",
        description="Passively watch an MQTT broker: connection history, "
        "topic traffic and retained-message inventory.",
    )
    parser.add_argument("--host", required=True, help="broker host")
    parser.add_argument("--port", type=int, default=1883, help="broker port (default 1883)")
    parser.add_argument(
        "--topic",
        action="append",
        dest="topics",
        help="topic filter to subscribe to (repeatable; default '#')",
    )
    parser.add_argument(
        "--sys",
        action="store_true",
        help="also subscribe to $SYS/# (broker internals; '#' alone never matches $ topics)",
    )
    parser.add_argument("--qos", type=int, default=0, choices=[0, 1], help="subscription QoS")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--client-id", default="mqttwatch")
    parser.add_argument("--keepalive", type=int, default=60)
    parser.add_argument(
        "--duration", type=float, default=None, help="stop after this many seconds"
    )
    parser.add_argument(
        "--report-every",
        type=float,
        default=60.0,
        help="print a summary every N seconds (0 = only at the end)",
    )
    parser.add_argument(
        "--quiet-threshold",
        type=float,
        default=None,
        help="flag topics silent for longer than N seconds in summaries",
    )
    parser.add_argument(
        "--events",
        metavar="FILE",
        help="append connection/subscription events as JSON lines to FILE",
    )
    parser.add_argument(
        "--log-messages",
        action="store_true",
        help="also log one event per received message (can be large)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    topics: list[tuple[str, int]] = [(t, args.qos) for t in (args.topics or ["#"])]
    if args.sys:
        topics.append(("$SYS/#", args.qos))

    stats = WatchStats()
    event_file = open(args.events, "a", encoding="utf-8") if args.events else None

    def on_event(event: dict) -> None:
        line = json.dumps(event, ensure_ascii=False)
        if event_file is not None:
            event_file.write(line + "\n")
            event_file.flush()
        else:
            print(line, flush=True)

    client = WatchClient(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        username=args.username,
        password=args.password,
        keepalive=args.keepalive,
        topics=topics,
        stats=stats,
        on_event=on_event,
        log_messages=args.log_messages,
    )

    stop_reporter = threading.Event()

    def reporter() -> None:
        while not stop_reporter.wait(args.report_every):
            print(format_summary(stats, quiet_threshold=args.quiet_threshold), flush=True)
            print("---", flush=True)

    reporter_thread = None
    if args.report_every > 0:
        reporter_thread = threading.Thread(target=reporter, daemon=True)
        reporter_thread.start()

    try:
        client.run(duration=args.duration)
    except KeyboardInterrupt:
        client.stop()
    finally:
        stop_reporter.set()
        if reporter_thread is not None:
            reporter_thread.join(timeout=1)
        print(format_summary(stats, quiet_threshold=args.quiet_threshold))
        if event_file is not None:
            event_file.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

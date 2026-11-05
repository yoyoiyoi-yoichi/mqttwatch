# mqttwatch

A passive watchdog for MQTT brokers. Standard library only: it speaks
MQTT 3.1.1 over a raw socket, stays subscribed, and keeps the history
you need after an incident — when the connection broke, what kept
flowing, and what the retained messages currently say.

## What it records

- **Connection history** — connect / disconnect events with reasons,
  uptime ratio, reconnect count (exponential backoff, resubscribes
  automatically)
- **Topic traffic** — message and byte counts per topic, QoS breakdown,
  first / last seen timestamps
- **Retained-message inventory** — the current retained payload of
  every topic (an empty retained payload clears the entry, as it
  should)
- **Quiet topics** — topics that were seen at least once and then went
  silent for longer than a threshold

The watcher never publishes application messages. Only protocol-level
PUBACK and PINGREQ go out.

## Install

```sh
pip install .
```

## Usage

Watch a broker (all topics), print a summary every minute:

```sh
mqttwatch --host broker.local
```

Also watch the broker's own `$SYS` tree (a plain `#` subscription never
matches `$` topics), log events to a file, run for an hour:

```sh
mqttwatch --host broker.local --sys \
  --events events.jsonl --duration 3600
```

Flag topics that go quiet for more than 5 minutes, and log every
message (one JSON line each):

```sh
mqttwatch --host 192.168.1.10 --topic 'sensors/#' \
  --quiet-threshold 300 --log-messages
```

Events are JSON lines, one per event: `connect`, `disconnect`
(with the reason), `subscribe`, and optionally `message`.

## Example summary

```
uptime 99.2%  connects 3  disconnects 2  messages 15230  bytes 402118
topics (by messages):
  sensors/temp  msgs=5021 bytes=25105 qos[q0:5021] last=03:12:44
  sensors/hum  msgs=4980 bytes=19872 qos[q0:4980] last=03:12:43
retained messages:
  sensors/temp = 21.5
quiet > 300s: sensors/co2
```

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The integration test runs against a tiny in-process fake broker, so no
real broker is needed.

## Limitations

- MQTT 3.1.1 only (no MQTT 5)
- Subscription QoS 0 / 1 (no QoS 2 handshake)
- No TLS yet — use it on trusted networks or behind a tunnel

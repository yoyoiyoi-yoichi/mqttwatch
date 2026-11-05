"""End-to-end test against a tiny in-process fake broker.

The fake broker speaks just enough MQTT to accept the watcher, then
pushes it two messages (one retained). The watcher's statistics must
reflect exactly what was sent.
"""

import socket
import struct
import threading
import unittest

from mqttwatch import packet
from mqttwatch.client import WatchClient
from mqttwatch.stats import WatchStats


def _frame(packet_type: int, flags: int, body: bytes) -> bytes:
    return bytes([(packet_type << 4) | flags]) + packet.encode_remaining_length(len(body)) + body


def _publish(topic: str, payload: bytes, retain: bool) -> bytes:
    body = struct.pack("!H", len(topic)) + topic.encode() + payload
    return _frame(packet.PUBLISH, 1 if retain else 0, body)


class FakeBroker(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]

    def run(self) -> None:
        conn, _ = self.listener.accept()
        conn.settimeout(10)
        reader = packet.PacketReader()
        subscribed = False
        try:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                reader.feed(chunk)
                while True:
                    found = reader.next_packet()
                    if found is None:
                        break
                    ptype, _, body = found
                    if ptype == packet.CONNECT:
                        conn.sendall(_frame(packet.CONNACK, 0, bytes([0, 0])))
                    elif ptype == packet.SUBSCRIBE:
                        packet_id = struct.unpack("!H", body[:2])[0]
                        conn.sendall(
                            _frame(packet.SUBACK, 0, struct.pack("!H", packet_id) + bytes([0]))
                        )
                        if not subscribed:
                            subscribed = True
                            conn.sendall(_publish("sensors/temp", b"21.5", retain=True))
                            conn.sendall(_publish("sensors/hum", b"40", retain=False))
                    elif ptype == packet.PINGREQ:
                        conn.sendall(_frame(packet.PINGRESP, 0, b""))
                    elif ptype == packet.DISCONNECT:
                        return
        except OSError:
            return
        finally:
            conn.close()
            self.listener.close()


class IntegrationTests(unittest.TestCase):
    def test_watcher_records_what_the_broker_sends(self):
        broker = FakeBroker()
        broker.start()
        stats = WatchStats()
        events: list[dict] = []
        client = WatchClient(
            host="127.0.0.1",
            port=broker.port,
            keepalive=30,
            topics=[("#", 0)],
            stats=stats,
            on_event=events.append,
        )
        thread = threading.Thread(target=client.run, kwargs={"duration": 2.0}, daemon=True)
        thread.start()
        thread.join(timeout=8)
        self.assertFalse(thread.is_alive(), "watcher did not stop after its duration")

        self.assertEqual(stats.connects, 1)
        self.assertEqual(stats.messages_total, 2)
        self.assertEqual(stats.topics["sensors/temp"].retained_payload, b"21.5")
        self.assertEqual(stats.topics["sensors/hum"].messages, 1)
        event_types = [e["type"] for e in events]
        self.assertIn("connect", event_types)
        self.assertIn("subscribe", event_types)


if __name__ == "__main__":
    unittest.main()

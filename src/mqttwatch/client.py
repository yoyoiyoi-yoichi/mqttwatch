"""The watching client: stays connected, records everything, reconnects.

The client is passive: it subscribes and listens. It never publishes
application messages (only protocol-level PUBACK / PINGREQ go out).
On any connection loss it records the disconnect and retries with
exponential backoff, resubscribing after each reconnect, so the history
shows exactly when the broker (or the path to it) was broken.
"""

from __future__ import annotations

import socket
import time
from typing import Callable

from . import packet
from .stats import WatchStats

Event = dict


class WatchClient:
    def __init__(
        self,
        host: str,
        port: int = 1883,
        client_id: str = "mqttwatch",
        username: str | None = None,
        password: str | None = None,
        keepalive: int = 60,
        topics: list[tuple[str, int]] | None = None,
        stats: WatchStats | None = None,
        on_event: Callable[[Event], None] | None = None,
        log_messages: bool = False,
        backoff_max: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.topics = topics if topics is not None else [("#", 0)]
        self.stats = stats if stats is not None else WatchStats()
        self.on_event = on_event
        self.log_messages = log_messages
        self.backoff_max = backoff_max
        self._stopped = False
        self._sock: socket.socket | None = None
        self._packet_id = 0

    # -- plumbing -----------------------------------------------------------
    def _emit(self, event: Event) -> None:
        if self.on_event is not None:
            self.on_event(event)

    def _next_packet_id(self) -> int:
        self._packet_id = (self._packet_id % 65535) + 1
        return self._packet_id

    def stop(self) -> None:
        self._stopped = True
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass

    # -- main loop -----------------------------------------------------------
    def run(self, duration: float | None = None) -> None:
        """Watch until stopped, or for `duration` seconds."""
        deadline = None if duration is None else time.monotonic() + duration
        backoff = 1.0
        first_attempt = True
        while not self._stopped:
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                self._session(deadline)
                backoff = 1.0
            except (OSError, ConnectionError, ValueError) as exc:
                self.stats.record_disconnect()
                self._emit(
                    {
                        "type": "disconnect",
                        "ts": time.time(),
                        "reason": str(exc),
                        "reconnect": not first_attempt,
                    }
                )
            first_attempt = False
            if self._stopped or (deadline is not None and time.monotonic() >= deadline):
                break
            time.sleep(min(backoff, self._remaining(deadline)))
            backoff = min(backoff * 2, self.backoff_max)

    @staticmethod
    def _remaining(deadline: float | None) -> float:
        if deadline is None:
            return 1.0
        return max(0.0, deadline - time.monotonic())

    def _session(self, deadline: float | None) -> None:
        sock = socket.create_connection((self.host, self.port), timeout=10)
        self._sock = sock
        sock.settimeout(0.25)
        reader = packet.PacketReader()
        try:
            sock.sendall(
                packet.build_connect(
                    self.client_id,
                    keepalive=self.keepalive,
                    username=self.username,
                    password=self.password,
                )
            )
            ptype, _, body = self._read_one(sock, reader, deadline)
            if ptype != packet.CONNACK:
                raise ConnectionError(f"expected CONNACK, got packet type {ptype}")
            _, return_code = packet.parse_connack(body)
            if return_code != 0:
                raise ConnectionError(f"broker refused connection, CONNACK code {return_code}")

            self.stats.record_connect()
            self._emit({"type": "connect", "ts": time.time(), "host": self.host, "port": self.port})
            sock.sendall(packet.build_subscribe(self._next_packet_id(), self.topics))
            self._emit(
                {
                    "type": "subscribe",
                    "ts": time.time(),
                    "topics": [t for t, _ in self.topics],
                }
            )
            self._listen(sock, reader, deadline)
        finally:
            try:
                sock.sendall(packet.build_disconnect())
            except OSError:
                pass
            sock.close()
            self._sock = None

    def _read_one(
        self, sock: socket.socket, reader: packet.PacketReader, deadline: float | None
    ) -> tuple[int, int, bytes]:
        while True:
            found = reader.next_packet()
            if found is not None:
                return found
            if deadline is not None and time.monotonic() >= deadline:
                raise ConnectionError("duration elapsed while waiting for a packet")
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                raise ConnectionError("broker closed the connection")
            reader.feed(chunk)

    def _listen(self, sock: socket.socket, reader: packet.PacketReader, deadline: float | None) -> None:
        last_rx = time.monotonic()
        last_tx = time.monotonic()
        while not self._stopped:
            if deadline is not None and time.monotonic() >= deadline:
                return
            found = reader.next_packet()
            if found is None:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    chunk = b""
                if chunk:
                    reader.feed(chunk)
                    continue
            else:
                ptype, flags, body = found
                last_rx = time.monotonic()
                if ptype == packet.PUBLISH:
                    message = packet.parse_publish(flags, body)
                    self.stats.record_message(
                        message.topic, message.payload, message.qos, message.retain
                    )
                    if message.qos == 1 and message.packet_id is not None:
                        sock.sendall(packet.build_puback(message.packet_id))
                        last_tx = time.monotonic()
                    if self.log_messages:
                        self._emit(
                            {
                                "type": "message",
                                "ts": time.time(),
                                "topic": message.topic,
                                "qos": message.qos,
                                "retain": message.retain,
                                "bytes": len(message.payload),
                            }
                        )
                # SUBACK / PINGRESP / anything else: receipt alone proves liveness.
            now = time.monotonic()
            if self.keepalive > 0:
                if now - last_tx >= self.keepalive * 0.75:
                    sock.sendall(packet.build_pingreq())
                    last_tx = now
                if now - last_rx > self.keepalive * 1.5:
                    raise ConnectionError("keepalive timeout: broker stopped responding")

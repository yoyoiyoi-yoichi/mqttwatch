"""Minimal MQTT 3.1.1 wire codec.

Only the subset a passive watcher needs: CONNECT / CONNACK, SUBSCRIBE /
SUBACK, PUBLISH (QoS 0 and 1), PUBACK, PINGREQ / PINGRESP, DISCONNECT.
Implemented on raw sockets with the standard library only.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
SUBSCRIBE = 8
SUBACK = 9
PINGREQ = 12
PINGRESP = 13
DISCONNECT = 14


def encode_remaining_length(n: int) -> bytes:
    if n < 0 or n > 268_435_455:
        raise ValueError("remaining length out of range")
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n > 0:
            digit |= 0x80
        out.append(digit)
        if n == 0:
            return bytes(out)


def _packet(packet_type: int, flags: int, body: bytes) -> bytes:
    return bytes([(packet_type << 4) | flags]) + encode_remaining_length(len(body)) + body


def _utf8(text: str) -> bytes:
    data = text.encode("utf-8")
    return struct.pack("!H", len(data)) + data


def build_connect(
    client_id: str,
    keepalive: int = 60,
    username: str | None = None,
    password: str | None = None,
    clean_session: bool = True,
) -> bytes:
    flags = 0
    payload = _utf8(client_id)
    if username is not None:
        flags |= 0x80
        payload += _utf8(username)
    if password is not None:
        flags |= 0x40
        payload += _utf8(password)
    if clean_session:
        flags |= 0x02
    variable_header = _utf8("MQTT") + bytes([4, flags]) + struct.pack("!H", keepalive)
    return _packet(CONNECT, 0, variable_header + payload)


def build_subscribe(packet_id: int, topics: list[tuple[str, int]]) -> bytes:
    body = struct.pack("!H", packet_id)
    for topic_filter, qos in topics:
        body += _utf8(topic_filter) + bytes([qos])
    return _packet(SUBSCRIBE, 0b0010, body)


def build_puback(packet_id: int) -> bytes:
    return _packet(PUBACK, 0, struct.pack("!H", packet_id))


def build_pingreq() -> bytes:
    return _packet(PINGREQ, 0, b"")


def build_disconnect() -> bytes:
    return _packet(DISCONNECT, 0, b"")


class PacketReader:
    """Incrementally frames MQTT packets out of a TCP byte stream."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf += data

    def next_packet(self) -> tuple[int, int, bytes] | None:
        """Return (packet_type, flags, body), or None if more bytes are needed."""
        if len(self._buf) < 2:
            return None
        idx = 1
        remaining = 0
        multiplier = 1
        while True:
            if idx >= len(self._buf):
                return None
            byte = self._buf[idx]
            remaining += (byte & 0x7F) * multiplier
            idx += 1
            if not byte & 0x80:
                break
            multiplier *= 128
            if idx > 5:
                raise ValueError("malformed remaining length")
        if len(self._buf) < idx + remaining:
            return None
        header = self._buf[0]
        body = bytes(self._buf[idx : idx + remaining])
        del self._buf[: idx + remaining]
        return header >> 4, header & 0x0F, body


@dataclass
class Publish:
    topic: str
    payload: bytes
    qos: int
    retain: bool
    dup: bool
    packet_id: int | None = None


def parse_publish(flags: int, body: bytes) -> Publish:
    qos = (flags >> 1) & 0b11
    topic_len = struct.unpack("!H", body[:2])[0]
    topic = body[2 : 2 + topic_len].decode("utf-8", "replace")
    offset = 2 + topic_len
    packet_id = None
    if qos > 0:
        packet_id = struct.unpack("!H", body[offset : offset + 2])[0]
        offset += 2
    return Publish(
        topic=topic,
        payload=body[offset:],
        qos=qos,
        retain=bool(flags & 0x01),
        dup=bool(flags & 0x08),
        packet_id=packet_id,
    )


def parse_connack(body: bytes) -> tuple[bool, int]:
    """Return (session_present, return_code). 0 means accepted."""
    return bool(body[0] & 0x01), body[1]

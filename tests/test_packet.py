import struct
import unittest

from mqttwatch import packet


class RemainingLengthTests(unittest.TestCase):
    def test_roundtrip_through_reader(self):
        for n in (0, 1, 127, 128, 16_383, 16_384, 2_097_151):
            encoded = packet.encode_remaining_length(n)
            self.assertLessEqual(len(encoded), 4)
            # Wrap in a PINGREQ-shaped frame: header + length + body
            frame = bytes([0xC0]) + encoded + bytes(n)
            reader = packet.PacketReader()
            reader.feed(frame)
            found = reader.next_packet()
            self.assertIsNotNone(found)
            ptype, _, body = found
            self.assertEqual(ptype, packet.PINGREQ)
            self.assertEqual(len(body), n)

    def test_reader_handles_byte_by_byte_feed(self):
        frame = packet.build_pingreq()
        reader = packet.PacketReader()
        result = None
        for byte in frame:
            reader.feed(bytes([byte]))
            result = reader.next_packet()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], packet.PINGREQ)

    def test_reader_waits_for_incomplete_packet(self):
        reader = packet.PacketReader()
        reader.feed(packet.build_pingreq()[:1])
        self.assertIsNone(reader.next_packet())


class BuildTests(unittest.TestCase):
    def test_connect_packet_shape(self):
        data = packet.build_connect("watcher", keepalive=30)
        self.assertEqual(data[0], 0x10)
        self.assertIn(b"MQTT", data)
        self.assertIn(b"watcher", data)
        # protocol level 4 (3.1.1) right after the protocol name
        idx = data.index(b"MQTT") + 4
        self.assertEqual(data[idx], 4)
        self.assertEqual(struct.unpack("!H", data[idx + 2 : idx + 4])[0], 30)

    def test_subscribe_has_reserved_flags(self):
        data = packet.build_subscribe(7, [("a/#", 1)])
        self.assertEqual(data[0], 0x82)  # SUBSCRIBE with flags 0010
        self.assertIn(b"a/#", data)


class ParsePublishTests(unittest.TestCase):
    def _publish_frame(self, topic: str, payload: bytes, qos: int, retain: bool) -> bytes:
        body = struct.pack("!H", len(topic)) + topic.encode()
        if qos > 0:
            body += struct.pack("!H", 42)
        body += payload
        flags = (qos << 1) | (1 if retain else 0)
        return bytes([(packet.PUBLISH << 4) | flags]) + packet.encode_remaining_length(len(body)) + body

    def test_parse_qos0_retained(self):
        reader = packet.PacketReader()
        reader.feed(self._publish_frame("sensors/temp", b"21.5", 0, True))
        ptype, flags, body = reader.next_packet()
        message = packet.parse_publish(flags, body)
        self.assertEqual(message.topic, "sensors/temp")
        self.assertEqual(message.payload, b"21.5")
        self.assertEqual(message.qos, 0)
        self.assertTrue(message.retain)
        self.assertIsNone(message.packet_id)

    def test_parse_qos1_has_packet_id(self):
        reader = packet.PacketReader()
        reader.feed(self._publish_frame("a/b", b"x", 1, False))
        _, flags, body = reader.next_packet()
        message = packet.parse_publish(flags, body)
        self.assertEqual(message.qos, 1)
        self.assertEqual(message.packet_id, 42)
        self.assertEqual(message.payload, b"x")

    def test_parse_connack(self):
        self.assertEqual(packet.parse_connack(bytes([0, 0])), (False, 0))
        self.assertEqual(packet.parse_connack(bytes([1, 5])), (True, 5))


if __name__ == "__main__":
    unittest.main()

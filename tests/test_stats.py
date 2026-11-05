import unittest

from mqttwatch.stats import WatchStats


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class StatsTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.stats = WatchStats(clock=self.clock)

    def test_uptime_ratio_counts_only_connected_time(self):
        s = self.stats
        s.record_connect()  # t=1000
        self.clock.now += 30
        s.record_disconnect()  # 30s connected
        self.clock.now += 10
        s.record_connect()
        self.clock.now += 20  # t=1060, 50s connected of 60s elapsed
        self.assertAlmostEqual(s.uptime_ratio(), 50 / 60)
        self.assertEqual(s.connects, 2)
        self.assertEqual(s.disconnects, 1)
        self.assertTrue(s.is_connected)

    def test_message_counts_and_retained_inventory(self):
        s = self.stats
        s.record_message("sensors/temp", b"21.5", qos=0, retain=True)
        self.clock.now += 5
        s.record_message("sensors/temp", b"22.0", qos=1, retain=True)
        s.record_message("sensors/hum", b"40", qos=0, retain=False)
        temp = s.topics["sensors/temp"]
        self.assertEqual(temp.messages, 2)
        self.assertEqual(temp.qos_counts, {0: 1, 1: 1})
        self.assertEqual(temp.retained_payload, b"22.0")
        self.assertEqual(set(s.retained_inventory()), {"sensors/temp"})
        self.assertEqual(s.messages_total, 3)

    def test_empty_retained_payload_clears(self):
        s = self.stats
        s.record_message("a/b", b"data", qos=0, retain=True)
        s.record_message("a/b", b"", qos=0, retain=True)
        self.assertEqual(s.retained_inventory(), {})

    def test_quiet_topics(self):
        s = self.stats
        s.record_message("old/topic", b"x", qos=0, retain=False)
        self.clock.now += 100
        s.record_message("fresh/topic", b"y", qos=0, retain=False)
        self.assertEqual(s.quiet_topics(60), ["old/topic"])
        self.assertEqual(s.quiet_topics(200), [])


if __name__ == "__main__":
    unittest.main()

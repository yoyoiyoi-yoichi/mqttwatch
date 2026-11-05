import unittest

from mqttwatch.topics import topic_matches


class TopicMatchTests(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(topic_matches("a/b", "a/b"))
        self.assertFalse(topic_matches("a/b", "a/c"))
        self.assertFalse(topic_matches("a/b", "a/b/c"))

    def test_single_level_wildcard(self):
        self.assertTrue(topic_matches("a/+", "a/b"))
        self.assertFalse(topic_matches("a/+", "a/b/c"))
        self.assertFalse(topic_matches("a/+", "a"))

    def test_multi_level_wildcard(self):
        self.assertTrue(topic_matches("a/#", "a/b/c"))
        self.assertTrue(topic_matches("a/#", "a"))  # '#' also matches the parent level
        self.assertTrue(topic_matches("#", "anything/at/all"))
        self.assertFalse(topic_matches("a/#", "ab/c"))

    def test_dollar_topics_need_dollar_filter(self):
        self.assertFalse(topic_matches("#", "$SYS/broker/uptime"))
        self.assertFalse(topic_matches("+/broker/uptime", "$SYS/broker/uptime"))
        self.assertTrue(topic_matches("$SYS/#", "$SYS/broker/uptime"))


if __name__ == "__main__":
    unittest.main()

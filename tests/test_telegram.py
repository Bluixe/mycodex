from __future__ import annotations

import unittest

from mycodex.telegram import parse_command


class TelegramTests(unittest.TestCase):
    def test_parse_command(self) -> None:
        self.assertEqual(parse_command("/bind my-codex"), ("bind", "my-codex"))
        self.assertEqual(parse_command("/bind@mycodex_bot demo"), ("bind", "demo"))
        self.assertEqual(parse_command("/bind-thread demo abc"), ("bind_thread", "demo abc"))
        self.assertIsNone(parse_command("plain text"))


if __name__ == "__main__":
    unittest.main()

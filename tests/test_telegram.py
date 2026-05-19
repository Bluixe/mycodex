from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from mycodex.db import Store
from mycodex.telegram import default_token_file, notify_project_bindings, parse_command, token_from_sources


class FakeTelegramClient:
    def __init__(self) -> None:
        self.messages = []

    def send_chunks(self, chat_id: str, text: str, *, message_thread_id: str | None = None) -> None:
        self.messages.append((chat_id, text, message_thread_id))


class TelegramTests(unittest.TestCase):
    def test_parse_command(self) -> None:
        self.assertEqual(parse_command("/bind my-codex"), ("bind", "my-codex"))
        self.assertEqual(parse_command("/bind@mycodex_bot demo"), ("bind", "demo"))
        self.assertEqual(parse_command("/bind-thread demo abc"), ("bind_thread", "demo abc"))
        self.assertIsNone(parse_command("plain text"))

    def test_notify_project_bindings_sends_to_matching_project(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            other_repo = Path(tmp) / "other"
            repo.mkdir()
            other_repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                other = store.add_project("other", other_repo)
                store.set_telegram_binding(
                    chat_id="-100",
                    message_thread_id="7",
                    project_id=project["id"],
                    thread_id="thread-1",
                )
                store.set_telegram_binding(
                    chat_id="-200",
                    message_thread_id=None,
                    project_id=other["id"],
                    thread_id="thread-2",
                )
                client = FakeTelegramClient()

                result = notify_project_bindings(
                    store,
                    project["id"],
                    "hello",
                    client=client,
                )

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(client.messages, [("-100", "hello", "7")])

    def test_token_from_sources_reads_default_db_local_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "my_codex.sqlite"
            token_path = default_token_file(db_path)
            token_path.parent.mkdir(parents=True)
            token_path.write_text("token-from-file\n", encoding="utf-8")

            token = token_from_sources(token_env="MISSING_TOKEN_ENV", db_path=db_path)

        self.assertEqual(token, "token-from-file")

    def test_token_from_sources_explicit_file_precedes_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "token.txt"
            token_path.write_text("explicit-file-token\n", encoding="utf-8")

            token = token_from_sources(
                token_file=token_path,
                token_env="PATH",
                db_path=Path(tmp) / "db.sqlite",
            )

        self.assertEqual(token, "explicit-file-token")


if __name__ == "__main__":
    unittest.main()

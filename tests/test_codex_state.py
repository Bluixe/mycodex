from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from mycodex.codex_state import discover_codex_projects


class CodexStateTests(unittest.TestCase):
    def test_discover_codex_projects_groups_by_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    first_user_message TEXT NOT NULL,
                    source TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO threads
                    (id, cwd, title, first_user_message, source, updated_at, archived)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("a", "/repo/one", "old", "old msg", "cli", 10, 0),
                    ("b", "/repo/one", "new", "new msg", "vscode", 20, 0),
                    ("c", "/repo/two", "other", "other msg", "cli", 15, 0),
                    ("d", "/repo/archived", "hidden", "hidden msg", "cli", 30, 1),
                ],
            )
            conn.commit()
            conn.close()

            projects = discover_codex_projects(db_path=db_path)
            self.assertEqual([p["cwd"] for p in projects], ["/repo/one", "/repo/two"])
            self.assertEqual(projects[0]["session_count"], 2)
            self.assertEqual(projects[0]["latest_thread_id"], "b")


if __name__ == "__main__":
    unittest.main()

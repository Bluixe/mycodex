from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.token_usage import token_usage_snapshot


class TokenUsageTests(unittest.TestCase):
    def test_snapshot_aggregates_usage_and_maps_project_by_thread(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_db = tmp_path / "logs_2.sqlite"
            repo_path = tmp_path / "repo"
            repo_path.mkdir()
            now = int(time.time())
            _init_logs_db(logs_db)
            _insert_usage_event(
                logs_db,
                ts=now - 10,
                process_uuid="proc-1",
                response_id="resp-1",
                model="gpt-5.5",
                input_tokens=100,
                output_tokens=20,
                cached_tokens=80,
                reasoning_tokens=5,
            )
            _insert_metadata_event(
                logs_db,
                ts=now - 9,
                process_uuid="proc-1",
                thread_id="thread-1",
                cwd=repo_path,
            )

            with Store(tmp_path / "mycodex.sqlite") as store:
                project = store.add_project("demo", repo_path)
                store.upsert_session(
                    project["id"],
                    {
                        "id": "thread-1",
                        "cwd": str(repo_path),
                        "preview": "",
                        "status": {"type": "notLoaded"},
                    },
                )
                store.commit()
                snapshot = token_usage_snapshot(store, logs_db_path=logs_db, days=1)

            self.assertTrue(snapshot["available"])
            self.assertEqual(snapshot["summary"]["events"], 1)
            self.assertEqual(snapshot["summary"]["total_tokens"], 120)
            self.assertEqual(snapshot["summary"]["cached_tokens"], 80)
            self.assertEqual(snapshot["summary"]["reasoning_tokens"], 5)
            self.assertEqual(snapshot["by_project"][0]["name"], "demo")
            self.assertEqual(snapshot["by_model"][0]["name"], "gpt-5.5")
            self.assertEqual(len(snapshot["by_hour"]), 24)
            self.assertTrue(any(row["total_tokens"] == 120 for row in snapshot["by_hour"]))
            self.assertEqual(snapshot["hour_axis"]["max_tokens"], 120)
            self.assertEqual(snapshot["day_axis"]["max_tokens"], 120)
            self.assertEqual(snapshot["project_pie"][0]["name"], "demo")
            self.assertEqual(snapshot["project_pie"][0]["percent"], 100.0)
            self.assertNotIn("feedback_log_body", snapshot["recent"][0])

    def test_snapshot_maps_project_by_cwd_without_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_db = tmp_path / "logs_2.sqlite"
            repo_path = tmp_path / "repo"
            repo_path.mkdir()
            now = int(time.time())
            _init_logs_db(logs_db)
            _insert_usage_event(
                logs_db,
                ts=now - 20,
                process_uuid="proc-2",
                response_id="resp-2",
                model="gpt-5.4",
                input_tokens=40,
                output_tokens=10,
            )
            _insert_metadata_event(
                logs_db,
                ts=now - 20,
                process_uuid="proc-2",
                cwd=repo_path,
            )

            with Store(tmp_path / "mycodex.sqlite") as store:
                store.add_project("demo", repo_path)
                snapshot = token_usage_snapshot(store, logs_db_path=logs_db, days=1)

            self.assertEqual(snapshot["by_project"][0]["name"], "demo")
            self.assertEqual(snapshot["summary"]["total_tokens"], 50)

    def test_future_metadata_does_not_remap_existing_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_db = tmp_path / "logs_2.sqlite"
            repo_a = tmp_path / "repo-a"
            repo_b = tmp_path / "repo-b"
            repo_a.mkdir()
            repo_b.mkdir()
            now = int(time.time())
            _init_logs_db(logs_db)
            _insert_metadata_event(
                logs_db,
                ts=now - 20,
                process_uuid="shared-proc",
                thread_id="thread-a",
                cwd=repo_a,
            )
            _insert_usage_event(
                logs_db,
                ts=now - 10,
                process_uuid="shared-proc",
                response_id="resp-a",
                model="gpt-5.5",
                input_tokens=90,
                output_tokens=10,
            )
            _insert_metadata_event(
                logs_db,
                ts=now - 9,
                process_uuid="shared-proc",
                thread_id="thread-b",
                cwd=repo_b,
            )

            with Store(tmp_path / "mycodex.sqlite") as store:
                project_a = store.add_project("project-a", repo_a)
                project_b = store.add_project("project-b", repo_b)
                store.upsert_session(
                    project_a["id"],
                    {
                        "id": "thread-a",
                        "cwd": str(repo_a),
                        "preview": "",
                        "status": {"type": "notLoaded"},
                    },
                )
                store.upsert_session(
                    project_b["id"],
                    {
                        "id": "thread-b",
                        "cwd": str(repo_b),
                        "preview": "",
                        "status": {"type": "notLoaded"},
                    },
                )
                store.commit()
                snapshot = token_usage_snapshot(store, logs_db_path=logs_db, days=1)

            self.assertEqual(snapshot["recent"][0]["project"], "project-a")
            self.assertEqual(snapshot["by_project"][0]["name"], "project-a")
            self.assertEqual(snapshot["by_project"][0]["total_tokens"], 100)

    def test_seen_usage_survives_codex_log_row_pruning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            logs_db = tmp_path / "logs_2.sqlite"
            repo_path = tmp_path / "repo"
            repo_path.mkdir()
            now = int(time.time())
            _init_logs_db(logs_db)
            _insert_metadata_event(
                logs_db,
                ts=now - 11,
                process_uuid="proc-prune",
                thread_id="thread-prune",
                cwd=repo_path,
            )
            _insert_usage_event(
                logs_db,
                ts=now - 10,
                process_uuid="proc-prune",
                response_id="resp-prune",
                model="gpt-5.5",
                input_tokens=70,
                output_tokens=30,
            )

            with Store(tmp_path / "mycodex.sqlite") as store:
                project = store.add_project("demo", repo_path)
                store.upsert_session(
                    project["id"],
                    {
                        "id": "thread-prune",
                        "cwd": str(repo_path),
                        "preview": "",
                        "status": {"type": "notLoaded"},
                    },
                )
                store.commit()
                first = token_usage_snapshot(store, logs_db_path=logs_db, days=1)
                _delete_usage_events(logs_db)
                second = token_usage_snapshot(store, logs_db_path=logs_db, days=1)

            self.assertEqual(first["summary"]["total_tokens"], 100)
            self.assertEqual(second["summary"]["total_tokens"], 100)
            self.assertEqual(second["recent"][0]["response_id"], "resp-prune")

    def test_missing_logs_db_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = token_usage_snapshot(logs_db_path=Path(tmp) / "missing.sqlite")

        self.assertFalse(snapshot["available"])
        self.assertIn("not found", snapshot["error"])
        self.assertEqual(len(snapshot["by_hour"]), 24)
        self.assertEqual(snapshot["hour_axis"]["max_tokens"], 0)
        self.assertEqual(snapshot["day_axis"]["max_tokens"], 0)
        self.assertEqual(snapshot["project_pie"], [])


def _init_logs_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                ts_nanos INTEGER NOT NULL DEFAULT 0,
                level TEXT NOT NULL DEFAULT 'INFO',
                target TEXT NOT NULL,
                feedback_log_body TEXT,
                module_path TEXT,
                file TEXT,
                line INTEGER,
                thread_id TEXT,
                process_uuid TEXT,
                estimated_bytes INTEGER NOT NULL DEFAULT 0
            )
            """
        )


def _insert_usage_event(
    path: Path,
    *,
    ts: int,
    process_uuid: str,
    response_id: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
    reasoning_tokens: int = 0,
) -> None:
    body = {
        "type": "response.completed",
        "response": {
            "id": response_id,
            "model": model,
            "completed_at": ts,
            "usage": {
                "input_tokens": input_tokens,
                "input_tokens_details": {"cached_tokens": cached_tokens},
                "output_tokens": output_tokens,
                "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
                "total_tokens": input_tokens + output_tokens,
            },
        },
    }
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO logs (ts, target, feedback_log_body, process_uuid)
            VALUES (?, 'codex_api::sse::responses', ?, ?)
            """,
            (ts, "SSE event: " + json.dumps(body, separators=(",", ":")), process_uuid),
        )


def _insert_metadata_event(
    path: Path,
    *,
    ts: int,
    process_uuid: str,
    thread_id: str = "",
    cwd: Path | None = None,
) -> None:
    fields = []
    if thread_id:
        fields.append(f"thread.id={thread_id}")
    if cwd is not None:
        fields.append(f"cwd={cwd}")
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO logs (ts, target, feedback_log_body, thread_id, process_uuid)
            VALUES (?, 'codex_core::stream_events_utils', ?, ?, ?)
            """,
            (ts, " ".join(fields), thread_id or None, process_uuid),
        )


def _delete_usage_events(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM logs WHERE target = 'codex_api::sse::responses'")


if __name__ == "__main__":
    unittest.main()

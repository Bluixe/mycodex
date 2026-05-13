from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.prompts import daily_summary_prompt
from mycodex.status import project_snapshot, snapshot_to_jsonable


class StoreTests(unittest.TestCase):
    def test_add_project_and_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "my_codex.sqlite"
            project_path = Path(tmp) / "repo"
            project_path.mkdir()
            with Store(db_path) as store:
                project = store.add_project("demo", project_path)
                self.assertEqual(project["name"], "demo")
                self.assertEqual(project["path"], str(project_path.resolve()))

                thread = {
                    "id": "thread-1",
                    "sessionId": "session-1",
                    "path": "/tmp/rollout.jsonl",
                    "cwd": str(project_path.resolve()),
                    "preview": "hello",
                    "source": "cli",
                    "modelProvider": "openai",
                    "cliVersion": "0.0.0",
                    "status": {"type": "notLoaded"},
                    "createdAt": 10,
                    "updatedAt": 20,
                    "gitInfo": {"sha": "abc", "branch": "main", "originUrl": "git@example"},
                }
                store.upsert_session(project["id"], thread)
                store.commit()
                sessions = store.list_sessions(project["id"])
                self.assertEqual(len(sessions), 1)
                self.assertEqual(sessions[0]["thread_id"], "thread-1")

    def test_runs_are_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                run_id = store.add_run(project["id"], kind="manual", prompt="hi")
                store.finish_run(run_id, status="completed", output_text="done")
                runs = store.list_runs(project["id"])
                self.assertEqual(runs[0]["status"], "completed")
                self.assertEqual(runs[0]["output_text"], "done")

    def test_todos_are_recorded_and_updated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                todo_id = store.add_todo(
                    project["id"],
                    title="Run smoke test",
                    body="Use a tiny config.",
                    priority=1,
                )

                todos = store.list_todos(project["id"])
                self.assertEqual(len(todos), 1)
                self.assertEqual(todos[0]["title"], "Run smoke test")
                self.assertEqual(todos[0]["status"], "open")

                store.update_todo_status(todo_id, "done")
                todo = store.get_todo(todo_id)
                self.assertIsNotNone(todo)
                self.assertEqual(todo["status"], "done")
                self.assertIsNotNone(todo["completed_at"])

    def test_project_snapshot_includes_todos_and_failed_run_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_todo(project["id"], title="Fix failure", status="blocked")
                run_id = store.add_run(project["id"], kind="manual", prompt="hi")
                store.finish_run(run_id, status="failed", error="boom")

                snapshot = snapshot_to_jsonable(project_snapshot(store, "demo"))
                self.assertEqual(snapshot["open_todo_count"], 1)
                self.assertEqual(snapshot["health"]["severity"], "danger")
                self.assertEqual(snapshot["latest_run"]["status"], "failed")


class PromptTests(unittest.TestCase):
    def test_daily_summary_mentions_project(self) -> None:
        prompt = daily_summary_prompt("demo")
        self.assertIn("demo", prompt)
        self.assertIn("每日进度总结", prompt)


if __name__ == "__main__":
    unittest.main()

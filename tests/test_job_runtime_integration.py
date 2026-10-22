"""Exercise the durable worker against the actual event parser, without inference."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from mycodex.codex_app import CodexAppServerClient
from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker


def usage(total, last, turn="turn-1"):
    return {"method": "thread/tokenUsage/updated", "params": {
        "threadId": "thread-1", "turnId": turn,
        "tokenUsage": {"total": {"totalTokens": total}, "last": {"totalTokens": last}},
    }}


def answer(text="answer"):
    return {"method": "item/completed", "params": {
        "threadId": "thread-1", "turnId": "turn-1",
        "item": {"id": "answer", "type": "agentMessage", "phase": "final_answer", "text": text},
    }}


def completed(status="completed"):
    return {"method": "turn/completed", "params": {
        "threadId": "thread-1", "turn": {"id": "turn-1", "status": status},
    }}


class FakeEventRuntime(CodexAppServerClient):
    def __init__(self, events, on_start=None):
        super().__init__("unused-test-runtime")
        self.interrupt_grace_seconds = 0.02
        self.interrupt_turn = MagicMock(side_effect=lambda **_: self._events.put(completed("interrupted")) or {})
        self.on_start = on_start
        for event in events:
            self._events.put(event)

    def start(self):
        pass

    def close(self):
        pass

    def list_threads(self, **_kwargs):
        return []

    def start_or_resume_thread(self, **_kwargs):
        return {"id": "thread-1"}

    def start_turn(self, **_kwargs):
        if self.on_start:
            self.on_start()
        return "turn-1", {"id": "turn-1", "status": "inProgress"}


class JobRuntimeIntegrationTests(unittest.TestCase):
    def execute(self, root, events, *, max_tokens=20, on_start=None):
        with Store(root / "db.sqlite") as store:
            project = store.add_project("demo", root)
            store.update_default_thread(project["id"], "thread-1")
            queue = JobQueue(store)
            job = queue.submit("demo", "prompt", {"prompt": "bounded", "max_total_tokens": max_tokens})
            runtime = FakeEventRuntime(events, on_start=(lambda: on_start(queue, job)) if on_start else None)
            with patch("mycodex.service.CodexAppServerClient", return_value=runtime):
                result = run_worker(store, once=True, job_id=job["id"])
            runs = [dict(row) for row in store.list_runs(project["id"])]
            return result["jobs"][0], runs[0], runtime

    def test_resumed_thread_native_budget_stops_current_turn_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            job, run, runtime = self.execute(Path(tmp), [
                usage(10000, 900, turn="previous-turn"),
                usage(10025, 25),
            ])
            self.assertEqual(job["status"], "cancelled")
            self.assertEqual(run["status"], "interrupted")
            self.assertEqual(run["output_text"], "")
            runtime.interrupt_turn.assert_called_once()

    def test_repeated_snapshots_do_not_double_count_or_charge_old_thread_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            job, run, runtime = self.execute(Path(tmp), [
                usage(10010, 10), usage(10010, 10), usage(10015, 5), answer(), completed(),
            ])
            self.assertEqual(job["status"], "succeeded")
            self.assertEqual(run["output_text"], "answer")
            runtime.interrupt_turn.assert_not_called()

    def test_cancelled_job_interrupts_turn_and_never_publishes_partial_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            job, run, runtime = self.execute(Path(tmp), [],
                on_start=lambda queue, job: queue.cancel(job["id"]))
            self.assertEqual(job["status"], "cancelled")
            self.assertEqual(run["status"], "interrupted")
            self.assertEqual(run["output_text"], "")
            runtime.interrupt_turn.assert_called_once()

    def test_lost_lease_stops_runtime_before_output_can_be_accepted(self):
        def expire(queue, job):
            queue.conn.execute("UPDATE jobs SET lease_expires_at=0 WHERE id=?", (job["id"],))
            queue.conn.commit()
        with tempfile.TemporaryDirectory() as tmp:
            job, run, runtime = self.execute(Path(tmp), [], on_start=expire)
            self.assertNotEqual(job["status"], "succeeded")
            self.assertEqual(run["status"], "interrupted")
            runtime.interrupt_turn.assert_called_once()

    def test_failed_codex_terminal_status_is_not_a_successful_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            job, run, _ = self.execute(Path(tmp), [answer(), completed("failed")])
            self.assertEqual(job["status"], "failed")
            self.assertEqual(run["status"], "failed")
            self.assertEqual(run["output_text"], "")


if __name__ == "__main__":
    unittest.main()

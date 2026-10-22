"""Verified pause checkpoints across the real parser, service and durable queue."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from mycodex.codex_app import CodexAppServerClient
from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker
from tests.test_job_runtime_integration import FakeEventRuntime, answer, completed, usage


class PauseRuntimeBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "db.sqlite")
        self.store.add_project("demo", self.temp.name)
        self.queue = JobQueue(self.store)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def execute(self, events, *, confirmed=True, during_setup=False, confirmation_grace=None):
        job = self.queue.submit("demo", "prompt", {"prompt": "bounded pause", "max_total_tokens": 100})
        runtime = FakeEventRuntime(events, on_start=lambda: self.queue.pause(job["id"]))
        # This fixture persists usage through real SQLite callbacks. The shared
        # event-parser fixture's 20ms shortcut is unsuitable for a positive
        # checkpoint assertion on slower CI filesystems. Use the real policy;
        # acknowledgement-only and deliberate-expiry cases opt into a short wait.
        runtime.interrupt_grace_seconds = (
            confirmation_grace if confirmation_grace is not None else
            CodexAppServerClient("unused-test-runtime").interrupt_grace_seconds if confirmed else 0.02
        )
        if not confirmed:
            runtime.interrupt_turn = MagicMock(return_value={})
        if during_setup:
            def thread_setup(**_kwargs):
                self.queue.pause(job["id"])
                return {"id": "thread-1"}
            runtime.start_or_resume_thread = thread_setup
            runtime.start_turn = MagicMock()
        with patch.dict("os.environ", {"MYCODEX_RUNTIME_BACKEND": "app-server"}), \
                patch("mycodex.service.CodexAppServerClient", return_value=runtime):
            run_worker(self.store, once=True, job_id=job["id"])
        return self.queue.get(job["id"]), runtime

    def test_ack_only_pause_is_blocked_and_cannot_resume(self):
        job, runtime = self.execute([], confirmed=False)
        self.assertEqual(job["status"], "blocked")
        self.assertFalse(job["result"]["interruption_confirmed"])
        with self.assertRaises(ValueError):
            self.queue.resume(job["id"])
        runtime.interrupt_turn.assert_called_once()

    def test_confirmed_interruption_has_a_resumable_thread_and_accounted_usage(self):
        job, _runtime = self.execute([usage(10, 10)])
        self.assertEqual(job["status"], "paused", job.get("error"))
        self.assertTrue(job["result"]["interruption_confirmed"])
        resumed = self.queue.resume(job["id"])
        self.assertEqual(resumed["checkpoint"]["thread_id"], "thread-1")
        self.assertEqual(resumed["checkpoint"]["consumed_tokens"], 10)

    def usage_persistence_latency(self, confirmation_grace=None):
        # Advance only the synthetic elapsed clock during persistence. No sleeps,
        # timer precision or scheduler assumptions determine either outcome.
        clock = [1000.0]
        original = JobQueue.observe_usage

        def persist(queue, *args, **kwargs):
            clock[0] += 0.05
            return original(queue, *args, **kwargs)

        with patch("mycodex.codex_app.time.monotonic", side_effect=lambda: clock[0]), \
                patch.object(JobQueue, "observe_usage", persist):
            return self.execute([usage(10, 10)], confirmation_grace=confirmation_grace)

    def test_usage_persistence_within_real_grace_preserves_confirmed_checkpoint(self):
        job, runtime = self.usage_persistence_latency()
        self.assertEqual(runtime.interrupt_grace_seconds,
                         CodexAppServerClient("unused-test-runtime").interrupt_grace_seconds)
        self.assertEqual(job["status"], "paused", job.get("error"))
        self.assertTrue(job["result"]["interruption_confirmed"])
        self.assertEqual(job["result"]["budget_usage"]["total_tokens"], 10)

    def test_usage_persistence_exhausting_grace_requires_reconciliation(self):
        job, _runtime = self.usage_persistence_latency(confirmation_grace=0.02)
        self.assertEqual(job["status"], "blocked")
        self.assertFalse(job["result"]["interruption_confirmed"])
        self.assertIn("terminal interruption was not confirmed", job["error"])
        with self.assertRaises(ValueError):
            self.queue.resume(job["id"])

    def test_successful_completion_race_is_not_replayed_as_a_paused_task(self):
        job, _runtime = self.execute([answer("finished"), completed()])
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["result"]["output_text"], "finished")
        with self.assertRaises(ValueError):
            self.queue.resume(job["id"])

    def test_pause_during_thread_setup_does_not_start_inference(self):
        job, runtime = self.execute([], during_setup=True)
        self.assertEqual(job["status"], "paused")
        self.assertTrue(job["result"]["not_started"])
        runtime.start_turn.assert_not_called()
        self.assertEqual(self.queue.resume(job["id"])["checkpoint"]["consumed_tokens"], 0)


if __name__ == "__main__":
    unittest.main()

"""Verified pause checkpoints across the real parser, service and durable queue."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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

    def execute(self, events, *, confirmed=True, during_setup=False):
        job = self.queue.submit("demo", "prompt", {"prompt": "bounded pause", "max_total_tokens": 100})
        runtime = FakeEventRuntime(events, on_start=lambda: self.queue.pause(job["id"]))
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
        self.assertEqual(job["status"], "paused")
        self.assertTrue(job["result"]["interruption_confirmed"])
        resumed = self.queue.resume(job["id"])
        self.assertEqual(resumed["checkpoint"]["thread_id"], "thread-1")
        self.assertEqual(resumed["checkpoint"]["consumed_tokens"], 10)

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

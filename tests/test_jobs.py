from __future__ import annotations

import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker


class JobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "db.sqlite"
        self.store = Store(self.path)
        self.project = self.store.add_project("demo", self.tmp.name)
        self.queue = JobQueue(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_submission_survives_reopen_and_is_idempotent(self):
        first = self.queue.submit("demo", "prompt", {"prompt": "hello"}, idempotency_key="request-1")
        with Store(self.path) as reopened:
            second = JobQueue(reopened).submit("demo", "prompt", {"prompt": "hello"}, idempotency_key="request-1")
        self.assertEqual(first["id"], second["id"])
        with self.assertRaises(ValueError):
            self.queue.submit("demo", "prompt", {"prompt": "different"}, idempotency_key="request-1")

    def test_atomic_claim_prevents_duplicate_execution(self):
        job = self.queue.submit("demo", "prompt", {"prompt": "hello"})
        barrier = threading.Barrier(2)
        def claim(owner):
            with Store(self.path) as store:
                barrier.wait(timeout=5)
                return JobQueue(store).claim(owner)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ["one", "two"]))
        self.assertEqual([row["id"] for row in results if row], [job["id"]])

    def test_experiment_task_atomic_claim(self):
        experiment = self.store.add_experiment(self.project["id"], name="test", thread_id="fake")
        task = self.store.upsert_experiment_task(experiment["id"], status="ready", idempotency_key="one")
        barrier = threading.Barrier(2)
        def claim(owner):
            with Store(self.path) as store:
                barrier.wait(timeout=5)
                return store.lease_next_experiment_task(experiment["id"], owner=owner)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ["one", "two"]))
        self.assertEqual([row["id"] for row in results if row], [task["id"]])

    def test_dependencies_wait_and_failed_dependency_blocks(self):
        parent = self.queue.submit("demo", "sync_sessions")
        child = self.queue.submit("demo", "prompt", {"prompt": "after"}, depends_on=[parent["id"]])
        first = self.queue.claim("owner")
        self.assertEqual(first["id"], parent["id"])
        self.assertIsNone(self.queue.claim("other"))
        self.queue.finish(first["id"], "owner", "failed")
        self.queue.recover()
        self.assertEqual(self.queue.get(child["id"])["status"], "blocked")

    def test_unstarted_lease_recovers_but_started_lease_blocks(self):
        first = self.queue.submit("demo", "prompt", {"prompt": "one"})
        leased = self.queue.claim("owner", lease_seconds=2)
        self.queue.recover(now=leased["lease_expires_at"] + 1)
        self.assertEqual(self.queue.get(first["id"])["status"], "queued")
        leased = self.queue.claim("next", lease_seconds=2)
        self.assertTrue(self.queue.start(first["id"], "next"))
        self.queue.recover(now=leased["lease_expires_at"] + 1)
        self.assertEqual(self.queue.get(first["id"])["status"], "blocked")
        self.assertFalse(self.queue.finish(first["id"], "owner", "succeeded"))

    def test_cancel_prevents_start_and_wrong_owner_cannot_finish(self):
        job = self.queue.submit("demo", "prompt", {"prompt": "one"})
        self.queue.claim("owner")
        self.assertFalse(self.queue.finish(job["id"], "wrong", "succeeded"))
        self.queue.cancel(job["id"])
        self.assertFalse(self.queue.start(job["id"], "owner"))
        self.assertEqual(self.queue.get(job["id"])["status"], "cancelled")

    def test_worker_records_success_and_failed_runtime_status(self):
        job = self.queue.submit("demo", "prompt", {"prompt": "one"})
        with patch("mycodex.service.send_prompt", return_value={"status": "failed", "error": "provider"}):
            result = run_worker(self.store, once=True)
        self.assertEqual(result["processed"], 1)
        self.assertEqual(self.queue.get(job["id"])["status"], "failed")

    def test_schema_and_authority_are_frozen(self):
        self.store.update_default_thread(self.project["id"], "old-thread")
        job = self.queue.submit("demo", "prompt", {"prompt": "one"})
        self.store.update_default_thread(self.project["id"], "new-thread")
        self.assertEqual(job["payload"]["thread_id"], "old-thread")
        self.assertEqual(job["payload"]["sandbox"], "read-only")
        for payload in ({"prompt": "x", "timeout": -1}, {"prompt": "x", "max_total_tokens": True}):
            with self.assertRaises(ValueError):
                self.queue.submit("demo", "prompt", payload)

    def test_dependencies_validate_before_touching_database(self):
        with self.assertRaises(ValueError):
            self.queue.submit("demo", "sync_sessions", depends_on=[{}])
        self.assertEqual(self.queue.list(), [])


if __name__ == "__main__":
    unittest.main()

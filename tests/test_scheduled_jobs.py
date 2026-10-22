import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from mycodex.db import Store
from mycodex.jobs import JobQueue, enqueue_scheduled_jobs, run_worker


class ScheduledJobTests(unittest.TestCase):
    def test_daily_schedule_queues_once_and_notification_targets_project_id(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            project = store.add_project("demo", tmp, daily_summary_enabled=True, daily_summary_time="00:00")
            first = enqueue_scheduled_jobs(store, notification={"enabled": True})
            self.assertEqual(len(first), 1)
            self.assertEqual(enqueue_scheduled_jobs(store), [])
            with patch("mycodex.service.send_prompt", return_value={"status": "completed", "output_text": "Summary"}), patch("mycodex.telegram.notify_project_bindings", return_value={"sent": 1}) as notify:
                run_worker(store, once=True)
            self.assertEqual(notify.call_args.args[1], project["id"])
            self.assertEqual(JobQueue(store).get(first[0]["id"])["status"], "succeeded")

    def test_idempotency_survives_changed_thread_and_checks_dependencies(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            project = store.add_project("demo", tmp)
            queue = JobQueue(store)
            original = queue.submit("demo", "prompt", {"prompt": "one"}, idempotency_key="same")
            store.update_default_thread(project["id"], "later")
            self.assertEqual(queue.submit("demo", "prompt", {"prompt": "one"}, idempotency_key="same")["id"], original["id"])
            parent = queue.submit("demo", "sync_sessions")
            with self.assertRaises(ValueError):
                queue.submit("demo", "prompt", {"prompt": "one"}, idempotency_key="same", depends_on=[parent["id"]])

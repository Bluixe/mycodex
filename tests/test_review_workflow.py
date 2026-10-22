import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker
from mycodex.review_workflow import submit_review


class ReviewWorkflowTests(unittest.TestCase):
    def test_reviews_are_independent_bounded_and_synthesis_waits(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            store.add_project("demo", tmp)
            workflow = submit_review(store, "demo", "Check this hypothesis", roles=["evidence", "experiment"], token_budget=9000)
            queue = JobQueue(store)
            first = queue.claim("first")
            second = queue.claim("second")
            self.assertNotEqual(first["resource_key"], second["resource_key"])
            self.assertIsNone(queue.claim("synthesis"))
            for job, owner in ((first, "first"), (second, "second")):
                self.assertEqual(job["payload"]["sandbox"], "read-only")
                self.assertEqual(job["payload"]["max_total_tokens"], 3000)
                queue.finish(job["id"], owner, "succeeded", result={"output_text": "Independent objection", "thread_id": f"review-{job['id']}"})
            with patch("mycodex.service.send_prompt", return_value={"status": "completed", "output_text": "Decision"}) as send:
                run_worker(store, once=True, job_id=workflow["synthesis"]["id"])
                self.assertIn("Independent objection", send.call_args.args[2])
                self.assertEqual(send.call_args.kwargs["sandbox"], "read-only")
            self.assertEqual(queue.get(workflow["synthesis"]["id"])["status"], "succeeded")

    def test_failure_blocks_synthesis_and_tiny_budget_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            store.add_project("demo", tmp)
            with self.assertRaises(ValueError):
                submit_review(store, "demo", "x", roles=["evidence"], token_budget=10)
            workflow = submit_review(store, "demo", "x", roles=["evidence"])
            queue = JobQueue(store)
            job = queue.claim("worker")
            queue.finish(job["id"], "worker", "failed")
            queue.recover()
            self.assertEqual(queue.get(workflow["synthesis"]["id"])["status"], "blocked")
